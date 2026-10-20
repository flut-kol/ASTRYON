package e2e

// Harness for the MVP-gate end-to-end tests (DPP-030). Everything here is gated
// on ASTRYON_E2E=1: the tests need a running dev stack (Kafka, ClickHouse with
// db/clickhouse migrations applied, topics created via `make topics`) and build
// the four services from their modules into a temp dir, then run them as
// subprocesses. Nothing is wiped; every run uses fresh random tenants and
// fresh consumer groups whose offsets are pre-committed to the log end.
//
// Env (defaults = the standard dev stack ports):
//
//	ASTRYON_E2E=1                      enable
//	ASTRYON_E2E_KAFKA                  127.0.0.1:19092
//	ASTRYON_E2E_CH_URL                 http://127.0.0.1:18124
//	ASTRYON_E2E_CH_PASSWORD            astryon_dev_only (user astryon, db astryon)
//	ASTRYON_E2E_PG_DSN                 postgres://astryon:astryon_dev_only@127.0.0.1:15432/astryon?sslmode=disable
//	ASTRYON_E2E_KEEP_LOGS=1            keep the temp dir (binaries + service logs)

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type env struct {
	kafka, chURL, chPass, pgDSN string
	dir                         string
	bins                        map[string]string
	procs                       []*proc
	proc, sink                  *proc
	procMetrics, sinkMetrics    string
	runID                       string
	lastTenant                  *tenantSvc
}

type proc struct {
	name string
	cmd  *exec.Cmd
	log  string
	done chan struct{}
}

var (
	stack   *env
	stackMu sync.Mutex
)

func TestMain(m *testing.M) {
	code := m.Run()
	if stack != nil {
		stack.shutdown()
	}
	os.Exit(code)
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func uuid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func freeAddr(t testing.TB) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// getStack lazily builds the binaries and starts the stream-processor and
// clickhouse-sink (shared by all tests). Gateways and query-apis are per tenant.
func getStack(t testing.TB) *env {
	t.Helper()
	if os.Getenv("ASTRYON_E2E") != "1" {
		t.Skip("set ASTRYON_E2E=1 (needs the dev stack: Kafka, ClickHouse, topics, CH migrations)")
	}
	stackMu.Lock()
	defer stackMu.Unlock()
	if stack != nil {
		return stack
	}
	e := &env{
		kafka:  getenv("ASTRYON_E2E_KAFKA", "127.0.0.1:19092"),
		chURL:  getenv("ASTRYON_E2E_CH_URL", "http://127.0.0.1:18124"),
		chPass: getenv("ASTRYON_E2E_CH_PASSWORD", "astryon_dev_only"),
		pgDSN:  getenv("ASTRYON_E2E_PG_DSN", "postgres://astryon:astryon_dev_only@127.0.0.1:15432/astryon?sslmode=disable"),
		bins:   map[string]string{},
		runID:  uuid()[:8],
	}
	var err error
	if e.dir, err = os.MkdirTemp("", "astryon-e2e-"); err != nil {
		t.Fatal(err)
	}
	stack = e // so TestMain cleans up even if setup fails below
	root, _ := filepath.Abs("../..")
	for _, bd := range []struct{ name, dir, pkg string }{
		{"gateway", "services/gateway", "./cmd/gateway"},
		{"stream-proc", "services/stream-processor", "./cmd"},
		{"clickhouse-sink", "services/clickhouse-sink", "./cmd/clickhouse-sink"},
		{"query-api", "services/query-api", "./cmd/query-api"},
		{"loadgen", "tests/load", "."},
	} {
		out := filepath.Join(e.dir, bd.name)
		cmd := exec.Command("go", "build", "-o", out, bd.pkg)
		cmd.Dir = filepath.Join(root, bd.dir)
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", bd.name, err, b)
		}
		e.bins[bd.name] = out
	}
	// Fresh groups start at the current log end so counters are not polluted by
	// earlier runs and nothing old is reprocessed.
	pg, sg := "e2e-proc-"+e.runID, "e2e-sink-"+e.runID
	e.commitGroupToEnd(t, pg, "raw.events.v1")
	e.commitGroupToEnd(t, sg, "processed.events.v1")
	e.procMetrics, e.sinkMetrics = freeAddr(t), freeAddr(t)
	e.proc = e.start(t, "stream-proc", e.bins["stream-proc"], map[string]string{
		"STREAMPROC_BROKERS": e.kafka, "STREAMPROC_GROUP_ID": pg, "STREAMPROC_BATCH_INTERVAL": "100ms",
		"STREAMPROC_WARM_START": "false", "STREAMPROC_METRICS_ADDR": e.procMetrics,
	})
	e.sink = e.start(t, "sink", e.bins["clickhouse-sink"], map[string]string{
		"CHSINK_KAFKA_BROKERS": e.kafka, "CHSINK_GROUP_ID": sg, "CHSINK_CH_URL": e.chURL,
		"CHSINK_CH_PASSWORD": e.chPass, "CHSINK_BATCH_INTERVAL": "200ms", "CHSINK_METRICS_ADDR": e.sinkMetrics,
	})
	waitHTTP(t, "http://"+e.procMetrics+"/debug/vars", 20*time.Second)
	waitHTTP(t, "http://"+e.sinkMetrics+"/debug/vars", 20*time.Second)
	return e
}

func (e *env) start(t testing.TB, name, bin string, vars map[string]string) *proc {
	logf := filepath.Join(e.dir, name+".log")
	f, err := os.Create(logf)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin)
	cmd.Env = os.Environ()
	for k, v := range vars {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	p := &proc{name: name, cmd: cmd, log: logf, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); f.Close(); close(p.done) }()
	e.procs = append(e.procs, p)
	return p
}

func (e *env) shutdown() {
	for _, p := range e.procs {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range e.procs {
		select {
		case <-p.done:
		case <-time.After(20 * time.Second):
			_ = p.cmd.Process.Kill()
		}
	}
	if os.Getenv("ASTRYON_E2E_KEEP_LOGS") == "1" {
		fmt.Fprintln(os.Stderr, "e2e logs kept in", e.dir)
		return
	}
	_ = os.RemoveAll(e.dir)
}

func (e *env) dumpLogs(t testing.TB) {
	for _, p := range e.procs {
		b, _ := os.ReadFile(p.log)
		if len(b) > 4000 {
			b = b[len(b)-4000:]
		}
		t.Logf("--- %s log tail ---\n%s", p.name, b)
	}
}

func waitHTTP(t testing.TB, u string, d time.Duration) {
	t.Helper()
	dl := time.Now().Add(d)
	for time.Now().Before(dl) {
		if r, err := http.Get(u); err == nil {
			r.Body.Close()
			if r.StatusCode < 500 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if stack != nil {
		stack.dumpLogs(t)
	}
	t.Fatalf("timeout waiting for %s", u)
}

// tenantSvc is a gateway + query-api pair authenticating one dev tenant.
type tenantSvc struct {
	tenant, token string
	gw, qa        string // base URLs
}

func (e *env) startTenant(t testing.TB, extraGW map[string]string) *tenantSvc {
	ts := &tenantSvc{tenant: uuid(), token: "tok-" + uuid()}
	gaddr, qaddr := freeAddr(t), freeAddr(t)
	gv := map[string]string{
		"ASTRYON_DEV_AUTH": "1", "ASTRYON_DEV_TOKEN": ts.token, "ASTRYON_DEV_TENANT_ID": ts.tenant,
		"ASTRYON_GATEWAY_ADDR": gaddr, "ASTRYON_KAFKA_BROKERS": e.kafka,
	}
	for k, v := range extraGW {
		gv[k] = v
	}
	e.start(t, "gateway-"+ts.tenant[:8], e.bins["gateway"], gv)
	e.start(t, "query-"+ts.tenant[:8], e.bins["query-api"], map[string]string{
		"ASTRYON_DEV_AUTH": "1", "ASTRYON_DEV_TOKEN": ts.token, "ASTRYON_DEV_TENANT_ID": ts.tenant,
		"ASTRYON_QUERY_ADDR": qaddr, "ASTRYON_CH_URL": e.chURL, "ASTRYON_CH_USER": "astryon",
		"ASTRYON_CH_PASSWORD": e.chPass, "DATABASE_URL": e.pgDSN,
		"ASTRYON_QUERY_CURSOR_KEY": "e2e-cursor-key-0123456789abcdef0123456789", "ASTRYON_QUERY_RATE_RPS": "1000", "ASTRYON_QUERY_RATE_BURST": "1000",
	})
	ts.gw, ts.qa = "http://"+gaddr, "http://"+qaddr
	waitHTTP(t, ts.gw+"/healthz", 20*time.Second)
	waitHTTP(t, ts.qa+"/healthz", 20*time.Second)
	return ts
}

// ---- Kafka helpers -------------------------------------------------------

func (e *env) client(t testing.TB, opts ...kgo.Opt) *kgo.Client {
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(e.kafka)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// endOffsets returns log-end offset per partition of topic.
func endOffsets(ctx context.Context, cl *kgo.Client, topic string) (map[int32]int64, error) {
	md := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &topic
	md.Topics = append(md.Topics, mt)
	mr, err := md.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	lo := kmsg.NewPtrListOffsetsRequest()
	lo.ReplicaID = -1
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = topic
	for _, tp := range mr.Topics {
		for _, p := range tp.Partitions {
			lp := kmsg.NewListOffsetsRequestTopicPartition()
			lp.Partition, lp.Timestamp = p.Partition, -1
			lt.Partitions = append(lt.Partitions, lp)
		}
	}
	lo.Topics = append(lo.Topics, lt)
	lr, err := lo.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	out := map[int32]int64{}
	for _, tp := range lr.Topics {
		for _, p := range tp.Partitions {
			if p.ErrorCode != 0 {
				return nil, fmt.Errorf("list offsets p%d: error %d", p.Partition, p.ErrorCode)
			}
			out[p.Partition] = p.Offset
		}
	}
	return out, nil
}

// commitGroupToEnd creates group offsets at the current log end (admin commit
// to an empty group) so a new consumer in that group starts at "now".
func (e *env) commitGroupToEnd(t testing.TB, group, topic string) {
	t.Helper()
	cl := e.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ends, err := endOffsets(ctx, cl, topic)
	if err != nil {
		t.Fatal(err)
	}
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group, req.Generation = group, -1
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = topic
	for p, o := range ends {
		cp := kmsg.NewOffsetCommitRequestTopicPartition()
		cp.Partition, cp.Offset = p, o
		ct.Partitions = append(ct.Partitions, cp)
	}
	req.Topics = append(req.Topics, ct)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			if p.ErrorCode != 0 {
				t.Fatalf("commit group %s p%d: error %d", group, p.Partition, p.ErrorCode)
			}
		}
	}
}

// groupLag returns total lag (log end - committed) of group on topic.
func groupLag(ctx context.Context, cl *kgo.Client, group, topic string) (int64, error) {
	ends, err := endOffsets(ctx, cl, topic)
	if err != nil {
		return 0, err
	}
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	ft := kmsg.NewOffsetFetchRequestTopic()
	ft.Topic = topic
	for p := range ends {
		ft.Partitions = append(ft.Partitions, p)
	}
	req.Topics = append(req.Topics, ft)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, err
	}
	var lag int64
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			c := p.Offset
			if c < 0 {
				c = 0
			}
			lag += ends[p.Partition] - c
		}
	}
	return lag, nil
}

// readTopic returns values of records on topic produced at or after since that
// contain substr (a tenant id), reading up to the log end captured at call time.
func (e *env) readTopic(t testing.TB, topic, substr string, since time.Time) [][]byte {
	cl := e.client(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(since.UnixMilli())))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ends, err := endOffsets(ctx, cl, topic)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	idle := 0
	for idle < 3 && ctx.Err() == nil {
		pc, pcancel := context.WithTimeout(ctx, time.Second)
		fs := cl.PollFetches(pc)
		pcancel()
		n := 0
		fs.EachRecord(func(r *kgo.Record) {
			n++
			if r.Offset < ends[r.Partition] && bytes.Contains(r.Value, []byte(substr)) {
				out = append(out, r.Value)
			}
		})
		if n == 0 {
			idle++
		} else {
			idle = 0
		}
	}
	return out
}

func (e *env) produceRaw(t testing.TB, key, value []byte) {
	cl := e.client(t)
	r := cl.ProduceSync(context.Background(), &kgo.Record{Topic: "raw.events.v1", Key: key, Value: value})
	if err := r.FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// ---- HTTP / ClickHouse helpers ---------------------------------------------

func httpJSON(t testing.TB, method, u, token string, body any, out any) int {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, u, rd)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if out != nil && len(b) > 0 {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("decode %s (%d): %v: %s", u, resp.StatusCode, err, b)
		}
	}
	return resp.StatusCode
}

// chQuery runs a read-only statement and returns the trimmed TSV body.
func (e *env) chQuery(t testing.TB, q string) string {
	t.Helper()
	req, _ := http.NewRequest("POST", e.chURL+"/?database=astryon&"+url.Values{"user": {"astryon"}, "password": {e.chPass}}.Encode(), strings.NewReader(q))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("clickhouse %d: %s", resp.StatusCode, b)
	}
	return strings.TrimSpace(string(b))
}

func expvarInt(t testing.TB, addr, key, field string) int64 {
	resp, err := http.Get("http://" + addr + "/debug/vars")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var raw map[string]map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&raw)
	f, _ := raw[key][field].(float64)
	return int64(f)
}

func eventually(t testing.TB, d time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	dl := time.Now().Add(d)
	last := ""
	for time.Now().Before(dl) {
		ok, s := cond()
		if ok {
			return
		}
		last = s
		time.Sleep(300 * time.Millisecond)
	}
	if stack != nil {
		stack.dumpLogs(t)
	}
	t.Fatalf("timeout waiting for %s: %s", what, last)
}
