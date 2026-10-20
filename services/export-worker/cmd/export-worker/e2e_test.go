package main

// Live end-to-end harness (EXPORT_E2E=1, otherwise skipped). Starts real ClickHouse 24.8, PostgreSQL 16 and MinIO
// (which verifies SigV4 and pre-signed expiry) in one isolated docker network, builds and runs the REAL export-worker
// binary and drives it over HTTP. Everything is named chagg-expe2e-*; passwords are generated at run time.
// Runbook: services/export-worker/README.md ("Live end-to-end"). Set EXPORT_E2E_KEEP=1 to keep the containers.
// MINIO_IMAGE overrides the MinIO image (default bitnamilegacy/minio:latest, entrypoint overridden).

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/astryon/export-worker/internal/export"
)

const (
	e2eNet   = "chagg-expe2e-net"
	e2eCH    = "chagg-expe2e-ch"
	e2ePG    = "chagg-expe2e-pg"
	e2eMinio = "chagg-expe2e-minio"
)

func rnd(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func docker(t *testing.T, stdin io.Reader, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", args...)
	cmd.Stdin = stdin
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("docker %s: %v\n%s%s", strings.Join(args[:min(len(args), 3)], " "), err, out.String(), errb.String())
	}
	return strings.TrimSpace(out.String())
}

func waitFor(t *testing.T, what string, d time.Duration, f func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if f() {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

type e2eEnv struct {
	t                      *testing.T
	root, tmp, bin         string
	chPort, pgPort, s3Port int
	chUser, chPass         string
	pgSuper, rtPass        string
	s3Key, s3Secret        string
	tenA, tenB, tenD       string
	store                  *export.S3Store
	chURL, pgURL, s3URL    string
	tokA, tokB, tokD       string
	workers                []*e2eWorker
	mu                     sync.Mutex
}

type e2eWorker struct {
	name, url, log string
	cmd            *exec.Cmd
	done           chan struct{}
}

func (e *e2eEnv) chQuery(q string, params map[string]string, body io.Reader) string {
	t := e.t
	t.Helper()
	u := e.chURL + "/?database=astryon&query=" + urlQ(q)
	for k, v := range params {
		u += "&param_" + k + "=" + v
	}
	req, _ := http.NewRequest("POST", u, body)
	req.Header.Set("X-ClickHouse-User", e.chUser)
	req.Header.Set("X-ClickHouse-Key", e.chPass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("clickhouse: %s %s", resp.Status, b)
	}
	return strings.TrimSpace(string(b))
}

func urlQ(s string) string {
	r := strings.NewReplacer("%", "%25", " ", "%20", "\n", "%0A", "{", "%7B", "}", "%7D", ":", "%3A", "'", "%27", "(", "%28", ")", "%29", ",", "%2C", "+", "%2B", "&", "%26", "=", "%3D", "<", "%3C", ">", "%3E")
	return r.Replace(s)
}

func (e *e2eEnv) psql(stdin string, args ...string) string {
	a := append([]string{"exec", "-i", "-e", "PGPASSWORD=" + e.pgSuper, e2ePG, "psql", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-X", "-q", "-tA"}, args...)
	return docker(e.t, strings.NewReader(stdin), a...)
}

func (e *e2eEnv) up() {
	t := e.t
	for _, c := range []string{e2eCH, e2ePG, e2eMinio} {
		if out := docker(t, nil, "ps", "-aq", "--filter", "name=^"+c+"$"); out != "" {
			t.Fatalf("container %s already exists; clean up first", c)
		}
	}
	docker(t, nil, "network", "create", e2eNet)
	e.chPort, e.pgPort, e.s3Port = freePort(t), freePort(t), freePort(t)
	e.chUser, e.chPass, e.pgSuper, e.rtPass = "exp", rnd(12), rnd(12), rnd(12)
	e.s3Key, e.s3Secret = "k"+rnd(6), rnd(16)
	e.tokA, e.tokB, e.tokD = rnd(16), rnd(16), rnd(16)

	docker(t, nil, "run", "-d", "--name", e2eCH, "--network", e2eNet, "-p", fmt.Sprintf("127.0.0.1:%d:8123", e.chPort),
		"-e", "CLICKHOUSE_USER="+e.chUser, "-e", "CLICKHOUSE_PASSWORD="+e.chPass, "-e", "CLICKHOUSE_DB=astryon",
		"clickhouse/clickhouse-server:24.8")
	docker(t, nil, "run", "-d", "--name", e2ePG, "--network", e2eNet, "-p", fmt.Sprintf("127.0.0.1:%d:5432", e.pgPort),
		"-e", "POSTGRES_PASSWORD="+e.pgSuper, "postgres:16-alpine")
	img := os.Getenv("MINIO_IMAGE")
	if img == "" {
		img = "bitnamilegacy/minio:latest"
	}
	docker(t, nil, "run", "-d", "--name", e2eMinio, "--network", e2eNet, "-p", fmt.Sprintf("127.0.0.1:%d:9000", e.s3Port),
		"-e", "MINIO_ROOT_USER="+e.s3Key, "-e", "MINIO_ROOT_PASSWORD="+e.s3Secret,
		"--entrypoint", "minio", img, "server", "/tmp/data", "--address", ":9000")

	e.chURL = fmt.Sprintf("http://127.0.0.1:%d", e.chPort)
	e.s3URL = fmt.Sprintf("http://127.0.0.1:%d", e.s3Port)
	waitFor(t, "clickhouse", 90*time.Second, func() bool {
		r, err := http.Get(e.chURL + "/ping")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	})
	waitFor(t, "postgres", 90*time.Second, func() bool {
		return exec.Command("docker", "exec", e2ePG, "pg_isready", "-U", "postgres").Run() == nil
	})
	time.Sleep(2 * time.Second) // postgres restarts once after initdb
	waitFor(t, "postgres (2)", 60*time.Second, func() bool {
		return exec.Command("docker", "exec", e2ePG, "pg_isready", "-U", "postgres").Run() == nil
	})
	waitFor(t, "minio", 60*time.Second, func() bool {
		r, err := http.Get(e.s3URL + "/minio/health/live")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	})

	// ClickHouse migrations (token substitution as db/apply.sh does).
	chFiles, _ := filepath.Glob(filepath.Join(e.root, "db/clickhouse/migrations/*.up.sql"))
	rep := strings.NewReplacer("__ON_CLUSTER__", "", "__RMT__", "MergeTree()", "__CLUSTER__", "default")
	for _, f := range chFiles {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		docker(t, strings.NewReader(rep.Replace(string(b))), "exec", "-i", e2eCH, "clickhouse-client",
			"--user", e.chUser, "--password", e.chPass, "-d", "astryon", "--multiquery")
	}
	// PostgreSQL migrations 0001-0010.
	for _, p := range []string{"000[1-9]_*", "0010_*"} {
		fs, _ := filepath.Glob(filepath.Join(e.root, "db/postgres/migrations", p+".up.sql"))
		for _, f := range fs {
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			e.psql(string(b))
		}
	}
	e.psql(fmt.Sprintf(`CREATE ROLE export_rt LOGIN PASSWORD '%s' NOSUPERUSER NOBYPASSRLS;
GRANT USAGE ON SCHEMA public TO export_rt;
GRANT SELECT, INSERT, UPDATE, DELETE ON export_jobs TO export_rt;
GRANT SELECT ON tenants TO export_rt;`, e.rtPass))
	for i, dst := range []*string{&e.tenA, &e.tenB, &e.tenD} {
		*dst = strings.Fields(e.psql(fmt.Sprintf(`INSERT INTO tenants (slug, name) VALUES ('expe2e-%d-%s', 'e2e') RETURNING id::text;`, i, rnd(3))))[0]
	}
	e.pgURL = fmt.Sprintf("postgres://export_rt:%s@127.0.0.1:%d/postgres?sslmode=disable", e.rtPass, e.pgPort)
	if out := e.psql(`SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname='export_rt'`); out != "f" {
		t.Fatalf("runtime role must be NOSUPERUSER NOBYPASSRLS: %q", out)
	}
	e.store = &export.S3Store{Endpoint: e.s3URL, Bucket: "chagg-expe2e", Region: "us-east-1",
		Creds: export.Creds{AccessKey: e.s3Key, SecretKey: e.s3Secret}}
	if err := e.store.CreateBucket(context.Background()); err != nil {
		t.Fatalf("create bucket with our SigV4 signer against real MinIO: %v", err)
	}
}

func (e *e2eEnv) down() {
	if os.Getenv("EXPORT_E2E_KEEP") == "1" {
		return
	}
	for _, w := range e.workers {
		if w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
	}
	for _, c := range []string{e2eCH, e2ePG, e2eMinio} {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = exec.Command("docker", "network", "rm", e2eNet).Run()
	_ = os.RemoveAll(e.tmp)
}

// seed inserts 100k rows: tenant A 50k unique + 10k duplicates of the first 10k event ids (60k rows), tenant B 40k.
func (e *e2eEnv) seed() {
	ins := func(tenant string, n int, payload string) {
		q := fmt.Sprintf(`INSERT INTO events_normalized_local (tenant_id, source_id, event_id, batch_id, processing_version, pipeline_id, pipeline_version, event_type, event_time, entity_id, attributes, payload, raw_ref, quality_flags)
SELECT {t:UUID}, {t:UUID}, reinterpretAsUUID(sipHash128(concat({t:String}, toString(number)))), {t:UUID}, 1, {t:UUID}, 1, 'e2e.test',
 toDateTime64('2026-02-01 00:00:00', 3, 'UTC') + number, concat('ent,"', toString(number)), map('k', '%s'), repeat('x', 250), 'raw', ['q'] FROM numbers(%d)`, payload, n)
		e.chQuery(q, map[string]string{"t": tenant}, nil)
	}
	ins(e.tenA, 50000, "orig")
	ins(e.tenA, 10000, "dup")
	ins(e.tenB, 40000, "orig")
}

func (e *e2eEnv) startWorker(name, token, tenant string, extra ...string) *e2eWorker {
	t := e.t
	logf := filepath.Join(e.tmp, name+".log")
	f, err := os.Create(logf)
	if err != nil {
		t.Fatal(err)
	}
	port := freePort(t)
	cmd := exec.Command(e.bin)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("LISTEN_ADDR=127.0.0.1:%d", port), "EXPORT_WORKERS=2",
		"CH_URL="+e.chURL, "CH_DATABASE=astryon", "CH_USER="+e.chUser, "CH_PASSWORD="+e.chPass,
		"S3_ENDPOINT="+e.s3URL, "S3_BUCKET=chagg-expe2e", "S3_ACCESS_KEY="+e.s3Key, "S3_SECRET_KEY="+e.s3Secret,
		"EXPORT_PG_URL="+e.pgURL, "EXPORT_WORKER_ID="+name, "EXPORT_LEASE_SECONDS=3",
		"EXPORT_MAX_CONCURRENT_PER_TENANT=100", "EXPORT_URL_TTL_SECONDS=4",
		"EXPORT_DEV_TOKEN="+token, "EXPORT_DEV_TENANT="+tenant)
	cmd.Env = append(cmd.Env, extra...)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	w := &e2eWorker{name: name, url: fmt.Sprintf("http://127.0.0.1:%d", port), log: logf, cmd: cmd, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); f.Close(); close(w.done) }()
	e.mu.Lock()
	e.workers = append(e.workers, w)
	e.mu.Unlock()
	waitFor(t, name+" listening", 20*time.Second, func() bool {
		r, err := http.Get(w.url + "/v1/exports/00000000-0000-0000-0000-000000000000")
		if err != nil {
			return false
		}
		r.Body.Close()
		return true
	})
	return w
}

func (w *e2eWorker) stop(t *testing.T) {
	_ = w.cmd.Process.Signal(os.Interrupt)
	select {
	case <-w.done:
	case <-time.After(35 * time.Second):
		_ = w.cmd.Process.Kill()
		t.Fatalf("%s did not stop on SIGTERM", w.name)
	}
}

type jobView struct {
	ID, Status, ErrorCode, DownloadURL string
	Rows, Bytes                        int64
	StartedAt, FinishedAt              *time.Time
	CreatedAt                          time.Time
}

func (j *jobView) UnmarshalJSON(b []byte) error {
	var r struct {
		ID          string     `json:"id"`
		Status      string     `json:"status"`
		ErrorCode   string     `json:"error_code"`
		DownloadURL string     `json:"download_url"`
		Rows        int64      `json:"rows"`
		Bytes       int64      `json:"bytes"`
		StartedAt   *time.Time `json:"started_at"`
		FinishedAt  *time.Time `json:"finished_at"`
		CreatedAt   time.Time  `json:"created_at"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	*j = jobView{r.ID, r.Status, r.ErrorCode, r.DownloadURL, r.Rows, r.Bytes, r.StartedAt, r.FinishedAt, r.CreatedAt}
	return nil
}

func api(t *testing.T, w *e2eWorker, token, method, path, body string) (int, []byte, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, w.url+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, resp.Header
}

func specJSON(format string, maxRows int, from, to string) string {
	return fmt.Sprintf(`{"format":%q,"from":%q,"to":%q,"processing_version":1,"max_rows":%d}`, format, from, to, maxRows)
}

const (
	wideFrom = "2026-02-01T00:00:00Z"
	wideTo   = "2026-02-03T00:00:00Z"
	hourTo   = "2026-02-01T01:00:00Z"
)

func (e *e2eEnv) post(w *e2eWorker, token, body string) string {
	code, b, _ := api(e.t, w, token, "POST", "/v1/exports", body)
	if code != 202 {
		e.t.Fatalf("POST: %d %s", code, b)
	}
	var j jobView
	if err := json.Unmarshal(b, &j); err != nil || j.ID == "" {
		e.t.Fatalf("POST body %s: %v", b, err)
	}
	return j.ID
}

func (e *e2eEnv) wait(w *e2eWorker, token, id string, want string, d time.Duration) jobView {
	t := e.t
	var j jobView
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		code, b, _ := api(t, w, token, "GET", "/v1/exports/"+id, "")
		if code != 200 {
			t.Fatalf("GET %s: %d %s", id, code, b)
		}
		if err := json.Unmarshal(b, &j); err != nil {
			t.Fatal(err)
		}
		if j.Status == want || (want != "RUNNING" && (j.Status == "SUCCEEDED" || j.Status == "FAILED")) {
			return j
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("job %s stuck in %s (want %s)", id, j.Status, want)
	return j
}

func httpGetRaw(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url) // no Authorization header: the signature is in the URL
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// newE2E brings up ClickHouse, PostgreSQL and MinIO (removed by t.Cleanup) and builds the worker binary.
func newE2E(t *testing.T) *e2eEnv {
	_, thisFile, _, _ := runtime.Caller(0)
	e := &e2eEnv{t: t, root: filepath.Join(filepath.Dir(thisFile), "../../../..")}
	e.tmp = t.TempDir()
	t.Cleanup(e.down)
	t0 := time.Now()
	e.up()
	t.Logf("infrastructure up in %s", time.Since(t0).Round(time.Second))
	e.bin = filepath.Join(e.tmp, "export-worker")
	if out, err := exec.Command("go", "build", "-o", e.bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return e
}

func TestE2ELive(t *testing.T) {
	if os.Getenv("EXPORT_E2E") != "1" {
		t.Skip("set EXPORT_E2E=1 (needs docker; see README 'Live end-to-end')")
	}
	e := newE2E(t)
	s0 := time.Now()
	e.seed()
	uniqA := e.chQuery(`SELECT uniqExact(event_id) FROM events_normalized_local WHERE tenant_id={t:UUID}`, map[string]string{"t": e.tenA}, nil)
	rowsA := e.chQuery(`SELECT count() FROM events_normalized_local WHERE tenant_id={t:UUID}`, map[string]string{"t": e.tenA}, nil)
	total := e.chQuery(`SELECT count() FROM events_normalized_local`, nil, nil)
	t.Logf("seeded %s rows in %s: tenant A %s rows / %s unique", total, time.Since(s0).Round(time.Millisecond), rowsA, uniqA)
	if total != "100000" || uniqA != "50000" || rowsA != "60000" {
		t.Fatalf("seed mismatch total=%s A=%s uniqA=%s", total, rowsA, uniqA)
	}

	wA := e.startWorker("wA", e.tokA, e.tenA)
	wB := e.startWorker("wB", e.tokB, e.tenB)

	download := func(j jobView) []byte {
		code, b := httpGetRaw(t, j.DownloadURL)
		if code != 200 {
			t.Fatalf("download: %d %s", code, b)
		}
		return b
	}

	var ndjsonJob jobView
	t.Run("ndjson_and_csv_tenant_A_dedup", func(t *testing.T) {
		for _, format := range []string{"ndjson", "csv"} {
			st := time.Now()
			id := e.post(wA, e.tokA, specJSON(format, 100000, wideFrom, wideTo))
			j := e.wait(wA, e.tokA, id, "SUCCEEDED", 120*time.Second)
			el := time.Since(st)
			if j.Status != "SUCCEEDED" || j.Rows != 50000 || j.DownloadURL == "" {
				t.Fatalf("%s: %+v", format, j)
			}
			b := download(j)
			if int64(len(b)) != j.Bytes {
				t.Fatalf("%s: downloaded %d bytes, job says %d", format, len(b), j.Bytes)
			}
			ids := map[string]bool{}
			switch format {
			case "ndjson":
				sc := bufio.NewScanner(bytes.NewReader(b))
				sc.Buffer(make([]byte, 1<<20), 1<<20)
				for sc.Scan() {
					var r map[string]any
					if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
						t.Fatal(err)
					}
					if r["source_id"] != e.tenA {
						t.Fatalf("foreign row: %v", r["source_id"])
					}
					ids[r["event_id"].(string)] = true
				}
				ndjsonJob = j
			case "csv":
				recs, err := csv.NewReader(bytes.NewReader(b)).ReadAll()
				if err != nil {
					t.Fatal(err)
				}
				if len(recs) != 50001 {
					t.Fatalf("csv records %d", len(recs))
				}
				for _, r := range recs[1:] {
					if r[1] != e.tenA {
						t.Fatalf("foreign row: %v", r[1])
					}
					ids[r[0]] = true
				}
			}
			if len(ids) != 50000 {
				t.Fatalf("%s: %d distinct event ids, want 50000", format, len(ids))
			}
			t.Logf("RESULT %s: rows=%d bytes=%d wall=%s (POST->SUCCEEDED seen)", format, j.Rows, j.Bytes, el.Round(time.Millisecond))
		}
	})

	t.Run("max_rows_overflow_leaves_no_object", func(t *testing.T) {
		id := e.post(wA, e.tokA, specJSON("ndjson", 1000, wideFrom, wideTo))
		j := e.wait(wA, e.tokA, id, "FAILED", 60*time.Second)
		if j.Status != "FAILED" || j.DownloadURL != "" {
			t.Fatalf("%+v", j)
		}
		if _, err := e.store.Get(context.Background(), export.ObjectKey(e.tenA, id, "ndjson")); err == nil {
			t.Fatal("object left after overflow")
		}
		t.Logf("RESULT overflow: status=%s error_code=%s, object absent", j.Status, j.ErrorCode)
	})

	t.Run("multipart_overflow_aborts_upload", func(t *testing.T) {
		// 20000 rows ~11 MB > one 5 MiB part: the cap trips after a multipart upload started.
		id := e.post(wA, e.tokA, specJSON("ndjson", 20000, wideFrom, wideTo))
		j := e.wait(wA, e.tokA, id, "FAILED", 60*time.Second)
		if j.Status != "FAILED" || j.ErrorCode != "row_cap_exceeded" {
			t.Fatalf("%+v", j)
		}
		if _, err := e.store.Get(context.Background(), export.ObjectKey(e.tenA, id, "ndjson")); err == nil {
			t.Fatal("object left after overflow")
		}
		out := docker(t, nil, "exec", e2eMinio, "sh", "-c", "find /tmp/data/.minio.sys/multipart -mindepth 2 -maxdepth 2 2>/dev/null | wc -l")
		if strings.TrimSpace(out) != "0" {
			t.Fatalf("incomplete multipart uploads left in MinIO: %s", out)
		}
		t.Logf("RESULT multipart overflow: FAILED %s, no object, no orphan multipart upload", j.ErrorCode)
	})

	t.Run("cross_tenant_get_is_404", func(t *testing.T) {
		if code, b, _ := api(t, wB, e.tokB, "GET", "/v1/exports/"+ndjsonJob.ID, ""); code != 404 {
			t.Fatalf("tenant B sees A's job: %d %s", code, b)
		}
		if code, _, _ := api(t, wA, e.tokB, "GET", "/v1/exports/"+ndjsonJob.ID, ""); code != 401 {
			t.Fatalf("wrong token: %d", code)
		}
	})

	t.Run("signature_and_expiry_enforced_by_minio", func(t *testing.T) {
		id := e.post(wA, e.tokA, specJSON("ndjson", 100000, hourTo[:11]+"00:00:00Z", hourTo))
		j := e.wait(wA, e.tokA, id, "SUCCEEDED", 60*time.Second)
		if j.Status != "SUCCEEDED" || j.Rows != 3600 {
			t.Fatalf("%+v", j)
		}
		// fresh URL from GET, valid
		_, b, _ := api(t, wA, e.tokA, "GET", "/v1/exports/"+id, "")
		var jj jobView
		_ = json.Unmarshal(b, &jj)
		u := jj.DownloadURL
		if code, _ := httpGetRaw(t, u); code != 200 {
			t.Fatalf("valid URL rejected: %d", code)
		}
		// tampered signature
		i := strings.Index(u, "X-Amz-Signature=") + len("X-Amz-Signature=")
		bad := []byte(u)
		if bad[i] == 'a' {
			bad[i] = 'b'
		} else {
			bad[i] = 'a'
		}
		code, body := httpGetRaw(t, string(bad))
		if code != 403 || !strings.Contains(string(body), "SignatureDoesNotMatch") {
			t.Fatalf("tampered signature: %d %s", code, body)
		}
		t.Logf("RESULT tampered signature -> %d SignatureDoesNotMatch", code)
		// same signature, different object
		other := strings.Replace(u, id, ndjsonJob.ID, 1)
		if code, _ := httpGetRaw(t, other); code != 403 {
			t.Fatalf("signature reused for another key: %d", code)
		}
		// no signature at all (bucket is private)
		if code, _ := httpGetRaw(t, strings.SplitN(u, "?", 2)[0]); code != 403 {
			t.Fatalf("unsigned GET: %d", code)
		}
		// expiry (worker TTL = 4 s)
		time.Sleep(5 * time.Second)
		code, body = httpGetRaw(t, u)
		if code != 403 || !strings.Contains(string(body), "expired") {
			t.Fatalf("expired URL: %d %s", code, body)
		}
		t.Logf("RESULT expired URL (ttl 4s, waited 5s) -> %d %s", code, strings.Join(strings.Fields(string(body))[:min(12, len(strings.Fields(string(body))))], " "))
		// a new GET re-signs and works again
		_, b, _ = api(t, wA, e.tokA, "GET", "/v1/exports/"+id, "")
		_ = json.Unmarshal(b, &jj)
		if code, _ := httpGetRaw(t, jj.DownloadURL); code != 200 {
			t.Fatalf("re-signed URL rejected: %d", code)
		}
	})

	t.Run("burst_split_across_two_workers_exactly_once", func(t *testing.T) {
		const n = 40
		st := time.Now()
		ids := make([]string, n)
		for i := range ids {
			ids[i] = e.post(wA, e.tokA, specJSON("ndjson", 100000, wideFrom, wideTo))
		}
		for _, id := range ids {
			j := e.wait(wA, e.tokA, id, "SUCCEEDED", 120*time.Second)
			if j.Status != "SUCCEEDED" || j.Rows != 50000 {
				t.Fatalf("%s: %+v", id, j)
			}
		}
		el := time.Since(st)
		time.Sleep(500 * time.Millisecond)
		runs := map[string]string{}
		per := map[string]int{}
		for _, w := range []*e2eWorker{wA, wB} {
			f, _ := os.Open(w.log)
			sc := bufio.NewScanner(f)
			sc.Buffer(make([]byte, 1<<20), 1<<20)
			for sc.Scan() {
				var r export.AuditRecord
				if json.Unmarshal(sc.Bytes(), &r) != nil || r.ExportID == "" {
					continue
				}
				for _, id := range ids {
					if id == r.ExportID {
						if prev, ok := runs[id]; ok {
							t.Fatalf("job %s ran twice (%s and %s)", id, prev, w.name)
						}
						runs[id] = w.name
						per[w.name]++
					}
				}
			}
			f.Close()
		}
		if len(runs) != n {
			t.Fatalf("%d of %d jobs have an audit record", len(runs), n)
		}
		if per["wA"] == 0 || per["wB"] == 0 {
			t.Fatalf("work was not split across both workers: %v", per)
		}
		t.Logf("RESULT burst: %d jobs x 50000 rows in %s, each ran exactly once; split %v", n, el.Round(time.Millisecond), per)
	})

	wA.stop(t)
	wB.stop(t)

	t.Run("quota_429_on_n_plus_1", func(t *testing.T) {
		wQ := e.startWorker("wQ", e.tokD, e.tenD, "EXPORT_MAX_PER_DAY=3")
		for i := 0; i < 3; i++ {
			id := e.post(wQ, e.tokD, specJSON("ndjson", 100, wideFrom, hourTo))
			e.wait(wQ, e.tokD, id, "SUCCEEDED", 30*time.Second)
		}
		code, b, h := api(t, wQ, e.tokD, "POST", "/v1/exports", specJSON("ndjson", 100, wideFrom, hourTo))
		if code != 429 || !strings.Contains(string(b), "quota_exceeded") || h.Get("Retry-After") == "" {
			t.Fatalf("4th export: %d %s retry-after=%q", code, b, h.Get("Retry-After"))
		}
		t.Logf("RESULT quota: 4th POST -> 429 Retry-After=%s", h.Get("Retry-After"))
		wQ.stop(t)
	})

	t.Run("kill9_mid_export_ends_FAILED_worker_lost", func(t *testing.T) {
		w1 := e.startWorker("wK1", e.tokA, e.tenA)
		docker(t, nil, "pause", e2eMinio) // uploads hang, so the job is deterministically mid-export
		paused := true
		defer func() {
			if paused {
				docker(t, nil, "unpause", e2eMinio)
			}
		}()
		id := e.post(w1, e.tokA, specJSON("ndjson", 100000, wideFrom, wideTo))
		e.wait(w1, e.tokA, id, "RUNNING", 20*time.Second)
		time.Sleep(1 * time.Second)
		killed := time.Now()
		_ = w1.cmd.Process.Kill()
		<-w1.done
		docker(t, nil, "unpause", e2eMinio)
		paused = false
		w2 := e.startWorker("wK2", e.tokA, e.tenA)
		j := e.wait(w2, e.tokA, id, "FAILED", 60*time.Second)
		if j.Status != "FAILED" || j.ErrorCode != "worker_lost" || j.StartedAt == nil {
			t.Fatalf("%+v", j)
		}
		if _, err := e.store.Get(context.Background(), export.ObjectKey(e.tenA, id, "ndjson")); err == nil {
			t.Fatal("object exists for a killed job")
		}
		t.Logf("RESULT kill -9: job RUNNING->FAILED worker_lost %s after the kill (lease 3s); no object", time.Since(killed).Round(100*time.Millisecond))
		// the lost job is never re-run; the survivor still works
		id2 := e.post(w2, e.tokA, specJSON("ndjson", 100000, wideFrom, hourTo))
		if j2 := e.wait(w2, e.tokA, id2, "SUCCEEDED", 60*time.Second); j2.Status != "SUCCEEDED" || j2.Rows != 3600 {
			t.Fatalf("%+v", j2)
		}
		w2.stop(t)
	})
}
