package main

// Failure-mode live harness (EXPORT_E2E=1): ClickHouse dies mid-stream, PostgreSQL restarts, SIGTERM and kill -9 mid-export
// (orphan multipart uploads) and the scope of a pre-signed URL. Same containers as TestE2ELive (chagg-expe2e-*).
// Run: EXPORT_E2E=1 go test ./cmd/export-worker -run TestE2EFailureModes -v -count=1 -timeout 30m

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/astryon/export-worker/internal/export"
)

func (e *e2eEnv) seedBig(tenant string, n int) {
	q := fmt.Sprintf(`INSERT INTO events_normalized_local (tenant_id, source_id, event_id, batch_id, processing_version, pipeline_id, pipeline_version, event_type, event_time, entity_id, attributes, payload, raw_ref, quality_flags)
SELECT {t:UUID}, {t:UUID}, reinterpretAsUUID(sipHash128(concat({t:String}, toString(number)))), {t:UUID}, 1, {t:UUID}, 1, 'e2e.big',
 toDateTime64('2026-02-01 00:00:00', 3, 'UTC') + number, concat('ent,"', toString(number)), map('k', 'big'), repeat('x', 250), 'raw', ['q'] FROM numbers(%d)`, n)
	e.chQuery(q, map[string]string{"t": tenant}, nil)
}

func (e *e2eEnv) uploads() []export.Upload {
	ups, err := e.store.ListMultipartUploads(context.Background(), "")
	if err != nil {
		e.t.Fatalf("ListMultipartUploads: %v", err)
	}
	return ups
}

func (e *e2eEnv) waitUpload(d time.Duration) {
	e.t.Helper()
	waitFor(e.t, "a multipart upload in flight", d, func() bool { return len(e.uploads()) > 0 })
}

// pgTry runs a query as superuser; ok=false while PostgreSQL is unreachable (never fails the test).
func (e *e2eEnv) pgTry(q string) (string, bool) {
	cmd := exec.Command("docker", "exec", "-i", "-e", "PGPASSWORD="+e.pgSuper, e2ePG, "psql", "-U", "postgres", "-d", "postgres", "-X", "-q", "-tA", "-c", q)
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err == nil
}

// row returns "status|error_code|rows|bytes" of a job.
func (e *e2eEnv) row(id string) string {
	out, ok := e.pgTry(fmt.Sprintf(`SELECT status||'|'||COALESCE(error_code,'')||'|'||rows||'|'||bytes FROM export_jobs WHERE id='%s'`, id))
	if !ok {
		return "?"
	}
	return out
}

func (e *e2eEnv) waitTerminal(ids []string, d time.Duration) map[string]string {
	e.t.Helper()
	res := map[string]string{}
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		for _, id := range ids {
			if _, done := res[id]; done {
				continue
			}
			if r := e.row(id); strings.HasPrefix(r, "SUCCEEDED") || strings.HasPrefix(r, "FAILED") {
				res[id] = r
			}
		}
		if len(res) == len(ids) {
			return res
		}
		time.Sleep(250 * time.Millisecond)
	}
	e.t.Fatalf("jobs not terminal after %s: %d of %d done", d, len(res), len(ids))
	return nil
}

func (e *e2eEnv) objectExists(tenant, id string) bool {
	_, err := e.store.Get(context.Background(), export.ObjectKey(tenant, id, "ndjson"))
	return err == nil
}

func auditOf(w *e2eWorker) map[string][]export.AuditRecord {
	out := map[string][]export.AuditRecord{}
	f, err := os.Open(w.log)
	if err != nil {
		return out
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r export.AuditRecord
		if json.Unmarshal(sc.Bytes(), &r) == nil && r.ExportID != "" {
			out[r.ExportID] = append(out[r.ExportID], r)
		}
	}
	return out
}

func logLines(w *e2eWorker) int {
	b, _ := os.ReadFile(w.log)
	return strings.Count(string(b), "\n")
}

func (e *e2eEnv) alive(w *e2eWorker) bool {
	select {
	case <-w.done:
		return false
	default:
		return true
	}
}

func TestE2EFailureModes(t *testing.T) {
	if os.Getenv("EXPORT_E2E") != "1" {
		t.Skip("set EXPORT_E2E=1 (needs docker; see README 'Live end-to-end')")
	}
	e := newE2E(t)
	e.tenA = strings.Fields(e.psql(fmt.Sprintf(`INSERT INTO tenants (slug, name) VALUES ('expe2e-f-%s', 'e2e') RETURNING id::text;`, rnd(3))))[0]
	tenF, tokF := e.tenA, e.tokA
	s0 := time.Now()
	e.seedBig(tenF, 1_000_000)
	t.Logf("seeded 1000000 rows for the failure tenant in %s", time.Since(s0).Round(time.Millisecond))
	big := specJSON("ndjson", 3_000_000, "2026-02-01T00:00:00Z", "2026-02-20T00:00:00Z") // 1M rows ~ 575 MB
	mid := specJSON("ndjson", 3_000_000, "2026-02-01T00:00:00Z", "2026-02-04T00:00:00Z") // 259 200 rows ~ 150 MB
	small := specJSON("ndjson", 100000, wideFrom, hourTo)                                // 3 600 rows

	nextOK := func(w *e2eWorker) time.Duration {
		st := time.Now()
		id := e.post(w, tokF, small)
		if j := e.wait(w, tokF, id, "SUCCEEDED", 60*time.Second); j.Status != "SUCCEEDED" || j.Rows != 3600 {
			t.Fatalf("next job: %+v", j)
		}
		return time.Since(st)
	}

	t.Run("clickhouse_KILL_QUERY_mid_stream", func(t *testing.T) {
		w := e.startWorker("wC", tokF, tenF, "EXPORT_MAX_CONCURRENT_PER_TENANT=1", "EXPORT_SWEEP_SECONDS=0")
		id := e.post(w, tokF, big)
		e.waitUpload(60 * time.Second)
		if code, b, _ := api(t, w, tokF, "POST", "/v1/exports", small); code != 429 {
			t.Fatalf("concurrency cap 1 must hold while the big job runs: %d %s", code, b)
		}
		killedAt := time.Now()
		e.chQuery(`KILL QUERY WHERE query LIKE '%e2e.big%' OR (query LIKE '%events_normalized%' AND query NOT LIKE 'KILL%' AND query NOT LIKE 'SELECT%system%') SYNC`, nil, nil)
		j := e.wait(w, tokF, id, "FAILED", 60*time.Second)
		if j.Status != "FAILED" || j.ErrorCode != "clickhouse_error" {
			t.Fatalf("%+v", j)
		}
		if e.objectExists(tenF, id) || len(e.uploads()) != 0 {
			t.Fatalf("object=%v uploads=%d after a ClickHouse failure", e.objectExists(tenF, id), len(e.uploads()))
		}
		rec := auditOf(w)[id]
		el := nextOK(w) // the concurrency slot (cap 1) was released
		t.Logf("RESULT KILL QUERY mid-stream: FAILED %s %s after the kill (rows streamed %d, %.0f MB); no object, 0 multipart uploads; audit error=%q; slot released: next job SUCCEEDED in %s",
			j.ErrorCode, time.Since(killedAt).Round(10*time.Millisecond), j.Rows, float64(j.Bytes)/1e6, firstErr(rec), el.Round(10*time.Millisecond))
		w.stop(t)
	})

	t.Run("clickhouse_container_stopped_mid_stream", func(t *testing.T) {
		w := e.startWorker("wC2", tokF, tenF, "EXPORT_MAX_CONCURRENT_PER_TENANT=1", "EXPORT_SWEEP_SECONDS=0")
		id := e.post(w, tokF, big)
		e.waitUpload(60 * time.Second)
		stoppedAt := time.Now()
		docker(t, nil, "kill", e2eCH)
		j := e.wait(w, tokF, id, "FAILED", 60*time.Second)
		if j.Status != "FAILED" || j.ErrorCode != "clickhouse_error" {
			t.Fatalf("%+v", j)
		}
		failAfter := time.Since(stoppedAt)
		if e.objectExists(tenF, id) || len(e.uploads()) != 0 {
			t.Fatalf("object=%v uploads=%d", e.objectExists(tenF, id), len(e.uploads()))
		}
		// while ClickHouse is down: a new job fails fast, with a clean state and a free slot
		id2 := e.post(w, tokF, small)
		j2 := e.wait(w, tokF, id2, "FAILED", 30*time.Second)
		if j2.ErrorCode != "clickhouse_error" || e.objectExists(tenF, id2) {
			t.Fatalf("job while CH is down: %+v", j2)
		}
		startAt := time.Now()
		docker(t, nil, "start", e2eCH)
		waitFor(t, "clickhouse back", 90*time.Second, func() bool {
			r, err := http.Get(e.chURL + "/ping")
			if err != nil {
				return false
			}
			r.Body.Close()
			return r.StatusCode == 200
		})
		pingAfter := time.Since(startAt)
		el := nextOK(w)
		t.Logf("RESULT docker kill ClickHouse mid-stream: FAILED clickhouse_error %s after the kill (rows %d), no object, 0 uploads; job posted while CH down: FAILED clickhouse_error; CH ping back after %s, worker served the next job %s after `docker start` (job %s)",
			failAfter.Round(10*time.Millisecond), j.Rows, pingAfter.Round(100*time.Millisecond), time.Since(startAt).Round(100*time.Millisecond), el.Round(10*time.Millisecond))
		w.stop(t)
	})

	runBatch := func(name string, ws []*e2eWorker, n int, disrupt func() (downFor time.Duration)) {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = e.post(ws[i%len(ws)], tokF, mid)
		}
		waitFor(t, "a job RUNNING", 60*time.Second, func() bool {
			out, _ := e.pgTry(`SELECT count(*) FROM export_jobs WHERE status='RUNNING'`)
			return out != "" && out != "0"
		})
		time.Sleep(1 * time.Second)
		st := time.Now()
		down := disrupt()
		res := e.waitTerminal(ids, 180*time.Second)
		allDone := time.Since(st)
		time.Sleep(1500 * time.Millisecond) // late audit lines
		aud := map[string]int{}
		for _, w := range ws {
			if !e.alive(w) {
				t.Fatalf("%s: worker %s crashed during the outage", name, w.name)
			}
			for id, rs := range auditOf(w) {
				aud[id] += len(rs)
			}
		}
		okN, lost, other := 0, 0, 0
		orphanObjects := 0
		for _, id := range ids {
			r := res[id]
			if aud[id] > 1 {
				t.Fatalf("%s: job %s ran %d times", name, id, aud[id])
			}
			switch {
			case strings.HasPrefix(r, "SUCCEEDED|"):
				okN++
				if !e.objectExists(tenF, id) {
					t.Fatalf("%s: job %s SUCCEEDED without object", name, id)
				}
			case strings.HasPrefix(r, "FAILED|worker_lost|"):
				lost++
				if e.objectExists(tenF, id) {
					orphanObjects++
				}
			default:
				other++
				t.Errorf("%s: job %s ended %s", name, id, r)
			}
		}
		lines := 0
		for _, w := range ws {
			lines += logLines(w)
		}
		t.Logf("RESULT %s: %d jobs x 259200 rows, downtime %s, all terminal %s after the disruption started: SUCCEEDED(resumed finish)=%d FAILED worker_lost=%d (of which with a finished object left=%d) other=%d; every job audited <= 1 time; workers alive; %d log lines in total",
			name, n, down.Round(100*time.Millisecond), allDone.Round(100*time.Millisecond), okN, lost, orphanObjects, other, lines)
	}

	t.Run("postgres_restart_and_outage", func(t *testing.T) {
		wP1 := e.startWorker("wP1", tokF, tenF, "EXPORT_MAX_CONCURRENT_PER_TENANT=100", "EXPORT_SWEEP_SECONDS=2", "EXPORT_SWEEP_GRACE_SECONDS=2")
		wP2 := e.startWorker("wP2", tokF, tenF, "EXPORT_MAX_CONCURRENT_PER_TENANT=100", "EXPORT_SWEEP_SECONDS=2", "EXPORT_SWEEP_GRACE_SECONDS=2")
		ws := []*e2eWorker{wP1, wP2}
		runBatch("docker restart postgres (jobs running+queued)", ws, 8, func() time.Duration {
			st := time.Now()
			docker(t, nil, "restart", "-t", "1", e2ePG)
			waitFor(t, "pg_isready", 60*time.Second, func() bool {
				return exec.Command("docker", "exec", e2ePG, "pg_isready", "-U", "postgres").Run() == nil
			})
			return time.Since(st)
		})
		runBatch("postgres stopped 8 s (> lease 3 s) with jobs running+queued", ws, 8, func() time.Duration {
			docker(t, nil, "stop", "-t", "1", e2ePG)
			st := time.Now()
			time.Sleep(8 * time.Second)
			if code, _, _ := api(t, wP1, tokF, "POST", "/v1/exports", small); code == 202 {
				t.Errorf("POST must not be accepted while PostgreSQL is down")
			} else {
				t.Logf("POST while PostgreSQL is down -> HTTP %d", code)
			}
			docker(t, nil, "start", e2ePG)
			waitFor(t, "pg_isready", 60*time.Second, func() bool {
				return exec.Command("docker", "exec", e2ePG, "pg_isready", "-U", "postgres").Run() == nil
			})
			return time.Since(st)
		})
		rs := time.Now()
		el := nextOK(wP1)
		t.Logf("RESULT after PostgreSQL returned the worker serves a new job in %s (POST->SUCCEEDED)", el.Round(10*time.Millisecond))
		_ = rs
		waitFor(t, "no orphan uploads", 30*time.Second, func() bool { return len(e.uploads()) == 0 })
		for _, w := range ws {
			w.stop(t)
		}
	})

	t.Run("sigterm_graceful_short_job", func(t *testing.T) {
		w := e.startWorker("wS", tokF, tenF, "EXPORT_SWEEP_SECONDS=0")
		id := e.post(w, tokF, mid)
		e.waitUpload(60 * time.Second)
		st := time.Now()
		_ = w.cmd.Process.Signal(syscall.SIGTERM)
		time.Sleep(300 * time.Millisecond)
		if _, err := http.Get(w.url + "/v1/exports/" + id); err == nil {
			t.Error("listener must be closed after SIGTERM")
		}
		select {
		case <-w.done:
		case <-time.After(40 * time.Second):
			t.Fatal("worker did not exit")
		}
		exit := time.Since(st)
		r := e.row(id)
		if !strings.HasPrefix(r, "SUCCEEDED|") || !e.objectExists(tenF, id) || len(e.uploads()) != 0 {
			t.Fatalf("job %s row=%s object=%v uploads=%d", id, r, e.objectExists(tenF, id), len(e.uploads()))
		}
		t.Logf("RESULT SIGTERM during a ~5 s export: worker waited, job %s, exit %s after SIGTERM, 0 uploads left", r, exit.Round(100*time.Millisecond))
	})

	t.Run("sigterm_graceful_job_longer_than_deadline", func(t *testing.T) {
		w := e.startWorker("wS2", tokF, tenF, "EXPORT_SWEEP_SECONDS=0")
		// stall uploads so the export cannot finish within the 25 s shutdown deadline
		id := e.post(w, tokF, big)
		e.waitUpload(60 * time.Second)
		docker(t, nil, "pause", e2eMinio)
		paused := true
		defer func() {
			if paused {
				docker(t, nil, "unpause", e2eMinio)
			}
		}()
		st := time.Now()
		_ = w.cmd.Process.Signal(syscall.SIGTERM)
		go func() { time.Sleep(26 * time.Second); docker(t, nil, "unpause", e2eMinio) }() // after the deadline: S3 reachable again for the abort retry
		var exit time.Duration
		select {
		case <-w.done:
			exit = time.Since(st)
		case <-time.After(90 * time.Second):
			t.Fatal("worker did not exit")
		}
		paused = false
		time.Sleep(1 * time.Second)
		r := e.row(id)
		ups := e.uploads()
		t.Logf("RESULT SIGTERM with S3 stalled (job cannot finish): worker exit %s after SIGTERM; job row=%s; object=%v; multipart uploads left=%d", exit.Round(100*time.Millisecond), r, e.objectExists(tenF, id), len(ups))
		if !strings.HasPrefix(r, "FAILED|shutdown|") {
			t.Errorf("want FAILED shutdown, got %s", r)
		}
		for _, u := range ups { // leave a clean slate for the next scenario; the sweeper test below covers cleanup
			_ = e.store.AbortUpload(context.Background(), u.Key, u.UploadID)
		}
	})

	t.Run("kill9_mid_multipart_orphan_and_sweeper", func(t *testing.T) {
		wO := e.startWorker("wO", tokF, tenF, "EXPORT_SWEEP_SECONDS=0")
		id := e.post(wO, tokF, big)
		e.waitUpload(60 * time.Second)
		killed := time.Now()
		_ = wO.cmd.Process.Kill()
		<-wO.done
		time.Sleep(500 * time.Millisecond)
		ups := e.uploads()
		if len(ups) != 1 {
			t.Fatalf("expected exactly 1 orphan multipart upload after kill -9, got %d", len(ups))
		}
		t.Logf("RESULT kill -9 mid-multipart: %d incomplete multipart upload remains (key %s) - the abort never ran", len(ups), ups[0].Key)
		wR := e.startWorker("wR", tokF, tenF, "EXPORT_SWEEP_SECONDS=2", "EXPORT_SWEEP_GRACE_SECONDS=2")
		rows := e.waitTerminal([]string{id}, 60*time.Second)
		failedAt := time.Since(killed)
		defer func() {
			if t.Failed() {
				b, _ := os.ReadFile(wR.log)
				t.Logf("wR log:\n%s", b)
			}
		}()
		waitFor(t, "sweeper aborts the orphan", 60*time.Second, func() bool { return len(e.uploads()) == 0 })
		t.Logf("RESULT sweeper: job %s %s after the kill; orphan upload aborted %s after the kill (sweep every 2 s, grace 2 s); object=%v",
			rows[id], failedAt.Round(100*time.Millisecond), time.Since(killed).Round(100*time.Millisecond), e.objectExists(tenF, id))
		nextOK(wR)
		wR.stop(t)
	})

	t.Run("signed_url_is_scoped_to_one_object", func(t *testing.T) {
		w := e.startWorker("wU", tokF, tenF, "EXPORT_SWEEP_SECONDS=0", "EXPORT_URL_TTL_SECONDS=600")
		id := e.post(w, tokF, small)
		j := e.wait(w, tokF, id, "SUCCEEDED", 60*time.Second)
		u, err := url.Parse(j.DownloadURL)
		if err != nil {
			t.Fatal(err)
		}
		withPath := func(p string) string { c := *u; c.Path, c.RawPath = p, ""; return c.String() }
		bucket := "/chagg-expe2e"
		key := bucket + "/" + export.ObjectKey(tenF, id, "ndjson")
		if code, _ := httpGetRaw(t, j.DownloadURL); code != 200 {
			t.Fatalf("valid URL: %d", code)
		}
		cases := map[string]string{
			"bucket root (list) with the object's signature":   withPath(bucket),
			"bucket root with trailing slash":                  withPath(bucket + "/"),
			"tenant prefix directory":                          withPath(bucket + "/exports/" + tenF + "/"),
			"exports/ prefix":                                  withPath(bucket + "/exports/"),
			"other key, same prefix":                           withPath(bucket + "/exports/" + tenF + "/" + e.tenB + ".ndjson"),
			"other tenant prefix":                              withPath(bucket + "/exports/" + e.tenB + "/" + id + ".ndjson"),
			"other bucket, same key":                           withPath("/other-bucket/" + export.ObjectKey(tenF, id, "ndjson")),
			"path traversal out of the prefix":                 strings.Replace(j.DownloadURL, "/exports/"+tenF+"/"+id, "/exports/"+tenF+"/../"+tenF+"/"+id, 1),
			"list query appended to the bucket":                withPath(bucket) + "&list-type=2&prefix=exports%2F",
			"object signature on a sibling object (extension)": withPath(key + ".bak"),
		}
		for name, tu := range cases {
			code, body := httpGetRaw(t, tu)
			if code == 200 || strings.Contains(string(body), "ListBucketResult") || strings.Contains(string(body), "e2e.big") {
				t.Errorf("%s: HTTP %d %.200s", name, code, body)
				continue
			}
			t.Logf("RESULT signed URL reused for %s -> HTTP %d", name, code)
		}
		for _, m := range []string{http.MethodPut, http.MethodDelete, http.MethodPost, http.MethodHead} {
			req, _ := http.NewRequest(m, j.DownloadURL, strings.NewReader("x"))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 || resp.StatusCode == 204 {
				t.Errorf("GET-signed URL accepted for %s: %d", m, resp.StatusCode)
			} else {
				t.Logf("RESULT GET-signed URL used with %s -> HTTP %d", m, resp.StatusCode)
			}
		}
		if code, _ := httpGetRaw(t, j.DownloadURL); code != 200 {
			t.Errorf("object must still be intact and readable via its own URL: %d", code)
		}
		w.stop(t)
	})
}

func firstErr(rs []export.AuditRecord) string {
	for _, r := range rs {
		if r.Error != "" {
			if len(r.Error) > 160 {
				return r.Error[:160]
			}
			return r.Error
		}
	}
	return ""
}

// pgProxy is a TCP proxy between ONE worker and PostgreSQL. block() resets every connection and refuses new ones
// (that worker's PostgreSQL link is down while the other workers and the test keep talking to PostgreSQL directly).
type pgProxy struct {
	ln      net.Listener
	target  string
	mu      sync.Mutex
	blocked bool
	conns   map[net.Conn]struct{}
}

func newPGProxy(t *testing.T, target string) *pgProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &pgProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.blocked {
				p.mu.Unlock()
				c.Close()
				continue
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				p.mu.Unlock()
				c.Close()
				continue
			}
			p.conns[c], p.conns[up] = struct{}{}, struct{}{}
			p.mu.Unlock()
			go func() { _, _ = io.Copy(up, c); up.Close(); c.Close() }()
			go func() { _, _ = io.Copy(c, up); up.Close(); c.Close() }()
		}
	}()
	t.Cleanup(func() { p.setBlocked(true); ln.Close() })
	return p
}

func (p *pgProxy) addr() string { return p.ln.Addr().String() }

func (p *pgProxy) setBlocked(b bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = b
	if b {
		for c := range p.conns {
			c.Close()
		}
		p.conns = map[net.Conn]struct{}{}
	}
}

// waitLog polls a worker log for a substring and returns when it appeared (zero time on timeout).
func waitLog(w *e2eWorker, sub string, d time.Duration) time.Time {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if b, _ := os.ReadFile(w.log); strings.Contains(string(b), sub) {
			return time.Now()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return time.Time{}
}

func logHas(w *e2eWorker, sub string) bool {
	b, _ := os.ReadFile(w.log)
	return strings.Contains(string(b), sub)
}

func (e *e2eEnv) waitNoUploads(d time.Duration) time.Duration {
	st := time.Now()
	waitFor(e.t, "no incomplete multipart uploads", d, func() bool { return len(e.uploads()) == 0 })
	return time.Since(st)
}

// TestE2ETimeoutsAndLeaseLoss proves LIVE: the ClickHouse idle/max-duration timeouts (docker pause of ClickHouse), a
// zombie run whose lease was taken away (that worker's PostgreSQL link is cut by a proxy until another worker's
// recovery failed the job) and a PostgreSQL outage longer than the finish-retry window (docker pause of PostgreSQL).
// Run: EXPORT_E2E=1 go test ./cmd/export-worker -run TestE2ETimeoutsAndLeaseLoss -v -count=1 -timeout 25m
func TestE2ETimeoutsAndLeaseLoss(t *testing.T) {
	if os.Getenv("EXPORT_E2E") != "1" {
		t.Skip("set EXPORT_E2E=1 (needs docker; see README 'Live end-to-end')")
	}
	e := newE2E(t)
	e.tenA = strings.Fields(e.psql(fmt.Sprintf(`INSERT INTO tenants (slug, name) VALUES ('expe2e-t-%s', 'e2e') RETURNING id::text;`, rnd(3))))[0]
	ten, tok := e.tenA, e.tokA
	e.seedBig(ten, 1_000_000)
	big := specJSON("ndjson", 3_000_000, "2026-02-01T00:00:00Z", "2026-02-20T00:00:00Z")
	mid := specJSON("ndjson", 3_000_000, "2026-02-01T00:00:00Z", "2026-02-04T00:00:00Z")
	small := specJSON("ndjson", 100000, wideFrom, hourTo)
	paused := map[string]bool{}
	pause := func(c string) { docker(t, nil, "pause", c); paused[c] = true }
	unpause := func(c string) {
		if paused[c] {
			docker(t, nil, "unpause", c)
			paused[c] = false
		}
	}
	t.Cleanup(func() {
		for c, p := range paused {
			if p {
				_ = exec.Command("docker", "unpause", c).Run()
			}
		}
	})
	nextOK := func(w *e2eWorker) time.Duration {
		st := time.Now()
		id := e.post(w, tok, small)
		if j := e.wait(w, tok, id, "SUCCEEDED", 60*time.Second); j.Status != "SUCCEEDED" || j.Rows != 3600 {
			t.Fatalf("next job: %+v", j)
		}
		return time.Since(st)
	}
	chBack := func() {
		waitFor(t, "clickhouse ping", 60*time.Second, func() bool {
			r, err := http.Get(e.chURL + "/ping")
			if err != nil {
				return false
			}
			r.Body.Close()
			return r.StatusCode == 200
		})
	}

	t.Run("clickhouse_stall_idle_timeout", func(t *testing.T) {
		w := e.startWorker("wI", tok, ten, "EXPORT_SWEEP_SECONDS=0", "EXPORT_CH_IDLE_TIMEOUT=3s", "EXPORT_PPROF_ADDR=127.0.0.1:0")
		id := e.post(w, tok, big)
		e.waitUpload(60 * time.Second)
		pause(e2eCH)
		at := time.Now()
		j := e.wait(w, tok, id, "FAILED", 60*time.Second)
		el := time.Since(at)
		unpause(e2eCH)
		if j.Status != "FAILED" || j.ErrorCode != "clickhouse_timeout" {
			t.Fatalf("%+v", j)
		}
		aborted := e.waitNoUploads(30 * time.Second)
		if e.objectExists(ten, id) {
			t.Fatal("object left")
		}
		chBack()
		rec := auditOf(w)[id]
		next := nextOK(w)
		t.Logf("RESULT docker pause ClickHouse mid-stream (idle timeout 3 s): job FAILED clickhouse_timeout %s after the pause (rows streamed %d); upload aborted, 0 multipart uploads %s later, no object; audit error=%q; after unpause the next job SUCCEEDED in %s",
			el.Round(10*time.Millisecond), j.Rows, aborted.Round(10*time.Millisecond), firstErr(rec), next.Round(10*time.Millisecond))
		w.stop(t)
	})

	t.Run("clickhouse_stall_max_duration", func(t *testing.T) {
		w := e.startWorker("wM", tok, ten, "EXPORT_SWEEP_SECONDS=0", "EXPORT_CH_IDLE_TIMEOUT=0", "EXPORT_CH_MAX_DURATION=8s")
		at := time.Now()
		id := e.post(w, tok, big)
		e.waitUpload(60 * time.Second)
		pause(e2eCH)
		j := e.wait(w, tok, id, "FAILED", 60*time.Second)
		el := time.Since(at)
		unpause(e2eCH)
		if j.Status != "FAILED" || j.ErrorCode != "clickhouse_timeout" {
			t.Fatalf("%+v", j)
		}
		e.waitNoUploads(30 * time.Second)
		if e.objectExists(ten, id) {
			t.Fatal("object left")
		}
		chBack()
		t.Logf("RESULT docker pause ClickHouse with the idle timeout off, max duration 8 s: job FAILED clickhouse_timeout %s after POST; no object, 0 uploads; audit error=%q",
			el.Round(10*time.Millisecond), firstErr(auditOf(w)[id]))
		nextOK(w)
		w.stop(t)
	})

	t.Run("zombie_lease_lost_while_running", func(t *testing.T) {
		px := newPGProxy(t, strings.SplitN(strings.SplitN(e.pgURL, "@", 2)[1], "/", 2)[0])
		pgViaProxy := strings.Replace(e.pgURL, strings.SplitN(strings.SplitN(e.pgURL, "@", 2)[1], "/", 2)[0], px.addr(), 1)
		wA := e.startWorker("wZA", tok, ten, "EXPORT_SWEEP_SECONDS=0", "EXPORT_PG_URL="+pgViaProxy)
		id := e.post(wA, tok, big)
		e.waitUpload(60 * time.Second)
		pause(e2eMinio) // the export cannot finish: it is a live zombie candidate
		defer unpause(e2eMinio)
		wB := e.startWorker("wZB", tok, ten, "EXPORT_SWEEP_SECONDS=0")
		cut := time.Now()
		px.setBlocked(true)
		waitFor(t, "worker B fails the job", 60*time.Second, func() bool { return strings.HasPrefix(e.row(id), "FAILED|worker_lost|") })
		lostAfter := time.Since(cut)
		time.Sleep(1 * time.Second)
		back := time.Now()
		px.setBlocked(false)
		seen := waitLog(wA, "ALERT export_lease_lost job="+id, 30*time.Second)
		if seen.IsZero() {
			t.Fatalf("worker A never noticed the lease loss; log:\n%s", func() string { b, _ := os.ReadFile(wA.log); return string(b) }())
		}
		cancelAfter := seen.Sub(back)
		unpause(e2eMinio) // the abort of the multipart upload goes through now
		cleanupAt := time.Now()
		cleaned := waitLog(wA, "export_lease_lost_cleanup_done job="+id, 45*time.Second)
		aborted := e.waitNoUploads(45 * time.Second)
		if cleaned.IsZero() {
			t.Fatal("run cleanup never finished")
		}
		time.Sleep(1500 * time.Millisecond)
		if e.objectExists(ten, id) || !strings.HasPrefix(e.row(id), "FAILED|worker_lost|") {
			t.Fatalf("object=%v row=%s", e.objectExists(ten, id), e.row(id))
		}
		if !e.alive(wA) || !e.alive(wB) {
			t.Fatal("a worker crashed")
		}
		next := nextOK(wA)
		t.Logf("RESULT zombie run (job running, upload in flight; worker A's PostgreSQL link cut, MinIO paused): worker B marked it FAILED worker_lost %s after the cut (lease 3 s); %s after A's link returned A's heartbeat saw the lost lease and cancelled the run (log ALERT export_lease_lost); the multipart upload was aborted %s after MinIO resumed (cleanup log %s after the resume); no object; row stays worker_lost; A serves a new job in %s",
			lostAfter.Round(10*time.Millisecond), cancelAfter.Round(10*time.Millisecond), aborted.Round(10*time.Millisecond), cleaned.Sub(cleanupAt).Round(10*time.Millisecond), next.Round(10*time.Millisecond))
		wA.stop(t)
		wB.stop(t)
	})

	t.Run("zombie_finish_rejected_object_deleted", func(t *testing.T) {
		px := newPGProxy(t, strings.SplitN(strings.SplitN(e.pgURL, "@", 2)[1], "/", 2)[0])
		pgViaProxy := strings.Replace(e.pgURL, strings.SplitN(strings.SplitN(e.pgURL, "@", 2)[1], "/", 2)[0], px.addr(), 1)
		wA := e.startWorker("wFA", tok, ten, "EXPORT_SWEEP_SECONDS=0", "EXPORT_PG_URL="+pgViaProxy)
		id := e.post(wA, tok, mid)
		waitFor(t, "job running", 30*time.Second, func() bool { return strings.HasPrefix(e.row(id), "RUNNING|") })
		wB := e.startWorker("wFB", tok, ten, "EXPORT_SWEEP_SECONDS=0")
		cut := time.Now()
		px.setBlocked(true) // A keeps streaming and writes the whole object, but can neither heartbeat nor finish
		waitFor(t, "worker B fails the job", 60*time.Second, func() bool { return strings.HasPrefix(e.row(id), "FAILED|worker_lost|") })
		lostAfter := time.Since(cut)
		waitFor(t, "zombie finished its object", 60*time.Second, func() bool { return e.objectExists(ten, id) })
		objAfter := time.Since(cut)
		time.Sleep(1 * time.Second)
		back := time.Now()
		px.setBlocked(false)
		ok := waitLog(wA, "ALERT export_finish_rejected job="+id, 30*time.Second)
		if ok.IsZero() {
			t.Fatalf("finish was not rejected/handled; log:\n%s", func() string { b, _ := os.ReadFile(wA.log); return string(b) }())
		}
		waitFor(t, "object deleted", 30*time.Second, func() bool { return !e.objectExists(ten, id) })
		delAfter := time.Since(back)
		if !strings.HasPrefix(e.row(id), "FAILED|worker_lost|") {
			t.Fatalf("row %s", e.row(id))
		}
		t.Logf("RESULT zombie that finishes (worker A's PostgreSQL link cut for the whole job): B marked it FAILED worker_lost %s after the cut; A had written the full object %s after the cut; %s after the link returned A's Finish was rejected by the owner guard and the unreferenced object was deleted (log ALERT export_finish_rejected); row stays worker_lost",
			lostAfter.Round(10*time.Millisecond), objAfter.Round(10*time.Millisecond), delAfter.Round(10*time.Millisecond))
		wA.stop(t)
		wB.stop(t)
	})

	t.Run("postgres_pause_longer_than_finish_retry", func(t *testing.T) {
		w := e.startWorker("wF", tok, ten, "EXPORT_SWEEP_SECONDS=0", "EXPORT_FINISH_RETRY=8s", "EXPORT_MAX_CONCURRENT_PER_TENANT=100")
		id := e.post(w, tok, mid)
		waitFor(t, "job running", 30*time.Second, func() bool { return strings.HasPrefix(e.row(id), "RUNNING|") })
		pause(e2ePG)
		defer unpause(e2ePG)
		paused0 := time.Now()
		ab := waitLog(w, "ALERT export_job_abandoned job="+id, 90*time.Second)
		if ab.IsZero() {
			t.Fatalf("job never abandoned; log:\n%s", func() string { b, _ := os.ReadFile(w.log); return string(b) }())
		}
		abandonAfter := ab.Sub(paused0)
		unpause(e2ePG)
		back := time.Now()
		waitFor(t, "pg back", 30*time.Second, func() bool { _, ok := e.pgTry("SELECT 1"); return ok })
		waitFor(t, "recovery marks worker_lost", 60*time.Second, func() bool { return strings.HasPrefix(e.row(id), "FAILED|worker_lost|") })
		lostAfter := time.Since(back)
		if !e.alive(w) {
			t.Fatal("worker crashed")
		}
		obj := e.objectExists(ten, id)
		next := nextOK(w)
		t.Logf("RESULT docker pause PostgreSQL for the rest of the job (finish retry 8 s, op timeout 15 s): the job was abandoned %s after the pause (log ALERT export_job_abandoned; a hung PostgreSQL costs one 15 s operation timeout before the retry budget is checked); %s after the unpause recovery marked it FAILED worker_lost; worker alive; unreferenced object left for the lifecycle rule=%v; new job served in %s",
			abandonAfter.Round(100*time.Millisecond), lostAfter.Round(100*time.Millisecond), obj, next.Round(10*time.Millisecond))
		w.stop(t)
	})
}
