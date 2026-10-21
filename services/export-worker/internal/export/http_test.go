package export

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type gateStore struct {
	*memStore
	started chan struct{}
	release chan struct{}
	err     error
}

func (g *gateStore) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	g.started <- struct{}{}
	select {
	case <-g.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if g.err != nil {
		return 0, g.err
	}
	return g.memStore.Put(ctx, key, r)
}

func newGate() *gateStore {
	return &gateStore{memStore: newMemStore(), started: make(chan struct{}, 10), release: make(chan struct{})}
}

func authFor(tok, tenant string, scopes ...string) Authenticator {
	return DevStaticAuthenticator{Token: tok, Principal: Principal{TenantID: tenant, Subject: "u1", Scopes: scopes}}
}

// multiAuth maps tokens to principals.
type multiAuth map[string]Principal

func (m multiAuth) Authenticate(r *http.Request) (Principal, error) {
	t := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if p, ok := m[t]; ok {
		return p, nil
	}
	return Principal{}, ErrUnauthenticated
}

var allScopes = []string{ScopeExportWrite, ScopeExportRead}

func testAuth() Authenticator {
	return multiAuth{
		"a":      {TenantID: tenantA, Subject: "ua", Scopes: allScopes},
		"b":      {TenantID: tenantB, Subject: "ub", Scopes: allScopes},
		"wonly":  {TenantID: tenantA, Scopes: []string{ScopeExportWrite}},
		"ronly":  {TenantID: tenantA, Scopes: []string{ScopeExportRead}},
		"notnt":  {TenantID: "", Scopes: allScopes},
		"badscp": {TenantID: tenantA, Scopes: []string{"astryon:query:read"}},
	}
}

type env struct {
	srv   *Server
	h     http.Handler
	store ObjectStore
	audit *MemoryAudit
	jobs  *MemoryJobStore
}

func newEnv(t *testing.T, st ObjectStore, q QuotaChecker, workers int) *env {
	t.Helper()
	ch := newFakeCH(ndjsonRows(3), 200)
	t.Cleanup(ch.Close)
	au := &MemoryAudit{}
	r := newRunner(ch, st, au)
	jobs := &MemoryJobStore{}
	s := &Server{Auth: testAuth(), Runner: r, Quota: q, Audit: au, Jobs: jobs, Workers: workers}
	return &env{srv: s, h: s.Handler(), store: st, audit: au, jobs: jobs}
}

func (e *env) do(method, path, tok, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	return w
}

const specBody = `{"format":"ndjson","from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z","processing_version":1,"max_rows":100}`

func errCodeOf(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var b struct {
		Error struct {
			Code    string `json:"code"`
			TraceID string `json:"trace_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("body %q: %v", w.Body.String(), err)
	}
	if b.Error.TraceID == "" || w.Header().Get("X-Request-ID") == "" {
		t.Fatalf("missing trace id / request id: %s", w.Body.String())
	}
	return b.Error.Code
}

func decodeMap(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	m := map[string]any{}
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("%q: %v", w.Body.String(), err)
	}
	return m
}

func waitStatus(t *testing.T, e *env, tok, id, want string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		w := e.do("GET", "/v1/exports/"+id, tok, "")
		m := decodeMap(t, w)
		if m["status"] == want {
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("status %v, want %s", m["status"], want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAuthPaths(t *testing.T) {
	e := newEnv(t, newMemStore(), nil, 1)
	cases := []struct {
		method, path, tok string
		status            int
		code              string
	}{
		{"POST", "/v1/exports", "", 401, "unauthenticated"},
		{"POST", "/v1/exports", "bogus", 401, "unauthenticated"},
		{"GET", "/v1/exports/exp-00000001", "", 401, "unauthenticated"},
		{"POST", "/v1/exports", "ronly", 403, "forbidden"},
		{"POST", "/v1/exports", "badscp", 403, "forbidden"},
		{"GET", "/v1/exports/exp-00000001", "wonly", 403, "forbidden"},
		{"POST", "/v1/exports", "notnt", 403, "forbidden"},
	}
	for _, c := range cases {
		w := e.do(c.method, c.path, c.tok, specBody)
		if w.Code != c.status || errCodeOf(t, w) != c.code {
			t.Errorf("%s %s tok=%q: %d %s", c.method, c.path, c.tok, w.Code, w.Body.String())
		}
	}
	if len(e.audit.Records()) != 0 {
		t.Error("nothing should have run")
	}
}

func TestBodyValidation(t *testing.T) {
	e := newEnv(t, newMemStore(), nil, 1)
	for name, body := range map[string]string{
		"tenant_id in body": `{"tenant_id":"` + tenantB + `","from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z","processing_version":1}`,
		"unknown field":     `{"foo":1}`,
		"malformed":         `{`,
	} {
		w := e.do("POST", "/v1/exports", "a", body)
		if w.Code != 400 || errCodeOf(t, w) != "invalid_request" {
			t.Errorf("%s: %d %s", name, w.Code, w.Body.String())
		}
	}
	w := e.do("POST", "/v1/exports", "a", `{"from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z"}`)
	if w.Code != 400 || errCodeOf(t, w) != "invalid_spec" {
		t.Errorf("missing processing_version: %d %s", w.Code, w.Body.String())
	}
	if w := e.do("POST", "/v1/exports", "a", strings.Repeat("x", maxBody+1)); w.Code != 413 {
		t.Errorf("large body: %d", w.Code)
	}
}

func TestLifecycleSuccessAndSignedURL(t *testing.T) {
	g := newGate()
	e := newEnv(t, g, nil, 1)
	w := e.do("POST", "/v1/exports", "a", specBody)
	if w.Code != 202 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	m := decodeMap(t, w)
	id := m["id"].(string)
	if m["status"] != "QUEUED" {
		t.Fatalf("%v", m)
	}
	<-g.started
	m = waitStatus(t, e, "a", id, "RUNNING")
	if _, ok := m["download_url"]; ok {
		t.Error("url while RUNNING")
	}
	close(g.release)
	m = waitStatus(t, e, "a", id, "SUCCEEDED")
	u, _ := m["download_url"].(string)
	if !strings.Contains(u, "exports/"+tenantA+"/"+id+".ndjson") || !strings.Contains(u, "X-Amz-Signature=") {
		t.Errorf("url %q", u)
	}
	if m["rows"].(float64) != 3 || m["bytes"].(float64) == 0 || m["download_expires_at"] == nil {
		t.Errorf("%v", m)
	}
	// never stored: job struct has no url, and a second GET signs a fresh one (same here, but computed per request)
	j, _ := e.jobs.Get(tenantA, id)
	if j.Status != StatusSucceeded || j.ObjectKey == "" {
		t.Errorf("%+v", j)
	}
	// audit: one success record
	recs := e.audit.Records()
	if len(recs) != 1 || recs[0].Outcome != OutcomeSuccess {
		t.Errorf("%+v", recs)
	}
	// Retry-safe: unknown id
	if w := e.do("GET", "/v1/exports/does-not-exist", "a", ""); w.Code != 404 {
		t.Errorf("%d", w.Code)
	}
}

func TestLifecycleFailed(t *testing.T) {
	g := newGate()
	g.err = errBoom
	close(g.release)
	e := newEnv(t, g, nil, 1)
	id := decodeMap(t, e.do("POST", "/v1/exports", "a", specBody))["id"].(string)
	m := waitStatus(t, e, "a", id, "FAILED")
	if m["error_code"] != "export_failed" {
		t.Errorf("%v", m)
	}
	if _, ok := m["download_url"]; ok {
		t.Error("url on FAILED")
	}
	if strings.Contains(w2s(m), "boom") {
		t.Error("internal error leaked")
	}
}

func w2s(m map[string]any) string { b, _ := json.Marshal(m); return string(b) }

func TestRowCapFailureCode(t *testing.T) {
	e := newEnv(t, newMemStore(), nil, 1)
	body := `{"from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z","processing_version":1,"max_rows":1}`
	id := decodeMap(t, e.do("POST", "/v1/exports", "a", body))["id"].(string)
	m := waitStatus(t, e, "a", id, "FAILED")
	if m["error_code"] != "row_cap_exceeded" {
		t.Errorf("%v", m)
	}
}

func TestCrossTenantIs404(t *testing.T) {
	e := newEnv(t, newMemStore(), nil, 1)
	id := decodeMap(t, e.do("POST", "/v1/exports", "a", specBody))["id"].(string)
	waitStatus(t, e, "a", id, "SUCCEEDED")
	w := e.do("GET", "/v1/exports/"+id, "b", "")
	if w.Code != 404 || errCodeOf(t, w) != "not_found" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "download_url") {
		t.Error("leak")
	}
}

func TestQuota429(t *testing.T) {
	g := newGate()
	e := newEnv(t, g, &MemoryQuota{MaxConcurrent: 1}, 1)
	if w := e.do("POST", "/v1/exports", "a", specBody); w.Code != 202 {
		t.Fatalf("%d", w.Code)
	}
	w := e.do("POST", "/v1/exports", "a", specBody)
	if w.Code != 429 || errCodeOf(t, w) != "too_many_concurrent" || w.Header().Get("Retry-After") == "" {
		t.Fatalf("%d %v %s", w.Code, w.Header(), w.Body.String())
	}
	// another tenant is unaffected
	if w := e.do("POST", "/v1/exports", "b", specBody); w.Code != 202 {
		t.Fatalf("%d", w.Code)
	}
	close(g.release)
	e.srv.Shutdown(context.Background())

	// daily quota
	e2 := newEnv(t, newMemStore(), &MemoryQuota{MaxPerDay: 1}, 1)
	if w := e2.do("POST", "/v1/exports", "a", specBody); w.Code != 202 {
		t.Fatalf("%d", w.Code)
	}
	w = e2.do("POST", "/v1/exports", "a", specBody)
	if w.Code != 429 || errCodeOf(t, w) != "quota_exceeded" || w.Header().Get("Retry-After") == "" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	var found bool
	for _, r := range e2.audit.Records() {
		found = found || r.Outcome == OutcomeQuotaExceeded
	}
	if !found {
		t.Error("quota rejection not audited")
	}
	e2.srv.Shutdown(context.Background())
}

func TestBoundedPoolAndQueueFull(t *testing.T) {
	g := newGate()
	e := newEnv(t, g, nil, 1)
	e.srv.MaxQueue = 1
	id1 := decodeMap(t, e.do("POST", "/v1/exports", "a", specBody))["id"].(string)
	<-g.started // job1 holds the only worker
	id2 := decodeMap(t, e.do("POST", "/v1/exports", "a", specBody))["id"].(string)
	if m := decodeMap(t, e.do("GET", "/v1/exports/"+id2, "a", "")); m["status"] != "QUEUED" {
		t.Errorf("second job should wait: %v", m)
	}
	if w := e.do("POST", "/v1/exports", "a", specBody); w.Code != 503 || errCodeOf(t, w) != "queue_full" {
		t.Errorf("%d %s", w.Code, w.Body.String())
	}
	close(g.release)
	waitStatus(t, e, "a", id1, "SUCCEEDED")
	waitStatus(t, e, "a", id2, "SUCCEEDED")
}

func TestShutdownMarksUnfinishedFailed(t *testing.T) {
	g := newGate() // never released: Put blocks until ctx cancelled
	e := newEnv(t, g, nil, 1)
	id1 := decodeMap(t, e.do("POST", "/v1/exports", "a", specBody))["id"].(string)
	<-g.started
	id2 := decodeMap(t, e.do("POST", "/v1/exports", "a", specBody))["id"].(string) // stays queued
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	e.srv.Shutdown(ctx)
	for _, id := range []string{id1, id2} {
		m := decodeMap(t, e.do("GET", "/v1/exports/"+id, "a", ""))
		if m["status"] != "FAILED" || m["error_code"] != "shutdown" {
			t.Errorf("%s: %v", id, m)
		}
	}
	if w := e.do("POST", "/v1/exports", "a", specBody); w.Code != 503 {
		t.Errorf("after shutdown: %d", w.Code)
	}
}

func TestShutdownWaitsForRunning(t *testing.T) {
	g := newGate()
	e := newEnv(t, g, nil, 1)
	id := decodeMap(t, e.do("POST", "/v1/exports", "a", specBody))["id"].(string)
	<-g.started
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); e.srv.Shutdown(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	close(g.release)
	wg.Wait()
	if m := decodeMap(t, e.do("GET", "/v1/exports/"+id, "a", "")); m["status"] != "SUCCEEDED" {
		t.Errorf("graceful shutdown should let it finish: %v", m)
	}
}

// downJobs simulates an unreachable PostgreSQL behind the job store.
type downJobs struct {
	*MemoryJobStore
	err error
}

func (d downJobs) Create(Job) error                { return d.err }
func (d downJobs) Get(string, string) (Job, error) { return Job{}, d.err }

func TestStoreOutageIs503(t *testing.T) {
	e := newEnv(t, newMemStore(), nil, 1)
	e.srv.Jobs = downJobs{e.jobs, fmt.Errorf("begin: %w", &pgconn.ConnectError{Config: &pgconn.Config{}})}
	e.h = e.srv.Handler()
	for _, w := range []*httptest.ResponseRecorder{
		e.do("POST", "/v1/exports", "a", specBody),
		e.do("GET", "/v1/exports/"+strings.Repeat("a", 12), "a", ""),
	} {
		if w.Code != 503 || errCodeOf(t, w) != "unavailable" || w.Header().Get("Retry-After") == "" {
			t.Fatalf("%d %v %s", w.Code, w.Header(), w.Body.String())
		}
	}
	e.srv.Jobs = downJobs{e.jobs, errors.New("syntax error")}
	e.h = e.srv.Handler()
	if w := e.do("POST", "/v1/exports", "a", specBody); w.Code != 500 {
		t.Fatalf("non-outage error: %d", w.Code)
	}
}
