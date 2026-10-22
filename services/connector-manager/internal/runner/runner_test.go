package runner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/astryon/connector-manager/internal/pullstore"
)

const (
	tenant = "0a1b2c3d-1111-4222-8333-444455556666"
	source = "0a1b2c3d-1111-4222-8333-444455557777"
	connID = "0a1b2c3d-1111-4222-8333-444455558888"
)

var now = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

type store struct {
	c        pullstore.Claimed
	claimed  bool
	succeed  []string // next cursors
	every    []time.Duration
	records  int64
	failCode string
	delay    time.Duration
	fenceErr error
}

func (s *store) Claim(context.Context) (pullstore.Claimed, error) {
	if s.claimed {
		return pullstore.Claimed{}, pullstore.ErrNoConnector
	}
	s.claimed = true
	return s.c, nil
}
func (s *store) Succeed(_ context.Context, _ pullstore.Claimed, next string, every time.Duration, n int64) error {
	s.succeed, s.every, s.records = append(s.succeed, next), append(s.every, every), s.records+n
	return s.fenceErr
}
func (s *store) Fail(_ context.Context, _ pullstore.Claimed, d time.Duration, code string) error {
	s.failCode, s.delay = code, d
	return nil
}

type emitter struct {
	events [][]byte
	err    error
}

func (e *emitter) Emit(_ context.Context, _, _ string, ev [][]byte) error {
	if e.err != nil {
		return e.err
	}
	e.events = append(e.events, ev...)
	return nil
}

type resolver struct{ auth string }

func (r resolver) Authorization(context.Context, string) (string, error) { return r.auth, nil }

func claimed(url string, extra map[string]any) pullstore.Claimed {
	cfg := map[string]any{"endpoint": url, "cursor_parameter": "after", "schema_id": "orders", "id_field": "id", "event_time_field": "ts",
		"entity_field": "customer", "requests_per_second": 1000.0, "interval_seconds": 60.0}
	for k, v := range extra {
		cfg[k] = v
	}
	return pullstore.Claimed{TenantID: tenant, ConnectorID: connID, SourceID: source, Config: cfg, Cursor: "c1", Fence: 3}
}

func newRunner(s *store, e *emitter) *Runner {
	return New(Config{Store: s, Emitter: e, Resolver: resolver{"Bearer tok"}, Now: func() time.Time { return now }, Jitter: func() float64 { return 0 }})
}

const page = `[{"id":"a","ts":"2026-10-21T10:00:00Z","customer":"c-1"},{"id":"b","ts":1760000000},"not an object",{"id":"c"}]`

func TestPullEmitsEnvelopesThenAdvancesCursor(t *testing.T) {
	var gotCursor, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCursor, gotAuth = r.URL.Query().Get("after"), r.Header.Get("Authorization")
		w.Header().Set("X-Next-Cursor", "c2")
		_, _ = w.Write([]byte(page))
	}))
	defer srv.Close()
	s, e := &store{c: claimed(srv.URL, nil)}, &emitter{}
	s.c.SecretRef = "vault://x"
	ok, err := newRunner(s, e).RunOnce(context.Background())
	if !ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if gotCursor != "c1" || gotAuth != "Bearer tok" {
		t.Fatalf("cursor=%q auth=%q", gotCursor, gotAuth)
	}
	if len(e.events) != 3 || s.records != 3 || len(s.succeed) != 1 || s.succeed[0] != "c2" || s.every[0] != 0 {
		t.Fatalf("events=%d succeed=%v every=%v", len(e.events), s.succeed, s.every)
	}
	var env map[string]any
	_ = json.Unmarshal(e.events[0], &env)
	if env["tenant_id"] != tenant || env["source_id"] != source || env["schema_id"] != "orders" || env["entity_key"] != "c-1" || env["event_time"] != "2026-10-21T10:00:00Z" {
		t.Fatalf("envelope = %v", env)
	}
	var last map[string]any
	_ = json.Unmarshal(e.events[2], &last)
	if last["event_time_missing"] != true {
		t.Fatalf("record without time must be flagged: %v", last)
	}
}

func TestLastPageWaitsForIntervalAndKeepsCursor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`[]`)) }))
	defer srv.Close()
	s := &store{c: claimed(srv.URL, nil)}
	if _, err := newRunner(s, &emitter{}).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.succeed[0] != "" || s.every[0] != 60*time.Second {
		t.Fatalf("next=%q every=%v", s.succeed[0], s.every[0])
	}
}

func TestFailuresKeepCursorAndHonourRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "600")
		http.Error(w, "slow down", 429)
	}))
	defer srv.Close()
	s := &store{c: claimed(srv.URL, nil)}
	ok, err := newRunner(s, &emitter{}).RunOnce(context.Background())
	if !ok || err == nil || s.failCode != "http_429" || s.delay != 600*time.Second || len(s.succeed) != 0 {
		t.Fatalf("ok=%v err=%v code=%s delay=%v succeed=%v", ok, err, s.failCode, s.delay, s.succeed)
	}
	// emit failure must not advance the cursor either
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(page)) }))
	defer srv2.Close()
	s2 := &store{c: claimed(srv2.URL, nil)}
	s2.c.ConsecutiveFailures = 2
	if _, err := newRunner(s2, &emitter{err: errors.New("kafka down")}).RunOnce(context.Background()); err == nil || s2.failCode != "emit_failed" || len(s2.succeed) != 0 {
		t.Fatalf("err=%v code=%s", err, s2.failCode)
	}
	if s2.delay != 2*time.Minute { // 30s * 2^2, no jitter
		t.Fatalf("backoff delay = %v", s2.delay)
	}
}

func TestBadConfigAndBadResponseAreScheduledNotDropped(t *testing.T) {
	s := &store{c: claimed("http://x", map[string]any{"schema_id": ""})}
	if _, err := newRunner(s, &emitter{}).RunOnce(context.Background()); err == nil || s.failCode != "bad_config" {
		t.Fatalf("err=%v code=%s", err, s.failCode)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"oops":1}`)) }))
	defer srv.Close()
	s2 := &store{c: claimed(srv.URL, nil)}
	if _, err := newRunner(s2, &emitter{}).RunOnce(context.Background()); err == nil || s2.failCode != "bad_response" {
		t.Fatalf("err=%v code=%s", err, s2.failCode)
	}
	s3 := &store{c: claimed(srv.URL, nil)}
	s3.c.SecretRef = "vault://x"
	r := New(Config{Store: s3, Emitter: &emitter{}, Now: func() time.Time { return now }, Jitter: func() float64 { return 0 }}) // no resolver
	if _, err := r.RunOnce(context.Background()); err == nil || s3.failCode != "no_secret_resolver" {
		t.Fatalf("err=%v code=%s", err, s3.failCode)
	}
}

func TestFencedStoreWriteIsCountedAndNothingReemitted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(page)) }))
	defer srv.Close()
	s := &store{c: claimed(srv.URL, nil), fenceErr: pullstore.ErrFenced}
	r := newRunner(s, &emitter{})
	if _, err := r.RunOnce(context.Background()); !errors.Is(err, pullstore.ErrFenced) || r.M.Fenced.Load() != 1 {
		t.Fatalf("err=%v fenced=%d", err, r.M.Fenced.Load())
	}
}

func TestEventIDsAreStable(t *testing.T) {
	m := Mapping{SchemaID: "s", SchemaVersion: 1, IDField: "id", ProcessingVersion: 1}
	a, _, _ := MapPage([]byte(`[{"id":"x","v":1}]`), m, tenant, source, connID, now)
	b, _, _ := MapPage([]byte(`[{"id":"x","v":2}]`), m, tenant, source, connID, now.Add(time.Hour))
	var ea, eb map[string]any
	_ = json.Unmarshal(a[0], &ea)
	_ = json.Unmarshal(b[0], &eb)
	if ea["event_id"] != eb["event_id"] {
		t.Fatal("id_field identity must survive content changes")
	}
	m.IDField = ""
	c, _, _ := MapPage([]byte(`[{"v":1}]`), m, tenant, source, connID, now)
	d, _, _ := MapPage([]byte(`[{"v":1}]`), m, tenant, source, connID, now)
	e, _, _ := MapPage([]byte(`[{"v":2}]`), m, tenant, source, connID, now)
	if string(c[0]) != string(d[0]) || strings.Contains(string(e[0]), "\"event_id\":\""+strings.Split(strings.Split(string(c[0]), `"event_id":"`)[1], `"`)[0]) {
		t.Fatal("content-hash identity broken")
	}
	if _, _, err := MapPage([]byte(`{"items":5}`), Mapping{SchemaID: "s", SchemaVersion: 1, RecordsPath: "items", ProcessingVersion: 1}, tenant, source, connID, now); !errors.Is(err, ErrBadResponse) {
		t.Fatalf("err=%v", err)
	}
}

func TestBackoffHookReplacesBuiltInSchedule(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(page)) }))
	defer srv.Close()
	s := &store{c: claimed(srv.URL, nil)}
	s.c.ConsecutiveFailures = 2
	var gotCode string
	var gotFailures int
	r := newRunner(s, &emitter{err: errors.New("kafka down")})
	r.Backoff = func(code string, failures int, _ error, _ time.Duration) (time.Duration, error) {
		gotCode, gotFailures = code, failures
		return 7 * time.Second, nil
	}
	if _, err := r.RunOnce(context.Background()); err == nil || s.failCode != "emit_failed" || s.delay != 7*time.Second || gotCode != "emit_failed" || gotFailures != 2 || len(s.succeed) != 0 {
		t.Fatalf("err=%v code=%s delay=%v hook=%s/%d", err, s.failCode, s.delay, gotCode, gotFailures)
	}
	// a hook error must not strand the connector silently: the failure is returned
	s2 := &store{c: claimed(srv.URL, nil)}
	r2 := newRunner(s2, &emitter{err: errors.New("kafka down")})
	r2.Backoff = func(string, int, error, time.Duration) (time.Duration, error) { return 0, errors.New("bad policy") }
	if _, err := r2.RunOnce(context.Background()); err == nil || s2.failCode != "" {
		t.Fatalf("err=%v code=%q", err, s2.failCode)
	}
}
