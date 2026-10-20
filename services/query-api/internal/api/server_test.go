package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeAn struct {
	tenants []string
	events  []Event
	after   *Keyset
	err     error
}

func (f *fakeAn) QueryEvents(_ context.Context, tenant string, r EventsRequest, after *Keyset) ([]Event, error) {
	f.tenants = append(f.tenants, tenant)
	f.after = after
	if f.err != nil {
		return nil, f.err
	}
	return f.events, nil
}
func (f *fakeAn) Aggregate(_ context.Context, tenant string, r AggregateRequest) (AggregateResult, error) {
	f.tenants = append(f.tenants, tenant)
	return AggregateResult{Rows: []map[string]any{{"events": int64(1)}}}, nil
}
func (f *fakeAn) MetricsSummary(_ context.Context, tenant string, _ TimeRange, _ string) (MetricsSummary, error) {
	f.tenants = append(f.tenants, tenant)
	return MetricsSummary{}, nil
}
func (f *fakeAn) QualitySummary(_ context.Context, tenant string, _ TimeRange, _ string) (QualitySummary, error) {
	f.tenants = append(f.tenants, tenant)
	return QualitySummary{}, nil
}
func (f *fakeAn) Ping(context.Context) error { return f.err }

type fakeCP struct {
	tenants []string
	sources []Source
	jobs    []Job
}

func (f *fakeCP) ListSources(_ context.Context, tenant string, p ListPage) ([]Source, error) {
	f.tenants = append(f.tenants, tenant)
	return f.sources, nil
}
func (f *fakeCP) ListJobs(_ context.Context, tenant string, _ JobFilter, p ListPage) ([]Job, error) {
	f.tenants = append(f.tenants, tenant)
	return f.jobs, nil
}
func (f *fakeCP) Ping(context.Context) error { return nil }

var allScopes = []string{ScopeEventsRead, ScopeMetricsRead, ScopeSourcesRead, ScopeJobsRead}

func newTestServer(an *fakeAn, cp *fakeCP, scopes []string, burst int) (*Server, http.Handler) {
	cfg := Config{CursorKey: testKey, RateLimitRPS: 1, RateLimitBurst: burst}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Scopes: scopes}}
	s := NewServer(cfg, auth, an, cp, nil)
	return s, s.Handler()
}

func do(h http.Handler, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

const evBody = `{"time_range":{"from":"2026-09-30T00:00:00Z","to":"2026-10-01T00:00:00Z"},"limit":2}`

func TestAuthAndErrorShape(t *testing.T) {
	_, h := newTestServer(&fakeAn{}, &fakeCP{}, allScopes, 100)
	r := httptest.NewRequest("POST", "/v1/query/events", strings.NewReader(evBody))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("code %d", w.Code)
	}
	var e apiError
	if json.Unmarshal(w.Body.Bytes(), &e) != nil || e.Code != "UNAUTHENTICATED" || e.TraceID == "" || e.TraceID != w.Header().Get("X-Request-ID") {
		t.Fatalf("bad error %+v / %s", e, w.Body)
	}
	w = do(h, "GET", "/v1/jobs", "", map[string]string{"X-Request-ID": "my-req-1"})
	if w.Header().Get("X-Request-ID") != "my-req-1" {
		t.Fatal("request id not echoed")
	}
}

func TestScopeRequired(t *testing.T) {
	an := &fakeAn{}
	_, h := newTestServer(an, &fakeCP{}, []string{ScopeJobsRead}, 100)
	for _, c := range [][2]string{{"POST", "/v1/query/events"}, {"POST", "/v1/query/aggregate"}, {"GET", "/v1/metrics/summary"},
		{"GET", "/v1/quality/summary"}, {"GET", "/v1/sources"}} {
		if w := do(h, c[0], c[1], evBody, nil); w.Code != 403 {
			t.Errorf("%v: code %d", c, w.Code)
		}
	}
	if w := do(h, "GET", "/v1/jobs", "", nil); w.Code != 200 {
		t.Errorf("jobs: %d", w.Code)
	}
	if len(an.tenants) != 0 {
		t.Fatal("store reached without scope")
	}
}

func TestTenantOnlyFromToken(t *testing.T) {
	an, cp := &fakeAn{}, &fakeCP{}
	_, h := newTestServer(an, cp, allScopes, 100)
	// tenant in body is rejected as an unknown field, never honoured
	body := `{"tenant_id":"` + tenantB + `","time_range":{"from":"2026-09-30T00:00:00Z","to":"2026-10-01T00:00:00Z"}}`
	if w := do(h, "POST", "/v1/query/events", body, nil); w.Code != 400 {
		t.Fatalf("code %d", w.Code)
	}
	hdr := map[string]string{"X-Tenant-ID": tenantB}
	do(h, "POST", "/v1/query/events", evBody, hdr)
	do(h, "POST", "/v1/query/aggregate", evBody, hdr)
	do(h, "GET", "/v1/metrics/summary?tenant_id="+tenantB, "", hdr)
	do(h, "GET", "/v1/quality/summary?tenant_id="+tenantB, "", hdr)
	do(h, "GET", "/v1/sources?tenant_id="+tenantB, "", hdr)
	do(h, "GET", "/v1/jobs?tenant_id="+tenantB, "", hdr)
	all := append(append([]string{}, an.tenants...), cp.tenants...)
	if len(all) != 6 {
		t.Fatalf("expected 6 store calls, got %d", len(all))
	}
	for _, tn := range all {
		if tn != tenantA {
			t.Fatalf("store saw tenant %s", tn)
		}
	}
}

func mkEvents(n int) []Event {
	base := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	out := make([]Event, n)
	for i := range out {
		out[i] = Event{EventID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa0" + string(rune('0'+i)), EventTime: base.Add(-time.Duration(i) * time.Second)}
	}
	return out
}

func TestEventsPaginationAndCursorBinding(t *testing.T) {
	an := &fakeAn{events: mkEvents(3)} // limit 2 -> 3 rows = has next page
	_, h := newTestServer(an, &fakeCP{}, allScopes, 100)
	w := do(h, "POST", "/v1/query/events", evBody, nil)
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var resp struct {
		Items      []Event `json:"items"`
		NextCursor *string `json:"next_cursor"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Items) != 2 || resp.NextCursor == nil {
		t.Fatalf("bad page %+v", resp)
	}
	// follow cursor
	next := `{"time_range":{"from":"2026-09-30T00:00:00Z","to":"2026-10-01T00:00:00Z"},"limit":2,"cursor":"` + *resp.NextCursor + `"}`
	if w = do(h, "POST", "/v1/query/events", next, nil); w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if an.after == nil || !an.after.Time.Equal(resp.Items[1].EventTime) || an.after.ID != resp.Items[1].EventID {
		t.Fatalf("keyset %+v", an.after)
	}
	// different filters with the same cursor -> rejected
	other := `{"time_range":{"from":"2026-09-29T00:00:00Z","to":"2026-10-01T00:00:00Z"},"limit":2,"cursor":"` + *resp.NextCursor + `"}`
	if w = do(h, "POST", "/v1/query/events", other, nil); w.Code != 400 || !strings.Contains(w.Body.String(), "CURSOR_INVALID") {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	// tampered
	bad := `{"time_range":{"from":"2026-09-30T00:00:00Z","to":"2026-10-01T00:00:00Z"},"limit":2,"cursor":"` + (*resp.NextCursor)[:len(*resp.NextCursor)-2] + `AA"}`
	if w = do(h, "POST", "/v1/query/events", bad, nil); w.Code != 400 {
		t.Fatalf("tampered cursor: %d", w.Code)
	}
}

func TestCursorFromAnotherTenantRejected(t *testing.T) {
	an := &fakeAn{events: mkEvents(3)}
	s, h := newTestServer(an, &fakeCP{}, allScopes, 100)
	var req EventsRequest
	json.Unmarshal([]byte(evBody), &req)
	req.Normalize()
	foreign := s.cursors.Encode("events", tenantB, req.queryHash(), time.Now(), srcID)
	body := `{"time_range":{"from":"2026-09-30T00:00:00Z","to":"2026-10-01T00:00:00Z"},"limit":2,"cursor":"` + foreign + `"}`
	if w := do(h, "POST", "/v1/query/events", body, nil); w.Code != 400 {
		t.Fatalf("%d", w.Code)
	}
	if len(an.tenants) != 0 {
		t.Fatal("store reached with foreign cursor")
	}
}

func TestHardLimitsAndBadBodies(t *testing.T) {
	_, h := newTestServer(&fakeAn{}, &fakeCP{}, allScopes, 100)
	tr := `"time_range":{"from":"2026-09-30T00:00:00Z","to":"2026-10-01T00:00:00Z"}`
	for name, b := range map[string]string{
		"limit":      `{` + tr + `,"limit":100000}`,
		"sql field":  `{` + tr + `,"where":"1=1"}`,
		"not json":   `{`,
		"trailing":   `{` + tr + `}{}`,
		"big filter": `{` + tr + `,"filters":{"event_types":["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u"]}}`,
	} {
		if w := do(h, "POST", "/v1/query/events", b, nil); w.Code != 400 {
			t.Errorf("%s: %d", name, w.Code)
		}
	}
	big := `{"x":"` + strings.Repeat("a", maxBodyBytes) + `"}`
	if w := do(h, "POST", "/v1/query/events", big, nil); w.Code != 413 {
		t.Errorf("big body: %d", w.Code)
	}
	if w := do(h, "GET", "/v1/jobs?limit=500", "", nil); w.Code != 400 {
		t.Errorf("jobs limit: %d", w.Code)
	}
	if w := do(h, "GET", "/v1/jobs?status=DROP", "", nil); w.Code != 400 {
		t.Errorf("jobs status: %d", w.Code)
	}
	if w := do(h, "GET", "/v1/metrics/summary?from=2020-01-01T00:00:00Z", "", nil); w.Code != 400 {
		t.Errorf("range: %d", w.Code)
	}
}

func TestRateLimit429(t *testing.T) {
	_, h := newTestServer(&fakeAn{}, &fakeCP{}, allScopes, 2)
	for i := 0; i < 2; i++ {
		if w := do(h, "GET", "/v1/jobs", "", nil); w.Code != 200 {
			t.Fatalf("%d", w.Code)
		}
	}
	w := do(h, "GET", "/v1/jobs", "", nil)
	if w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("%d %v", w.Code, w.Header())
	}
}

func TestUpstreamErrorIsGeneric(t *testing.T) {
	_, h := newTestServer(&fakeAn{err: errors.New("dial tcp 10.0.0.1: secret-host")}, &fakeCP{}, allScopes, 100)
	w := do(h, "POST", "/v1/query/events", evBody, nil)
	if w.Code != 503 || strings.Contains(w.Body.String(), "secret-host") {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w = do(h, "GET", "/healthz", "", nil); w.Code != 503 {
		t.Fatalf("health %d", w.Code)
	}
}

func TestListsPaginate(t *testing.T) {
	now := time.Now().UTC()
	cp := &fakeCP{sources: []Source{{ID: srcID, CreatedAt: now}, {ID: tenantB, CreatedAt: now}}}
	_, h := newTestServer(&fakeAn{}, cp, allScopes, 100)
	w := do(h, "GET", "/v1/sources?limit=1", "", nil)
	var resp struct {
		Items      []Source `json:"items"`
		NextCursor *string  `json:"next_cursor"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if w.Code != 200 || len(resp.Items) != 1 || resp.NextCursor == nil {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w = do(h, "GET", "/v1/sources?limit=1&cursor="+*resp.NextCursor, "", nil); w.Code != 200 {
		t.Fatalf("%d", w.Code)
	}
	// a sources cursor must not work on jobs
	if w = do(h, "GET", "/v1/jobs?limit=1&cursor="+*resp.NextCursor, "", nil); w.Code != 400 {
		t.Fatalf("cross-kind %d", w.Code)
	}
}

func TestConfig(t *testing.T) {
	env := map[string]string{}
	get := func(k string) string { return env[k] }
	if _, err := LoadConfig(get); err == nil {
		t.Fatal("empty config accepted")
	}
	env["ASTRYON_DEV_AUTH"], env["ASTRYON_DEV_TOKEN"], env["ASTRYON_DEV_TENANT_ID"] = "1", "t", tenantA
	env["ASTRYON_CH_URL"] = "http://x"
	if _, err := LoadConfig(get); err == nil {
		t.Fatal("missing cursor key accepted")
	}
	env["ASTRYON_QUERY_CURSOR_KEY"] = string(testKey)
	if c, err := LoadConfig(get); err != nil || len(c.DevScopes) != 4 {
		t.Fatalf("%v %+v", err, c)
	}
}
