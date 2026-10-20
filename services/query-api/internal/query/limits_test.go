package query

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const tenantB = "00000000-0000-0000-0000-0000000000b2"

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestLimiter(cfg LimiterConfig) (*Limiter, *clock) {
	c := &clock{t: time.Unix(1_000_000, 0)}
	l := NewLimiter(cfg)
	l.now = c.now
	return l, c
}

func TestBurstThen429AndRefill(t *testing.T) {
	l, clk := newTestLimiter(LimiterConfig{RatePerSec: 2, Burst: 3, MaxConcurrent: 100})
	try := func(ten string) (int, *apiError) {
		rel, ra, err := l.Acquire(ten)
		if err == nil {
			rel()
		}
		return ra, err
	}
	for i := 0; i < 3; i++ {
		if _, err := try(tenant); err != nil {
			t.Fatalf("burst %d rejected: %v", i, err)
		}
	}
	ra, err := try(tenant)
	if err == nil || err.status != 429 || err.Code != "rate_limited" || ra != 1 {
		t.Fatalf("want rate_limited ra=1, got %v ra=%d", err, ra)
	}
	// tenants isolated: B still has a full bucket.
	if _, err := try(tenantB); err != nil {
		t.Fatalf("tenant B affected by A: %v", err)
	}
	clk.add(500 * time.Millisecond) // 1 token at 2/s
	if _, err := try(tenant); err != nil {
		t.Fatalf("no refill: %v", err)
	}
	if _, err := try(tenant); err == nil {
		t.Fatal("refill exceeded rate")
	}
	clk.add(time.Hour) // refill caps at burst
	for i := 0; i < 3; i++ {
		if _, err := try(tenant); err != nil {
			t.Fatalf("after idle burst %d: %v", i, err)
		}
	}
	if _, err := try(tenant); err == nil {
		t.Fatal("bucket exceeded burst")
	}
}

func TestRetryAfterSlowRate(t *testing.T) {
	l, _ := newTestLimiter(LimiterConfig{RatePerSec: 0.25, Burst: 1, MaxConcurrent: 5})
	rel, _, _ := l.Acquire(tenant)
	rel()
	if _, ra, err := l.Acquire(tenant); err == nil || ra != 4 {
		t.Fatalf("ra=%d err=%v", ra, err)
	}
}

func TestConcurrencyCapAndRelease(t *testing.T) {
	l, _ := newTestLimiter(LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 2})
	r1, _, _ := l.Acquire(tenant)
	r2, _, _ := l.Acquire(tenant)
	if _, _, err := l.Acquire(tenant); err == nil || err.Code != "too_many_concurrent_queries" || err.status != 429 {
		t.Fatalf("want concurrency 429, got %v", err)
	}
	if r, _, err := l.Acquire(tenantB); err != nil {
		t.Fatalf("tenant B blocked by A: %v", err)
	} else {
		r()
	}
	r1()
	r1() // double release must not free a second slot
	if _, _, err := l.Acquire(tenant); err != nil {
		t.Fatalf("slot not released: %v", err)
	}
	if _, _, err := l.Acquire(tenant); err == nil {
		t.Fatal("double release freed extra slot")
	}
	r2()
}

func TestIdleEviction(t *testing.T) {
	l, clk := newTestLimiter(LimiterConfig{RatePerSec: 1, Burst: 1, MaxConcurrent: 2, IdleTTL: time.Minute})
	relA, _, _ := l.Acquire(tenant) // stays in flight: must not be evicted
	relB, _, _ := l.Acquire(tenantB)
	relB()
	clk.add(2 * time.Minute)
	l.Acquire("00000000-0000-0000-0000-0000000000c3") // triggers sweep
	if _, ok := l.tenants[tenantB]; ok {
		t.Fatal("idle tenant not evicted")
	}
	if _, ok := l.tenants[tenant]; !ok {
		t.Fatal("in-flight tenant evicted")
	}
	relA()
}

func TestMaxTenantsBound(t *testing.T) {
	l, clk := newTestLimiter(LimiterConfig{RatePerSec: 1, Burst: 1, MaxConcurrent: 1, MaxTenants: 3})
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		rel, _, err := l.Acquire(id)
		if err != nil {
			t.Fatal(err)
		}
		rel()
		clk.add(time.Millisecond)
	}
	if n := l.size(); n > 3 {
		t.Fatalf("tenants unbounded: %d", n)
	}
}

func TestLoadLimits(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		check   func(LimiterConfig, Caps) bool
	}{
		{"defaults", nil, false, func(c LimiterConfig, k Caps) bool { return c == DefaultLimiterConfig && k == DefaultCaps }},
		{"all set", map[string]string{"QUERY_RATE_PER_SEC": "2.5", "QUERY_BURST": "7", "QUERY_MAX_CONCURRENT": "3",
			"QUERY_IDLE_TTL_SECONDS": "60", "QUERY_MAX_TENANTS": "9", "QUERY_MAX_EXECUTION_SECONDS": "5",
			"QUERY_MAX_RESULT_ROWS": "50", "QUERY_MAX_MEMORY_BYTES": "1000"}, false,
			func(c LimiterConfig, k Caps) bool {
				return c.RatePerSec == 2.5 && c.Burst == 7 && c.MaxConcurrent == 3 && c.IdleTTL == time.Minute &&
					c.MaxTenants == 9 && k == Caps{5, 50, 1000}
			}},
		{"bad rate", map[string]string{"QUERY_RATE_PER_SEC": "fast"}, true, nil},
		{"zero burst", map[string]string{"QUERY_BURST": "0"}, true, nil},
		{"negative", map[string]string{"QUERY_MAX_CONCURRENT": "-1"}, true, nil},
		{"nan", map[string]string{"QUERY_RATE_PER_SEC": "NaN"}, true, nil},
		{"bad exec", map[string]string{"QUERY_MAX_EXECUTION_SECONDS": "1s"}, true, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, k, err := LoadLimits(func(s string) string { return tc.env[s] })
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v", err)
			}
			if !tc.wantErr && !tc.check(c, k) {
				t.Fatalf("got %+v %+v", c, k)
			}
		})
	}
}

// multiAuth maps bearer token -> principal so tests can use several tenants.
type multiAuth map[string]Principal

func (m multiAuth) Authenticate(r *http.Request) (Principal, error) {
	if p, ok := m[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]; ok {
		return p, nil
	}
	return Principal{}, ErrUnauthenticated
}

func httpSetup(t *testing.T, cfg LimiterConfig, caps Caps, backend http.HandlerFunc) (*httptest.Server, *Limiter) {
	ch := httptest.NewServer(backend)
	t.Cleanup(ch.Close)
	l := NewLimiter(cfg)
	pa := Principal{TenantID: tenant, Scopes: []string{ScopeQueryRead}}
	pb := Principal{TenantID: tenantB, Scopes: []string{ScopeQueryRead}}
	s := &Server{CHURL: ch.URL, Auth: multiAuth{"a": pa, "b": pb}, Limiter: l, Caps: caps}
	api := httptest.NewServer(s.Handler())
	t.Cleanup(api.Close)
	return api, l
}

func TestHTTP429Shape(t *testing.T) {
	api, _ := httpSetup(t, LimiterConfig{RatePerSec: 1, Burst: 1, MaxConcurrent: 5}, Caps{},
		func(w http.ResponseWriter, r *http.Request) {})
	body := `{` + base + `}`
	if c, b := post(t, api, "/v1/query/events", "a", body); c != 200 {
		t.Fatalf("first: %d %s", c, b)
	}
	req, _ := http.NewRequest("POST", api.URL+"/v1/query/events", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer a")
	req.Header.Set("X-Request-ID", "rid-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("status=%d retry-after=%q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	buf := new(strings.Builder)
	_, _ = bufCopy(buf, resp)
	for _, want := range []string{`"code":"rate_limited"`, `"trace_id":"rid-1"`, `"message"`, `"details"`} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("body missing %s: %s", want, buf.String())
		}
	}
	if c, _ := post(t, api, "/v1/query/events", "b", body); c != 200 {
		t.Fatalf("tenant b limited by a: %d", c)
	}
	// unauthenticated callers must not consume any tenant state.
	if c, _ := post(t, api, "/v1/query/events", "nope", body); c != 401 {
		t.Fatalf("want 401, got %d", c)
	}
}

func bufCopy(b *strings.Builder, resp *http.Response) (int, error) {
	buf := make([]byte, 4096)
	n, err := resp.Body.Read(buf)
	b.Write(buf[:n])
	return n, err
}

func TestHTTPConcurrencyReleasedOnSuccessErrorAndCancel(t *testing.T) {
	started := make(chan struct{}, 4)
	gate := make(chan struct{})
	mode := make(chan string, 1)
	api, l := httpSetup(t, LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 1}, Caps{},
		func(w http.ResponseWriter, r *http.Request) {
			switch <-mode {
			case "block":
				started <- struct{}{}
				select {
				case <-gate:
				case <-r.Context().Done():
				}
			case "fail":
				http.Error(w, "boom", 500)
			}
		})
	body := `{` + base + `}`
	inflight := func() int {
		l.mu.Lock()
		defer l.mu.Unlock()
		if st := l.tenants[tenant]; st != nil {
			return st.inflight
		}
		return 0
	}
	waitIdle := func() {
		for i := 0; i < 200 && inflight() != 0; i++ {
			time.Sleep(10 * time.Millisecond)
		}
		if inflight() != 0 {
			t.Fatal("slot leaked")
		}
	}

	// success path holds the slot while in flight; second request is 429.
	mode <- "block"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequestWithContext(ctx, "POST", api.URL+"/v1/query/events", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer a")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	<-started
	c, b := post(t, api, "/v1/query/events", "a", body)
	if c != 429 || !strings.Contains(b, "too_many_concurrent_queries") {
		t.Fatalf("want concurrency 429: %d %s", c, b)
	}
	mode <- "ok"
	if c, _ := post(t, api, "/v1/query/events", "b", body); c == 429 {
		t.Fatal("tenant b blocked by a")
	}
	cancel() // client cancel releases the slot
	<-done
	waitIdle()

	// backend error releases the slot
	mode <- "fail"
	if c, _ := post(t, api, "/v1/query/events", "a", body); c != 502 {
		t.Fatalf("want 502, got %d", c)
	}
	waitIdle()

	// normal completion releases the slot
	mode <- "ok"
	if c, _ := post(t, api, "/v1/query/events", "a", body); c != 200 {
		t.Fatalf("want 200, got %d", c)
	}
	waitIdle()
	close(gate)
}

func TestCapsPassedToClickHouse(t *testing.T) {
	var got map[string][]string
	api, _ := httpSetup(t, LimiterConfig{}, Caps{MaxExecutionSeconds: 7, MaxResultRows: 55, MaxMemoryBytes: 123456},
		func(w http.ResponseWriter, r *http.Request) { got = r.URL.Query() })
	if c, b := post(t, api, "/v1/query/events", "a", `{`+base+`}`); c != 200 {
		t.Fatalf("%d %s", c, b)
	}
	for k, v := range map[string]string{"max_execution_time": "7", "max_result_rows": "55", "max_memory_usage": "123456", "readonly": "1"} {
		if got[k] == nil || got[k][0] != v {
			t.Fatalf("%s = %v, want %s", k, got[k], v)
		}
	}
}
