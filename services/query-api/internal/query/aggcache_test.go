package query

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func rowsOf(n int) []map[string]any { return []map[string]any{{"count": n}} }

func TestAggCacheHitExpiryAndErrors(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newAggCache(10*time.Second, 10)
	c.now = func() time.Time { return now }
	var calls int
	fn := func() ([]map[string]any, error) { calls++; return rowsOf(calls), nil }
	for i := 0; i < 3; i++ {
		if _, err := c.get(context.Background(), "k", fn); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("calls=%d, want 1 (cached)", calls)
	}
	now = now.Add(11 * time.Second)
	r, _ := c.get(context.Background(), "k", fn)
	if calls != 2 || r[0]["count"] != 2 {
		t.Fatalf("expired entry was served: calls=%d rows=%v", calls, r)
	}
	boom := errors.New("boom")
	if _, err := c.get(context.Background(), "e", func() ([]map[string]any, error) { return nil, boom }); err != boom {
		t.Fatal(err)
	}
	if _, err := c.get(context.Background(), "e", fn); err != nil {
		t.Fatalf("error must not be cached: %v", err)
	}
}

func TestAggCacheSingleflight(t *testing.T) {
	c := newAggCache(time.Minute, 10)
	var calls atomic.Int32
	release := make(chan struct{})
	fn := func() ([]map[string]any, error) { calls.Add(1); <-release; return rowsOf(1), nil }
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = c.get(context.Background(), "k", fn) }()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1 shared query", calls.Load())
	}
}

func TestAggCacheBoundedAndTenantKey(t *testing.T) {
	c := newAggCache(time.Minute, 3)
	for i := 0; i < 20; i++ {
		_, _ = c.get(context.Background(), string(rune('a'+i)), func() ([]map[string]any, error) { return rowsOf(i), nil })
	}
	if len(c.entries) > 3 {
		t.Fatalf("entries=%d exceeds bound", len(c.entries))
	}
	p := url.Values{"param_x": {"1"}}
	if aggKey("t1", "SELECT 1", p) == aggKey("t2", "SELECT 1", p) {
		t.Fatal("tenant must be part of the key")
	}
}

func TestAggregateServedFromCache(t *testing.T) {
	var hits atomic.Int32
	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"count":"3"}` + "\n"))
	}))
	defer ch.Close()
	const other = "00000000-0000-0000-0000-0000000000b2"
	auth := MultiAuth{"tok": Principal{TenantID: tenant, Scopes: []string{ScopeQueryRead}}, "tok2": Principal{TenantID: other, Scopes: []string{ScopeQueryRead}}}
	s := &Server{CHURL: ch.URL, Auth: auth, AggCacheTTL: time.Minute}
	api := httptest.NewServer(s.Handler())
	defer api.Close()
	body := `{` + base + `,"metrics":["count"]}`
	for i := 0; i < 3; i++ {
		if c, b := post(t, api, "/v1/query/aggregate", "tok", body); c != 200 {
			t.Fatalf("%d %s", c, b)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("ClickHouse hit %d times for 3 identical requests, want 1", hits.Load())
	}
	post(t, api, "/v1/query/aggregate", "tok2", body)
	if hits.Load() != 2 {
		t.Fatalf("other tenant must not reuse the cached result (hits=%d)", hits.Load())
	}
}

// MultiAuth maps bearer tokens to principals (test helper).
type MultiAuth map[string]Principal

func (m MultiAuth) Authenticate(r *http.Request) (Principal, error) {
	if p, ok := m[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]; ok {
		return p, nil
	}
	return Principal{}, errors.New("unauthenticated")
}
