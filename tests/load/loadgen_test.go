package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
)

const tenantA = "11111111-1111-4111-8111-111111111111"
const tenantB = "22222222-2222-4222-8222-222222222222"

func newTestBuilder(opt Options, ts ...Tenant) *Builder {
	if len(ts) == 0 {
		ts = []Tenant{{ID: tenantA, Token: "t"}}
	}
	return NewBuilder(opt, ts, 42)
}

func TestEventBuilderValid(t *testing.T) {
	b := newTestBuilder(Options{EventSize: 1024})
	for i := 0; i < 200; i++ {
		e := b.Envelope(b.tenants[0])
		if err := e.Validate(); err != nil {
			t.Fatalf("event %d invalid: %v", i, err)
		}
	}
}

func TestEventSizeApprox(t *testing.T) {
	for _, size := range []int{600, 1024, 4096} {
		b := newTestBuilder(Options{EventSize: size})
		raw, _ := b.validEvent(b.tenants[0])
		if len(raw) < size*9/10 || len(raw) > size*11/10 {
			t.Errorf("size %d: got %d bytes", size, len(raw))
		}
	}
}

func decodeBatch(t *testing.T, body []byte) []json.RawMessage {
	t.Helper()
	var req struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatal(err)
	}
	return req.Events
}

func TestBatchRatios(t *testing.T) {
	b := newTestBuilder(Options{EventSize: 800, DupRatio: 0.2, InvalidRatio: 0.1})
	var invalid, dups, total int
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		batch := b.BuildBatch(50)
		invalid += batch.Invalid
		dups += batch.Dups
		evs := decodeBatch(t, batch.Body)
		if len(evs) != 50 {
			t.Fatalf("batch size %d", len(evs))
		}
		for _, raw := range evs {
			total++
			var e ec.Envelope
			if json.Unmarshal(raw, &e) == nil && e.Validate() == nil {
				seen[e.EventID] = true
			}
		}
	}
	if r := float64(invalid) / float64(total); r < 0.07 || r > 0.13 {
		t.Errorf("invalid ratio %.3f", r)
	}
	if r := float64(dups) / float64(total); r < 0.15 || r > 0.25 {
		t.Errorf("dup ratio %.3f", r)
	}
}

func TestDuplicatesReuseEventID(t *testing.T) {
	b := newTestBuilder(Options{EventSize: 700, DupRatio: 0.5})
	ids := map[string]int{}
	for i := 0; i < 20; i++ {
		for _, raw := range decodeBatch(t, b.BuildBatch(50).Body) {
			var e ec.Envelope
			_ = json.Unmarshal(raw, &e)
			ids[e.EventID]++
		}
	}
	rep := 0
	for _, n := range ids {
		if n > 1 {
			rep++
		}
	}
	if rep == 0 {
		t.Fatal("expected repeated event_ids")
	}
}

func TestInvalidEventsFailValidationOrDecoding(t *testing.T) {
	b := newTestBuilder(Options{EventSize: 700})
	for i := 0; i < 100; i++ {
		raw := b.invalidEvent(b.tenants[0])
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		_, unknown := m["unknown_field_xyz"]
		var e ec.Envelope
		if !unknown && (json.Unmarshal(raw, &e) != nil || e.Validate() == nil) {
			t.Fatalf("invalid event passed validation: %s", raw)
		}
	}
}

func TestTenantSkew(t *testing.T) {
	b := newTestBuilder(Options{EventSize: 700, TenantSkew: 1.5}, Tenant{tenantA, "a"}, Tenant{tenantB, "b"})
	c := map[string]int{}
	for i := 0; i < 5000; i++ {
		c[b.PickTenant().ID]++
	}
	if c[tenantA] < 2*c[tenantB] {
		t.Errorf("expected skew toward first tenant: %v", c)
	}
	u := newTestBuilder(Options{EventSize: 700}, Tenant{tenantA, "a"}, Tenant{tenantB, "b"})
	c = map[string]int{}
	for i := 0; i < 5000; i++ {
		c[u.PickTenant().ID]++
	}
	if c[tenantA] < 2200 || c[tenantA] > 2800 {
		t.Errorf("uniform expected: %v", c)
	}
}

func TestLimiterSchedule(t *testing.T) {
	now := time.Unix(1000, 0)
	var slept time.Duration
	l := NewLimiter(100) // 100 units/s
	l.now = func() time.Time { return now }
	l.sleep = func(_ context.Context, d time.Duration) error { slept += d; now = now.Add(d); return nil }
	for i := 0; i < 10; i++ {
		if err := l.WaitN(context.Background(), 50); err != nil {
			t.Fatal(err)
		}
	}
	// 10 reservations of 0.5s: first is free, so 9 * 0.5s waited.
	if want := 4500 * time.Millisecond; slept != want {
		t.Fatalf("slept %v want %v", slept, want)
	}
	// idle time is not banked
	now = now.Add(time.Hour)
	slept = 0
	_ = l.WaitN(context.Background(), 50)
	_ = l.WaitN(context.Background(), 50)
	if slept != 500*time.Millisecond {
		t.Fatalf("after idle slept %v", slept)
	}
}

func TestLimiterUnlimitedAndCancel(t *testing.T) {
	l := NewLimiter(0)
	if d := l.reserve(1000); d != 0 {
		t.Fatal(d)
	}
	l = NewLimiter(1)
	ctx, cancel := context.WithCancel(context.Background())
	_ = l.WaitN(ctx, 1)
	cancel()
	if err := l.WaitN(ctx, 1); err == nil {
		t.Fatal("expected ctx error")
	}
}

func TestLimiterRealRate(t *testing.T) {
	l := NewLimiter(2000)
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = l.WaitN(context.Background(), 10)
			}
		}()
	}
	wg.Wait()
	// 2000 units at 2000/s ~ 1s (minus first free slot)
	if el := time.Since(start); el < 850*time.Millisecond || el > 1500*time.Millisecond {
		t.Fatalf("elapsed %v", el)
	}
}

func TestPercentile(t *testing.T) {
	var s []time.Duration
	for i := 1; i <= 100; i++ {
		s = append(s, time.Duration(i)*time.Millisecond)
	}
	for p, want := range map[float64]time.Duration{50: 50, 95: 95, 99: 99, 100: 100, 1: 1, 0: 1} {
		if got := Percentile(s, p); got != want*time.Millisecond {
			t.Errorf("p%v = %v want %v", p, got, want)
		}
	}
	if Percentile(nil, 50) != 0 {
		t.Error("empty")
	}
	if got := Percentile([]time.Duration{7}, 99); got != 7 {
		t.Error(got)
	}
	// non-trivial rank: 10 samples, p95 -> rank 10, p50 -> rank 5
	s = s[:10]
	if Percentile(s, 95) != 10*time.Millisecond || Percentile(s, 50) != 5*time.Millisecond {
		t.Error("rank")
	}
}

func TestRunAgainstFakeGateway(t *testing.T) {
	var mu sync.Mutex
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		mu.Unlock()
		var req struct {
			Events []json.RawMessage `json:"events"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		type res struct {
			EventID string `json:"event_id"`
			Status  string `json:"status"`
		}
		var out struct {
			Accepted, Quarantined, Failed int
			Results                       []res `json:"results"`
		}
		for _, raw := range req.Events {
			var e ec.Envelope
			if json.Unmarshal(raw, &e) == nil && e.Validate() == nil {
				out.Accepted++
				out.Results = append(out.Results, res{e.EventID, "accepted"})
			} else {
				out.Quarantined++
				out.Results = append(out.Results, res{"", "quarantined"})
			}
		}
		w.WriteHeader(207)
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": out.Accepted, "quarantined": out.Quarantined, "failed": 0, "results": out.Results})
	}))
	defer srv.Close()
	cfg := RunConfig{URL: srv.URL, Rate: 1000, Duration: 500 * time.Millisecond, BatchSize: 20, Concurrency: 2,
		Timeout: 5 * time.Second, Seed: 1, TrackIDs: true, Tenants: []Tenant{{tenantA, "secret"}},
		Opt: Options{EventSize: 900, DupRatio: 0.1, InvalidRatio: 0.1}}
	st, el := Run(context.Background(), cfg, srv.Client())
	r := st.Report(el)
	if r.Sent == 0 || r.Sent != r.Accepted+r.Quarantined+r.Failed || r.Failed != 0 {
		t.Fatalf("%+v", r)
	}
	if r.Quarantined == 0 || *r.AcceptedRepeats == 0 || *r.UniqueAccepted != r.Accepted-*r.AcceptedRepeats {
		t.Fatalf("%+v", r)
	}
	if gotAuth != "Bearer secret" || r.LatencyMs["p50"] <= 0 {
		t.Fatalf("auth %q, report %+v", gotAuth, r)
	}
	if r.Sent < 300 || r.Sent > 700 {
		t.Errorf("rate not honoured: sent %d in 0.5s at 1000/s", r.Sent)
	}
	if r.Text() == "" {
		t.Fatal("text")
	}
}

func TestParseTenants(t *testing.T) {
	if _, err := parseTenants("", "", ""); err == nil {
		t.Error("want error")
	}
	if _, err := parseTenants("tok", "bad", ""); err == nil {
		t.Error("want error")
	}
	ts, err := parseTenants("tok", tenantA, tenantB+"=x")
	if err != nil || len(ts) != 2 || ts[0].Token != "tok" {
		t.Fatal(ts, err)
	}
}
