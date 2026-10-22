package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestProfilesProduceExpectedShapes(t *testing.T) {
	for _, name := range ProfileNames() {
		cfg := RunConfig{BatchSize: 50}
		cfg.Opt.EventSize = 1024
		if err := ApplyProfile(&cfg, name, false, false); err != nil {
			t.Fatal(err)
		}
		b := NewBuilder(cfg.Opt, []Tenant{{ID: tenantA, Token: "t"}, {ID: tenantB, Token: "u"}}, 7)
		bt := b.BuildBatch(300)
		var body struct {
			Events []json.RawMessage `json:"events"`
		}
		if err := json.Unmarshal(bt.Body, &body); err != nil || len(body.Events) != 300 {
			t.Fatalf("%s: bad batch: %v", name, err)
		}
		if bt.Valid+bt.Invalid != 300 {
			t.Fatalf("%s: counts", name)
		}
	}
	if err := ApplyProfile(&RunConfig{}, "nope", false, false); err == nil {
		t.Fatal("unknown profile accepted")
	}
}

func TestWideLateHotSchemaInvalid(t *testing.T) {
	b := newTestBuilder(Options{EventSize: 4096, Shape: "wide", WideFields: 100, LateRatio: 1, LateMax: 48 * time.Hour, HotKeyRatio: 1})
	e := b.Envelope(b.tenants[0])
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(e.Payload, &m); err != nil || len(m) < 50 {
		t.Fatalf("payload not wide: %d keys %v", len(m), err)
	}
	if _, ok := m["nested"].(map[string]any); !ok {
		t.Fatal("no nested object")
	}
	if len(e.Payload) < 2500 || len(e.Payload) > 6000 {
		t.Fatalf("wide payload size %d", len(e.Payload))
	}
	if !e.EventTime.Before(time.Now().Add(-time.Minute)) || e.EntityKey != "ent-hot" {
		t.Fatalf("late/hot not applied: %v %s", e.EventTime, e.EntityKey)
	}
	bi := newTestBuilder(Options{EventSize: 512, InvalidRatio: 1, SchemaInvalid: true})
	for i := 0; i < 30; i++ {
		var raw map[string]any
		_ = json.Unmarshal(bi.invalidEvent(bi.tenants[0]), &raw)
		if _, bad := raw["unknown_field_xyz"]; bad || raw["event_id"] == "not-a-uuid" {
			t.Fatal("schema-invalid event used decode-level corruption")
		}
		v, _ := raw["schema_version"].(float64)
		if raw["schema_id"] != "" && v > 0 {
			t.Fatalf("event not schema-invalid: %v", raw)
		}
	}
}

func TestParsePromAndBacklog(t *testing.T) {
	v, err := ParsePromValue([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"10"]},{"metric":{},"value":[1,"5.5"]}]}}`))
	if err != nil || v != 15.5 {
		t.Fatalf("%v %v", v, err)
	}
	if _, err := ParsePromValue([]byte(`{"status":"error"}`)); err == nil {
		t.Fatal("error status accepted")
	}
	end := 10.0
	r := EvalBacklog("q", []BacklogSample{{0, 0}, {5, 900000}, {10, 800000}, {20, 400}, {25, 0}}, &end, 1000, "")
	if r.Peak != 900000 || !r.Drained || r.DrainSec == nil || *r.DrainSec != 10 {
		t.Fatalf("%+v", r)
	}
	r = EvalBacklog("q", []BacklogSample{{12, 5e6}}, &end, 1000, "")
	if r.Drained {
		t.Fatal("should not be drained")
	}
}

func TestEvaluateChecks(t *testing.T) {
	rep := Report{Sent: 1000, Failed: 0, EventsPerSec: 900, LatencyMs: map[string]float64{"p95": 100}}
	cs := Evaluate(Criteria{AckP95Ms: 250, MinRateFrac: 0.95}, rep, 1000, nil, nil)
	if AllPass(cs) {
		t.Fatal("rate 90% must fail the 95% criterion")
	}
	rep.EventsPerSec = 1000
	if !AllPass(Evaluate(Criteria{AckP95Ms: 250, MinRateFrac: 0.95}, rep, 1000, nil, nil)) {
		t.Fatal("should pass")
	}
	rep.Failed = 1
	if AllPass(Evaluate(Criteria{AckP95Ms: 250}, rep, 0, nil, nil)) {
		t.Fatal("failed events must fail")
	}
}

func TestBurstPhases(t *testing.T) {
	p := BurstPhases(1000, time.Second, 120000, 2*time.Second, 3*time.Second)
	if len(p) != 3 || p[1].Name != "burst" || p[1].Rate != 120000 {
		t.Fatalf("%+v", p)
	}
	if len(BurstPhases(0, 0, 5, time.Second, time.Second)) != 1 {
		t.Fatal("zero baseline should be burst only")
	}
}

func TestScenarioAgainstFakeGatewayAndQuery(t *testing.T) {
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Events []struct {
				EventID string `json:"event_id"`
			} `json:"events"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		res := []map[string]string{}
		for _, e := range req.Events {
			res = append(res, map[string]string{"event_id": e.EventID, "status": "accepted"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(req.Events), "results": res})
	}))
	defer gw.Close()
	qs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer qs.Close()
	cfg := RunConfig{URL: gw.URL, BatchSize: 20, Concurrency: 2, Timeout: 2 * time.Second, Seed: 1, Tenants: []Tenant{{ID: tenantA, Token: "t"}}}
	cfg.Opt.EventSize = 512
	out := t.TempDir() + "/r.json"
	code := runScenario(context.Background(), cfg, scenarioFlags{
		burstRate: 2000, burstDur: 300 * time.Millisecond, baseRate: 200, baseDur: 200 * time.Millisecond, drainDur: 300 * time.Millisecond,
		queryURL: qs.URL, queryRate: 50, queryConc: 2, ackP95: 250, minRateFrac: 0.5, enforce: true,
	}, "x", gw.Client(), out)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var rep ScenarioReport
	if err := json.Unmarshal(raw, &rep); err != nil || len(rep.Phases) != 3 || rep.Query == nil || rep.Query.Requests == 0 || !rep.Pass {
		t.Fatalf("report %v %+v", err, rep)
	}
}
