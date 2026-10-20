package query

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// domainStub is a SharedCounter with failure domains: tenants starting with "x" live in domain "X", others in "Y".
type domainStub struct {
	mu     sync.Mutex
	failX  bool
	tokens map[string]float64
	calls  map[string]int
}

func newDomainStub() *domainStub {
	return &domainStub{tokens: map[string]float64{}, calls: map[string]int{}}
}
func (d *domainStub) FailureDomain(t string) string {
	if t[0] == 'x' {
		return "X"
	}
	return "Y"
}
func (d *domainStub) Acquire(t string, cfg LimiterConfig) (SharedDecision, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls[t]++
	if t[0] == 'x' && d.failX {
		return SharedDecision{}, errors.New("node X down")
	}
	tok, ok := d.tokens[t]
	if !ok {
		tok = float64(cfg.Burst)
	}
	if tok < 1 {
		return SharedDecision{Reason: "rate", RetryAfter: 1, Tokens: tok, TokensKnown: true}, nil
	}
	d.tokens[t] = tok - 1
	return SharedDecision{Admitted: true, Slot: "s", Tokens: tok - 1, TokensKnown: true}, nil
}
func (d *domainStub) Release(string, string) error { return nil }

func countAdmitted(l *Limiter, tenant string, n int) int {
	ok := 0
	for i := 0; i < n; i++ {
		if rel, _, err := l.Acquire(tenant); err == nil {
			ok++
			rel()
		}
	}
	return ok
}

func TestFailureIsPerDomain(t *testing.T) {
	st := newDomainStub()
	l, _ := newTestLimiter(LimiterConfig{RatePerSec: 1, Burst: 10, MaxConcurrent: 50})
	l.UseShared(st, FailLocal)
	st.failX = true
	countAdmitted(l, "x1", 1) // marks domain X down
	before := st.calls["y1"]
	if got := countAdmitted(l, "y1", 10); got != 10 {
		t.Fatalf("healthy-domain tenant admitted %d, want 10", got)
	}
	if st.calls["y1"]-before != 10 {
		t.Fatalf("healthy tenant must keep using the shared store, calls=%d", st.calls["y1"]-before)
	}
	if got := countAdmitted(l, "y1", 5); got != 0 { // exact shared limit, no local escape hatch
		t.Fatalf("exact shared limit exceeded: %d", got)
	}
	cx := st.calls["x1"]
	countAdmitted(l, "x2", 3)
	if st.calls["x1"] != cx || st.calls["x2"] != 0 {
		t.Fatalf("X tenants must skip the store during the cooldown (x2 calls=%d)", st.calls["x2"])
	}
	if fb, _ := l.SharedStats(); fb != 4 {
		t.Fatalf("fallbacks=%d want 4", fb)
	}
}

func TestFailClosedOnlyDomain(t *testing.T) {
	st := newDomainStub()
	l, _ := newTestLimiter(LimiterConfig{RatePerSec: 1, Burst: 10})
	l.UseShared(st, FailClosed)
	st.failX = true
	if _, _, err := l.Acquire("x1"); err == nil || err.status != 503 {
		t.Fatalf("x1: want 503, got %v", err)
	}
	if _, _, err := l.Acquire("x2"); err == nil || err.status != 503 {
		t.Fatalf("x2 (same domain): want 503, got %v", err)
	}
	if rel, _, err := l.Acquire("y1"); err != nil {
		t.Fatalf("y1 must keep working: %v", err)
	} else {
		rel()
	}
}

func TestDomainRecoversAfterCooldown(t *testing.T) {
	st := newDomainStub()
	l, clk := newTestLimiter(LimiterConfig{RatePerSec: 1, Burst: 10})
	l.UseShared(st, FailLocal)
	st.failX = true
	countAdmitted(l, "x1", 1)
	st.failX = false
	clk.add(sharedCooldown + time.Millisecond)
	before := st.calls["x1"]
	countAdmitted(l, "x1", 1)
	if st.calls["x1"] != before+1 {
		t.Fatal("domain should be retried after the cooldown")
	}
}

func TestFallbackSeededFromKnownTokensAndReplicas(t *testing.T) {
	st := newDomainStub()
	l, _ := newTestLimiter(LimiterConfig{RatePerSec: 4, Burst: 20, MaxConcurrent: 50, Replicas: 4})
	l.UseShared(st, FailLocal)
	countAdmitted(l, "x1", 12) // Redis now knows 8 tokens left
	st.failX = true
	// seed = 8 / 4 replicas = 2 (clock frozen: no refill)
	if got := countAdmitted(l, "x1", 10); got != 2 {
		t.Fatalf("fallback admitted %d, want 2 (known 8 / 4 replicas)", got)
	}
}

func TestFallbackUnknownUsesFractionAndRate(t *testing.T) {
	st := newDomainStub()
	st.failX = true
	l, clk := newTestLimiter(LimiterConfig{RatePerSec: 8, Burst: 20, MaxConcurrent: 50, Replicas: 2, FallbackFraction: 0.5})
	l.UseShared(st, FailLocal)
	if got := countAdmitted(l, "x1", 20); got != 5 { // 0.5*20/2
		t.Fatalf("seed admitted %d, want 5", got)
	}
	clk.add(time.Second) // refill at rate/replicas = 4/s (below the 2 s cooldown: still skipping the store)
	if got := countAdmitted(l, "x1", 20); got != 4 {
		t.Fatalf("1s refill admitted %d, want 4", got)
	}
	clk.add(5 * time.Second) // below the episode gap: no reseed
	if got := countAdmitted(l, "x1", 20); got != 10 { // capacity burst/replicas
		t.Fatalf("idle refill capped at %d, want 10", got)
	}
}

func TestLoadLimitsFallbackEnv(t *testing.T) {
	env := map[string]string{"QUERY_REDIS_FALLBACK_FRACTION": "0.25", "QUERY_REPLICAS": "3"}
	cfg, _, err := LoadLimits(func(k string) string { return env[k] })
	if err != nil || cfg.FallbackFraction != 0.25 || cfg.Replicas != 3 {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
	for _, bad := range []map[string]string{{"QUERY_REDIS_FALLBACK_FRACTION": "0"}, {"QUERY_REDIS_FALLBACK_FRACTION": "1.5"}, {"QUERY_REPLICAS": "0"}, {"QUERY_REPLICAS": "x"}} {
		if _, _, err := LoadLimits(func(k string) string { return bad[k] }); err == nil {
			t.Fatalf("%v should fail", bad)
		}
	}
}
