package query

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// perTenantStub is a SharedCounter without failure domains (single node / Sentinel) whose failures are per tenant.
type perTenantStub struct {
	mu    sync.Mutex
	fail  map[string]bool
	calls map[string]int
}

func newPerTenantStub() *perTenantStub {
	return &perTenantStub{fail: map[string]bool{}, calls: map[string]int{}}
}

func (s *perTenantStub) Acquire(t string, cfg LimiterConfig) (SharedDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[t]++
	if s.fail[t] {
		return SharedDecision{}, errors.New("redis down")
	}
	return SharedDecision{Admitted: true, Slot: "s"}, nil
}
func (s *perTenantStub) Release(string, string) error { return nil }
func (s *perTenantStub) callsOf(t string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[t]
}

func TestCooldownIsPerTenantFailClosed(t *testing.T) {
	st := newPerTenantStub()
	l, clk := newTestLimiter(LimiterConfig{RatePerSec: 1, Burst: 5, MaxConcurrent: 50})
	l.UseShared(st, FailClosed)
	st.fail["a"] = true
	if _, _, err := l.Acquire("a"); err == nil || err.status != 503 {
		t.Fatalf("tenant a: want 503, got %v", err)
	}
	before := st.callsOf("a")
	if _, _, err := l.Acquire("a"); err == nil || err.status != 503 {
		t.Fatalf("tenant a within cooldown: want 503, got %v", err)
	}
	if st.callsOf("a") != before {
		t.Fatal("tenant a must be skipped during its cooldown")
	}
	for i := 0; i < 3; i++ { // tenant b is unaffected and still hits Redis
		rel, _, err := l.Acquire("b")
		if err != nil {
			t.Fatalf("tenant b must not see a's failure: %v", err)
		}
		rel()
	}
	if st.callsOf("b") != 3 {
		t.Fatalf("tenant b should use Redis every time, calls=%d", st.callsOf("b"))
	}
	clk.add(sharedCooldown + 1)
	st.fail["a"] = false
	rel, _, err := l.Acquire("a")
	if err != nil {
		t.Fatalf("tenant a should recover after cooldown: %v", err)
	}
	rel()
}

func TestFallbackIsPerTenantFailLocal(t *testing.T) {
	st := newPerTenantStub()
	l, _ := newTestLimiter(LimiterConfig{RatePerSec: 1, Burst: 10, MaxConcurrent: 50})
	l.UseShared(st, FailLocal)
	st.fail["a"] = true
	if got := countAdmitted(l, "a", 30); got >= 30 || got == 0 {
		t.Fatalf("tenant a should be bounded by the local fallback, admitted %d", got)
	}
	fb := l.fallbacks.Load()
	if got := countAdmitted(l, "b", 30); got != 30 {
		t.Fatalf("tenant b has exact (stub: unlimited) Redis limits, admitted %d", got)
	}
	if l.fallbacks.Load() != fb {
		t.Fatal("tenant b must not be served by the fallback")
	}
}

func TestCooldownMapBounded(t *testing.T) {
	st := newPerTenantStub()
	l, clk := newTestLimiter(LimiterConfig{RatePerSec: 1, Burst: 5, MaxConcurrent: 50, MaxTenants: 8})
	l.UseShared(st, FailClosed)
	for i := 0; i < 100; i++ {
		ten := fmt.Sprintf("t%d", i)
		st.fail[ten] = true
		l.Acquire(ten)
		l.smu.Lock()
		n := len(l.downUntil)
		l.smu.Unlock()
		if n > 8 {
			t.Fatalf("downUntil grew to %d, cap 8", n)
		}
		clk.add(1) // staggered expiry so the soonest-expiring entry is evicted
	}
}

func TestCooldownConcurrentTenants(t *testing.T) {
	st := newPerTenantStub()
	l, _ := newTestLimiter(LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 1000, MaxTenants: 16})
	l.UseShared(st, FailLocal)
	for i := 0; i < 50; i += 2 {
		st.fail[fmt.Sprintf("t%d", i)] = true
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ten := fmt.Sprintf("t%d", (g+i)%50)
				if rel, _, err := l.Acquire(ten); err == nil {
					rel()
				}
			}
		}(g)
	}
	wg.Wait()
	l.smu.Lock()
	defer l.smu.Unlock()
	if len(l.downUntil) > 16 {
		t.Fatalf("downUntil %d > cap", len(l.downUntil))
	}
}
