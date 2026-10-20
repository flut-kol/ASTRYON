package query

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Caps are hard ClickHouse settings applied to every query (DPP-050).
type Caps struct {
	MaxExecutionSeconds int   // max_execution_time
	MaxResultRows       int   // max_result_rows
	MaxMemoryBytes      int64 // max_memory_usage
}

// DefaultCaps are the safe defaults used when a field is zero or unset.
var DefaultCaps = Caps{MaxExecutionSeconds: 30, MaxResultRows: MaxLimit + 1, MaxMemoryBytes: 2_000_000_000}

func (c Caps) withDefaults() Caps {
	if c.MaxExecutionSeconds <= 0 {
		c.MaxExecutionSeconds = DefaultCaps.MaxExecutionSeconds
	}
	if c.MaxResultRows <= 0 {
		c.MaxResultRows = DefaultCaps.MaxResultRows
	}
	if c.MaxMemoryBytes <= 0 {
		c.MaxMemoryBytes = DefaultCaps.MaxMemoryBytes
	}
	return c
}

// FailureMode says what the Limiter does when the shared counter (Redis) is unreachable.
type FailureMode string

const (
	// FailLocal falls back to the per-instance in-memory limiter (availability over exact global limits).
	FailLocal FailureMode = "local"
	// FailClosed rejects the request with 503 limiter_unavailable (exact limits over availability).
	FailClosed FailureMode = "closed"
)

// SharedDecision is the outcome of a SharedCounter.Acquire.
type SharedDecision struct {
	Admitted   bool
	Reason     string // when !Admitted: "rate" or "concurrent"
	RetryAfter int    // seconds, >= 1 when !Admitted
	Slot       string // when Admitted: lease id to pass to Release
}

// SharedCounter is a cross-replica store for the per-tenant rate bucket and concurrency leases.
// Acquire must atomically check both limits and, only if both pass, consume a token and take a lease slot.
// Leases expire on their own (cfg.SlotLease) so a crashed holder never leaks a slot. tenant is always the verified
// Principal tenant. Errors mean "store unavailable" and trigger the FailureMode policy.
type SharedCounter interface {
	Acquire(tenant string, cfg LimiterConfig) (SharedDecision, error)
	Release(tenant, slot string) error
}

// LeaseRenewer is optionally implemented by a SharedCounter: Renew extends the lease of a held slot and reports
// false once the slot is gone (released or expired). The Limiter calls it every SlotLease/3 while a request runs.
type LeaseRenewer interface {
	Renew(tenant, slot string, cfg LimiterConfig) (bool, error)
}

// LimiterConfig configures per-tenant protection. State is in-memory per instance unless a SharedCounter is attached.
type LimiterConfig struct {
	RatePerSec    float64       // token refill rate per tenant
	Burst         int           // bucket size per tenant
	MaxConcurrent int           // in-flight queries per tenant
	IdleTTL       time.Duration // tenants idle (no tokens owed, nothing in flight) longer than this are evicted
	MaxTenants    int           // hard bound on tracked tenants
	SlotLease     time.Duration // shared mode: concurrency lease; must exceed the longest query (default 60s)
}

// DefaultLimiterConfig holds safe defaults.
var DefaultLimiterConfig = LimiterConfig{RatePerSec: 10, Burst: 20, MaxConcurrent: 8, IdleTTL: 10 * time.Minute, MaxTenants: 10000, SlotLease: 60 * time.Second}

func (c LimiterConfig) withDefaults() LimiterConfig {
	d := DefaultLimiterConfig
	if c.RatePerSec <= 0 {
		c.RatePerSec = d.RatePerSec
	}
	if c.Burst <= 0 {
		c.Burst = d.Burst
	}
	if c.MaxConcurrent <= 0 {
		c.MaxConcurrent = d.MaxConcurrent
	}
	if c.IdleTTL <= 0 {
		c.IdleTTL = d.IdleTTL
	}
	if c.MaxTenants <= 0 {
		c.MaxTenants = d.MaxTenants
	}
	if c.SlotLease <= 0 {
		c.SlotLease = d.SlotLease
	}
	return c
}

type tenantState struct {
	tokens   float64
	last     time.Time
	inflight int
	seen     time.Time
}

// Limiter is a per-tenant token bucket plus in-flight cap. Keys are tenant IDs from the verified Principal only.
type Limiter struct {
	cfg       LimiterConfig
	now       func() time.Time
	mu        sync.Mutex
	tenants   map[string]*tenantState
	lastSweep time.Time

	shared    SharedCounter
	mode      FailureMode
	smu       sync.Mutex // guards downUntil, lastLog
	downUntil time.Time
	lastLog   time.Time
	fallbacks atomic.Uint64 // requests served by the local limiter because shared was down
	rejected  atomic.Uint64 // requests rejected 503 because shared was down (closed mode)
	renewing  atomic.Int64  // lease-renewal goroutines currently running (observability/tests)
}

const sharedCooldown = 2 * time.Second // after a shared failure, skip it this long (no per-request timeout storm)

// UseShared attaches a cross-replica store. mode "" means FailLocal.
func (l *Limiter) UseShared(sc SharedCounter, mode FailureMode) {
	if mode == "" {
		mode = FailLocal
	}
	l.shared, l.mode = sc, mode
}

// SharedStats returns how many requests used the local fallback and how many were rejected (503) because the
// shared store was unavailable.
func (l *Limiter) SharedStats() (fallbacks, rejected uint64) {
	return l.fallbacks.Load(), l.rejected.Load()
}

func (l *Limiter) logSharedErr(format string, err error) {
	l.smu.Lock()
	now := l.now()
	ok := now.Sub(l.lastLog) >= time.Minute
	if ok {
		l.lastLog = now
	}
	l.smu.Unlock()
	if ok {
		fb, rj := l.SharedStats()
		log.Printf(format+" (fallbacks=%d rejected=%d; logged at most once per minute)", err, fb, rj)
	}
}

func NewLimiter(cfg LimiterConfig) *Limiter {
	return &Limiter{cfg: cfg.withDefaults(), now: time.Now, tenants: map[string]*tenantState{}}
}

// Acquire admits one query for tenant. On success the caller MUST call release exactly once.
// On rejection it returns an *apiError (429) with Retry-After seconds in retryAfter.
func (l *Limiter) Acquire(tenant string) (release func(), retryAfter int, err *apiError) {
	return l.AcquireCtx(context.Background(), tenant)
}

// AcquireCtx is Acquire with a context: in shared mode the slot lease is renewed in the background every
// SlotLease/3 until release is called or ctx is done, so a query running longer than the lease keeps counting
// against the cap. A crashed replica stops renewing and its slot expires after at most one lease.
func (l *Limiter) AcquireCtx(ctx context.Context, tenant string) (release func(), retryAfter int, err *apiError) {
	if l.shared != nil {
		if rel, ra, aerr, done := l.acquireShared(ctx, tenant); done {
			return rel, ra, aerr
		}
	}
	return l.acquireLocal(tenant)
}

// acquireShared returns done=false when the caller should use the local limiter (fail-local policy).
func (l *Limiter) acquireShared(ctx context.Context, tenant string) (release func(), retryAfter int, aerr *apiError, done bool) {
	l.smu.Lock()
	down := l.now().Before(l.downUntil)
	l.smu.Unlock()
	var err error
	var d SharedDecision
	if !down {
		d, err = l.shared.Acquire(tenant, l.cfg)
		if err != nil {
			l.smu.Lock()
			l.downUntil = l.now().Add(sharedCooldown)
			l.smu.Unlock()
		}
	}
	if down || err != nil {
		if err != nil {
			l.logSharedErr("query-api: shared limiter unavailable: %v", err)
		}
		if l.mode == FailClosed {
			l.rejected.Add(1)
			return nil, 1, &apiError{status: 503, Code: "limiter_unavailable",
				Message: "rate limiter temporarily unavailable"}, true
		}
		l.fallbacks.Add(1)
		return nil, 0, nil, false
	}
	if !d.Admitted {
		ra := d.RetryAfter
		if ra < 1 {
			ra = 1
		}
		if d.Reason == "concurrent" {
			return nil, 1, &apiError{status: 429, Code: "too_many_concurrent_queries",
				Message: "too many concurrent queries for this tenant", Details: map[string]any{"max_concurrent": l.cfg.MaxConcurrent}}, true
		}
		return nil, ra, &apiError{status: 429, Code: "rate_limited",
			Message: "query rate limit exceeded for this tenant", Details: map[string]any{"retry_after_seconds": ra}}, true
	}
	stop := make(chan struct{})
	if rn, ok := l.shared.(LeaseRenewer); ok {
		l.renewing.Add(1)
		go l.renewLoop(ctx, rn, tenant, d.Slot, stop)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			if err := l.shared.Release(tenant, d.Slot); err != nil {
				l.logSharedErr("query-api: shared limiter release failed (lease will expire): %v", err)
			}
		})
	}, 0, nil, true
}

func (l *Limiter) renewLoop(ctx context.Context, rn LeaseRenewer, tenant, slot string, stop <-chan struct{}) {
	defer l.renewing.Add(-1)
	every := l.cfg.SlotLease / 3
	if every < time.Millisecond {
		every = time.Millisecond
	}
	tk := time.NewTicker(every)
	defer tk.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		ok, err := rn.Renew(tenant, slot, l.cfg)
		if err != nil {
			l.logSharedErr("query-api: shared limiter lease renewal failed (will retry): %v", err)
			continue
		}
		if !ok { // released elsewhere or already expired: nothing left to keep alive
			return
		}
	}
}

func (l *Limiter) acquireLocal(tenant string) (release func(), retryAfter int, err *apiError) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.sweep(now)
	st := l.tenants[tenant]
	if st == nil {
		if len(l.tenants) >= l.cfg.MaxTenants {
			l.evictOldest()
		}
		st = &tenantState{tokens: float64(l.cfg.Burst), last: now}
		l.tenants[tenant] = st
	}
	st.tokens = math.Min(float64(l.cfg.Burst), st.tokens+now.Sub(st.last).Seconds()*l.cfg.RatePerSec)
	st.last, st.seen = now, now
	if st.inflight >= l.cfg.MaxConcurrent {
		return nil, 1, &apiError{status: 429, Code: "too_many_concurrent_queries",
			Message: "too many concurrent queries for this tenant", Details: map[string]any{"max_concurrent": l.cfg.MaxConcurrent}}
	}
	if st.tokens < 1 {
		ra := int(math.Ceil((1 - st.tokens) / l.cfg.RatePerSec))
		if ra < 1 {
			ra = 1
		}
		return nil, ra, &apiError{status: 429, Code: "rate_limited",
			Message: "query rate limit exceeded for this tenant", Details: map[string]any{"retry_after_seconds": ra}}
	}
	st.tokens--
	st.inflight++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			st.inflight--
			st.seen = l.now()
		})
	}, 0, nil
}

// sweep evicts idle tenants (nothing in flight, unseen for IdleTTL). Runs at most once per second. Caller holds mu.
func (l *Limiter) sweep(now time.Time) {
	if now.Sub(l.lastSweep) < time.Second {
		return
	}
	l.lastSweep = now
	for k, st := range l.tenants {
		if st.inflight == 0 && now.Sub(st.seen) >= l.cfg.IdleTTL {
			delete(l.tenants, k)
		}
	}
}

// evictOldest drops the least recently seen tenant without in-flight queries. Caller holds mu.
func (l *Limiter) evictOldest() {
	var key string
	var oldest time.Time
	for k, st := range l.tenants {
		if st.inflight == 0 && (key == "" || st.seen.Before(oldest)) {
			key, oldest = k, st.seen
		}
	}
	if key != "" {
		delete(l.tenants, key)
	}
}

func (l *Limiter) size() int { l.mu.Lock(); defer l.mu.Unlock(); return len(l.tenants) }

// LoadLimits reads QUERY_RATE_PER_SEC, QUERY_BURST, QUERY_MAX_CONCURRENT, QUERY_IDLE_TTL_SECONDS, QUERY_MAX_TENANTS,
// QUERY_REDIS_LEASE_SECONDS, QUERY_MAX_EXECUTION_SECONDS, QUERY_MAX_RESULT_ROWS, QUERY_MAX_MEMORY_BYTES. Unset -> defaults; invalid -> error.
func LoadLimits(get func(string) string) (LimiterConfig, Caps, error) {
	cfg, caps := DefaultLimiterConfig, DefaultCaps
	var errs []string
	num := func(k string, apply func(string) error) {
		if v := strings.TrimSpace(get(k)); v != "" {
			if err := apply(v); err != nil {
				errs = append(errs, fmt.Sprintf("%s=%q: must be a positive number", k, v))
			}
		}
	}
	pos := func(dst *int) func(string) error {
		return func(v string) error {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return fmt.Errorf("bad")
			}
			*dst = n
			return nil
		}
	}
	num("QUERY_RATE_PER_SEC", func(v string) error {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || !(f > 0) || math.IsInf(f, 0) {
			return fmt.Errorf("bad")
		}
		cfg.RatePerSec = f
		return nil
	})
	num("QUERY_BURST", pos(&cfg.Burst))
	num("QUERY_MAX_CONCURRENT", pos(&cfg.MaxConcurrent))
	num("QUERY_MAX_TENANTS", pos(&cfg.MaxTenants))
	num("QUERY_IDLE_TTL_SECONDS", func(v string) error {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("bad")
		}
		cfg.IdleTTL = time.Duration(n) * time.Second
		return nil
	})
	num("QUERY_REDIS_LEASE_SECONDS", func(v string) error {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("bad")
		}
		cfg.SlotLease = time.Duration(n) * time.Second
		return nil
	})
	num("QUERY_MAX_EXECUTION_SECONDS", pos(&caps.MaxExecutionSeconds))
	num("QUERY_MAX_RESULT_ROWS", pos(&caps.MaxResultRows))
	num("QUERY_MAX_MEMORY_BYTES", func(v string) error {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("bad")
		}
		caps.MaxMemoryBytes = n
		return nil
	})
	if len(errs) > 0 {
		return cfg, caps, fmt.Errorf("invalid limits config: %s", strings.Join(errs, "; "))
	}
	return cfg, caps, nil
}
