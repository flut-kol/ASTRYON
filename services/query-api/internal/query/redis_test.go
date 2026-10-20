package query

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- in-process fake RESP server. It does not run Lua: on EVAL/EVALSHA of limiterScript it executes a Go port of
// the same algorithm, so these tests prove the client/limiter logic, not the Lua text (the live tests do). ----

type fakeRedis struct {
	ln          net.Listener
	mu          sync.Mutex
	now         time.Time
	rate        map[string][2]float64 // key -> tokens, ts(ms)
	conc        map[string]map[string]int64
	loaded      map[string]bool                         // script sha -> loaded (SCRIPT cache)
	clk         func() time.Time                        // nil = manual clock f.now; set for real-time (renewal) tests
	hook        func(args []string, asking bool) string // returns a raw error line (e.g. "-MOVED ...") to emit instead of running
	keyed       atomic.Int64                            // keyed commands (EVAL/EVALSHA/ZREM) received, incl. redirected ones
	asked       atomic.Int64                            // keyed commands that arrived after ASKING
	evalshas    int
	evals       int
	password    string
	conns       []net.Conn
	authed      atomic.Int32
	slotsRaw    string // raw RESP reply to CLUSTER SLOTS ("" = "-ERR This instance has cluster support disabled")
	sentinelRaw string
	slotsReq    atomic.Int64 // CLUSTER SLOTS requests received
}

func newFakeRedis(t *testing.T) *fakeRedis { return newFakeRedisL(t, nil) }

// newFakeRedisL optionally wraps the listener (TLS).
func newFakeRedisL(t *testing.T, wrap func(net.Listener) net.Listener) *fakeRedis {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if wrap != nil {
		ln = wrap(ln)
	}
	f := &fakeRedis{ln: ln, now: time.Unix(2_000_000, 0), rate: map[string][2]float64{}, conc: map[string]map[string]int64{}}
	go f.serve()
	t.Cleanup(f.stop)
	return f
}

func (f *fakeRedis) addr() string { return f.ln.Addr().String() }
func (f *fakeRedis) nowMs() int64 { // caller holds mu
	if f.clk != nil {
		return f.clk().UnixMilli()
	}
	return f.now.UnixMilli()
}
func (f *fakeRedis) advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}
func (f *fakeRedis) stop() {
	f.ln.Close()
	f.mu.Lock()
	for _, c := range f.conns {
		c.Close()
	}
	f.mu.Unlock()
}
func (f *fakeRedis) concKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ks []string
	for k := range f.conc {
		ks = append(ks, k)
	}
	for k := range f.rate {
		ks = append(ks, k)
	}
	return ks
}
func (f *fakeRedis) slots(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conc[key])
}

func (f *fakeRedis) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.conns = append(f.conns, c)
		f.mu.Unlock()
		go f.handle(c)
	}
}

func (f *fakeRedis) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	f.mu.Lock()
	pw := f.password
	f.mu.Unlock()
	authed := pw == ""
	asking := false
	for {
		args, err := readCmd(r)
		if err != nil || len(args) == 0 {
			return
		}
		cmd := strings.ToUpper(args[0])
		wasAsking := asking
		asking = false
		if cmd == "EVAL" || cmd == "EVALSHA" || cmd == "ZREM" {
			f.keyed.Add(1)
			if wasAsking {
				f.asked.Add(1)
			}
			f.mu.Lock()
			h := f.hook
			f.mu.Unlock()
			if h != nil {
				if line := h(args, wasAsking); line != "" {
					io.WriteString(c, line+"\r\n")
					continue
				}
			}
		}
		switch {
		case cmd == "ASKING":
			asking = true
			io.WriteString(c, "+OK\r\n")
		case cmd == "AUTH":
			if args[len(args)-1] == pw {
				authed = true
				f.authed.Add(1)
				io.WriteString(c, "+OK\r\n")
			} else {
				io.WriteString(c, "-WRONGPASS invalid\r\n")
			}
		case !authed:
			io.WriteString(c, "-NOAUTH Authentication required.\r\n")
		case cmd == "CLUSTER":
			f.slotsReq.Add(1)
			f.mu.Lock()
			raw := f.slotsRaw
			f.mu.Unlock()
			if raw == "" {
				raw = "-ERR This instance has cluster support disabled\r\n"
			}
			io.WriteString(c, raw)
		case cmd == "SENTINEL":
			f.mu.Lock()
			raw := f.sentinelRaw
			f.mu.Unlock()
			io.WriteString(c, raw)
		case cmd == "SELECT":
			io.WriteString(c, "+OK\r\n")
		case cmd == "EVALSHA":
			f.mu.Lock()
			f.evalshas++
			ok := f.loaded[args[1]]
			f.mu.Unlock()
			if !ok {
				io.WriteString(c, "-NOSCRIPT No matching script. Please use EVAL.\r\n")
				continue
			}
			io.WriteString(c, f.run(args[1], args[3:]))
		case cmd == "EVAL":
			f.mu.Lock()
			f.evals++
			if f.loaded == nil {
				f.loaded = map[string]bool{}
			}
			f.loaded[scriptSHA(args[1])] = true
			f.mu.Unlock()
			io.WriteString(c, f.run(args[1], args[3:]))
		case cmd == "ZREM":
			f.mu.Lock()
			delete(f.conc[args[1]], args[2])
			f.mu.Unlock()
			io.WriteString(c, ":1\r\n")
		default:
			io.WriteString(c, "-ERR unknown command\r\n")
		}
	}
}

// runRenew: a = KEYS(1)+ARGV(leaseMs, slot): same semantics as renewScript.
func (f *fakeRedis) runRenew(a []string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.nowMs()
	lease, _ := strconv.ParseInt(a[1], 10, 64)
	if exp, ok := f.conc[a[0]][a[2]]; ok && exp > now {
		f.conc[a[0]][a[2]] = now + lease
		return ":1\r\n"
	}
	return ":0\r\n"
}

// run: sha selects the script; args = KEYS+ARGV (limiter: KEYS(2)+ARGV(5))
func (f *fakeRedis) run(shaOrScript string, a []string) string {
	if shaOrScript == renewSHA || shaOrScript == renewScript {
		return f.runRenew(a)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rk, ck := a[0], a[1]
	rate, _ := strconv.ParseFloat(a[2], 64)
	burst, _ := strconv.ParseFloat(a[3], 64)
	maxc, _ := strconv.Atoi(a[4])
	lease, _ := strconv.ParseInt(a[5], 10, 64)
	now := f.nowMs()
	set := f.conc[ck]
	if set == nil {
		set = map[string]int64{}
		f.conc[ck] = set
	}
	for s, exp := range set {
		if exp <= now {
			delete(set, s)
		}
	}
	if len(set) >= maxc {
		return "*2\r\n:1\r\n:1\r\n"
	}
	st, ok := f.rate[rk]
	if !ok {
		st = [2]float64{burst, float64(now)}
	}
	tokens := st[0] + maxf(0, float64(now)-st[1])/1000*rate
	if tokens > burst {
		tokens = burst
	}
	if tokens < 1 {
		f.rate[rk] = [2]float64{tokens, float64(now)}
		ra := int64((1-tokens)/rate + 0.999999)
		return fmt.Sprintf("*2\r\n:2\r\n:%d\r\n", ra)
	}
	f.rate[rk] = [2]float64{tokens - 1, float64(now)}
	set[a[6]] = now + lease
	return "*2\r\n:0\r\n:0\r\n"
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func readCmd(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	out := make([]string, n)
	for i := range out {
		l, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		sz, _ := strconv.Atoi(strings.TrimSpace(l[1:]))
		buf := make([]byte, sz+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out[i] = string(buf[:sz])
	}
	return out, nil
}

// ---- helpers ----

func sharedLimiter(t *testing.T, f *fakeRedis, url string, cfg LimiterConfig, mode FailureMode) *Limiter {
	t.Helper()
	rc, err := NewRedisCounter(url)
	if err != nil {
		t.Fatal(err)
	}
	rc.CmdTimeout = 300 * time.Millisecond
	l := NewLimiter(cfg)
	l.UseShared(rc, mode)
	return l
}

// ---- unit tests against the fake ----

func TestRedisRateSharedAcrossInstances(t *testing.T) {
	f := newFakeRedis(t)
	cfg := LimiterConfig{RatePerSec: 1, Burst: 4, MaxConcurrent: 100}
	a := sharedLimiter(t, f, "redis://"+f.addr(), cfg, FailLocal)
	b := sharedLimiter(t, f, "redis://"+f.addr(), cfg, FailLocal)
	admitted := 0
	for i := 0; i < 20; i++ {
		l := a
		if i%2 == 1 {
			l = b
		}
		if rel, _, err := l.Acquire(tenant); err == nil {
			admitted++
			rel()
		} else if err.Code != "rate_limited" || err.status != 429 {
			t.Fatalf("unexpected %v", err)
		}
	}
	if admitted != 4 {
		t.Fatalf("admitted %d, want burst 4 across both instances", admitted)
	}
	f.advance(2 * time.Second)
	n := 0
	for i := 0; i < 6; i++ {
		l := []*Limiter{a, b}[i%2]
		if rel, _, err := l.Acquire(tenant); err == nil {
			n++
			rel()
		}
	}
	if n != 2 {
		t.Fatalf("refill admitted %d, want 2", n)
	}
	f.mu.Lock()
	evals, evalshas := f.evals, f.evalshas
	f.mu.Unlock()
	if evals != 1 || evalshas < 2 {
		t.Fatalf("expected EVALSHA with a single EVAL fallback, got evalsha=%d eval=%d", evalshas, evals)
	}
}

func TestRedisConcurrencyAndRelease(t *testing.T) {
	f := newFakeRedis(t)
	cfg := LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 2}
	a := sharedLimiter(t, f, "redis://"+f.addr(), cfg, FailLocal)
	b := sharedLimiter(t, f, "redis://"+f.addr(), cfg, FailLocal)
	r1, _, e1 := a.Acquire(tenant)
	r2, _, e2 := b.Acquire(tenant)
	if e1 != nil || e2 != nil {
		t.Fatal(e1, e2)
	}
	if _, _, err := a.Acquire(tenant); err == nil || err.Code != "too_many_concurrent_queries" {
		t.Fatalf("want concurrency 429, got %v", err)
	}
	r1()
	r1() // idempotent
	if r, _, err := b.Acquire(tenant); err != nil {
		t.Fatalf("slot not freed: %v", err)
	} else {
		r()
	}
	r2()
	if n := f.slots(concKey(tenant)); n != 0 {
		t.Fatalf("leaked %d slots", n)
	}
}

func TestRedisLeaseExpiryFreesCrashedSlot(t *testing.T) {
	f := newFakeRedis(t)
	cfg := LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 1, SlotLease: 10 * time.Second}
	a := sharedLimiter(t, f, "redis://"+f.addr(), cfg, FailLocal)
	b := sharedLimiter(t, f, "redis://"+f.addr(), cfg, FailLocal)
	if _, _, err := a.Acquire(tenant); err != nil { // never released: replica "crashed"
		t.Fatal(err)
	}
	if _, _, err := b.Acquire(tenant); err == nil {
		t.Fatal("cap not enforced")
	}
	f.advance(9 * time.Second)
	if _, _, err := b.Acquire(tenant); err == nil {
		t.Fatal("lease expired too early")
	}
	f.advance(2 * time.Second)
	if rel, _, err := b.Acquire(tenant); err != nil {
		t.Fatalf("lease did not expire: %v", err)
	} else {
		rel()
	}
}

func TestRedisTenantIsolationAndKeys(t *testing.T) {
	f := newFakeRedis(t)
	cfg := LimiterConfig{RatePerSec: 1, Burst: 1, MaxConcurrent: 1}
	l := sharedLimiter(t, f, "redis://"+f.addr(), cfg, FailLocal)
	if _, _, err := l.Acquire(tenant); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Acquire(tenant); err == nil {
		t.Fatal("A not limited")
	}
	if _, _, err := l.Acquire(tenantB); err != nil {
		t.Fatalf("B affected by A: %v", err)
	}
	for _, k := range f.concKeys() {
		if k != rateKey(tenant) && k != concKey(tenant) && k != rateKey(tenantB) && k != concKey(tenantB) {
			t.Fatalf("unexpected key %q", k)
		}
	}
}

func TestRedisFallbackLocalWhenDown(t *testing.T) {
	f := newFakeRedis(t)
	cfg := LimiterConfig{RatePerSec: 1, Burst: 2, MaxConcurrent: 5}
	l := sharedLimiter(t, f, "redis://"+f.addr(), cfg, FailLocal)
	clk := &clock{t: time.Unix(1_000_000, 0)}
	l.now = clk.now
	f.stop()
	ok := 0
	for i := 0; i < 5; i++ {
		rel, _, err := l.Acquire(tenant)
		if err == nil {
			ok++
			rel()
		} else if err.Code != "rate_limited" {
			t.Fatalf("unexpected %v", err)
		}
	}
	if ok != 1 { // seeded at FallbackFraction(0.5)*burst, no Redis-known count
		t.Fatalf("local fallback admitted %d, want seed 1", ok)
	}
	fb, rj := l.SharedStats()
	if fb != 5 || rj != 0 {
		t.Fatalf("stats fallbacks=%d rejected=%d", fb, rj)
	}
}

func TestRedisFailClosed503(t *testing.T) {
	f := newFakeRedis(t)
	l := sharedLimiter(t, f, "redis://"+f.addr(), LimiterConfig{}, FailClosed)
	f.stop()
	for i := 0; i < 3; i++ {
		_, ra, err := l.Acquire(tenant)
		if err == nil || err.status != 503 || err.Code != "limiter_unavailable" || ra != 1 {
			t.Fatalf("want 503, got %v ra=%d", err, ra)
		}
	}
	if _, rj := l.SharedStats(); rj != 3 {
		t.Fatalf("rejected=%d", rj)
	}
}

func TestRedisRecoversAfterCooldown(t *testing.T) {
	f := newFakeRedis(t)
	l := sharedLimiter(t, f, "redis://"+f.addr(), LimiterConfig{RatePerSec: 100, Burst: 100, MaxConcurrent: 5}, FailClosed)
	clk := &clock{t: time.Unix(1_000_000, 0)}
	l.now = clk.now
	l.shared.(*RedisCounter).DialTimeout = 100 * time.Millisecond
	addr := f.addr()
	f.stop()
	if _, _, err := l.Acquire(tenant); err == nil || err.status != 503 {
		t.Fatalf("want 503, got %v", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skip("port reuse failed")
	}
	f2 := &fakeRedis{ln: ln, now: time.Unix(2_000_000, 0), rate: map[string][2]float64{}, conc: map[string]map[string]int64{}}
	go f2.serve()
	defer f2.stop()
	clk.add(sharedCooldown + time.Second)
	if rel, _, err := l.Acquire(tenant); err != nil {
		t.Fatalf("did not recover: %v", err)
	} else {
		rel()
	}
}

func TestRedisAuthFromURL(t *testing.T) {
	f := newFakeRedis(t)
	f.mu.Lock()
	f.password = "s3cret"
	f.mu.Unlock()
	good := sharedLimiter(t, f, "redis://:s3cret@"+f.addr()+"/2", LimiterConfig{}, FailClosed)
	if rel, _, err := good.Acquire(tenant); err != nil {
		t.Fatalf("auth: %v", err)
	} else {
		rel()
	}
	if f.authed.Load() == 0 {
		t.Fatal("AUTH not sent")
	}
	bad := sharedLimiter(t, f, "redis://u:wrong@"+f.addr(), LimiterConfig{}, FailClosed)
	if _, _, err := bad.Acquire(tenant); err == nil || err.status != 503 {
		t.Fatalf("bad password should fail closed, got %v", err)
	}
}

func TestLoadShared(t *testing.T) {
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if sc, mode, err := LoadShared(get(nil)); err != nil || sc != nil || mode != FailLocal {
		t.Fatalf("unset: %v %v %v", sc, mode, err)
	}
	if sc, mode, err := LoadShared(get(map[string]string{"QUERY_REDIS_URL": "redis://h:1/0", "QUERY_REDIS_FAILURE": "closed"})); err != nil || sc == nil || mode != FailClosed {
		t.Fatalf("closed: %v %v %v", sc, mode, err)
	}
	for _, m := range []map[string]string{
		{"QUERY_REDIS_FAILURE": "open"}, {"QUERY_REDIS_URL": "http://x"}, {"QUERY_REDIS_URL": "redis://h/x"}, {"QUERY_REDIS_URL": "redis://"},
	} {
		if _, _, err := LoadShared(get(m)); err == nil {
			t.Fatalf("%v should fail", m)
		}
	}
}

func TestNoRedisUnchanged(t *testing.T) {
	l, _ := newTestLimiter(LimiterConfig{RatePerSec: 1, Burst: 1, MaxConcurrent: 1})
	if rel, _, err := l.Acquire(tenant); err != nil {
		t.Fatal(err)
	} else {
		rel()
	}
	if fb, rj := l.SharedStats(); fb != 0 || rj != 0 {
		t.Fatal("stats should be zero without shared store")
	}
}

func TestRedisCounterDomainAndGlobalFailure(t *testing.T) {
	rc, err := NewRedisCounter("redis://127.0.0.1:7000")
	if err != nil {
		t.Fatal(err)
	}
	if got := rc.FailureDomain("t"); got != "127.0.0.1:7000" {
		t.Fatalf("unknown topology -> first seed, got %q", got)
	}
	rc.ranges = []slotRange{{0, 16383, "10.0.0.1:6379"}}
	if got := rc.FailureDomain("t"); got != "10.0.0.1:6379" {
		t.Fatalf("got %q", got)
	}
	if !rc.GlobalFailure(redisError("CLUSTERDOWN The cluster is down")) || rc.GlobalFailure(redisError("CLUSTERDOWN Hash slot not served")) || rc.GlobalFailure(fmt.Errorf("x")) {
		t.Fatal("GlobalFailure classification wrong")
	}
}
