package query

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- test PKI (generated in the test, never committed) ----

type testPKI struct {
	caPEM  []byte
	server tls.Certificate
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "query-api test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	srvKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	srvTpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "redis.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, DNSNames: []string{"redis.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTpl, ca, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return testPKI{
		caPEM:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		server: tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey},
	}
}

func (p testPKI) caFile(t *testing.T) string {
	f := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(f, p.caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func newFakeRedisTLS(t *testing.T, p testPKI) *fakeRedis {
	return newFakeRedisL(t, func(ln net.Listener) net.Listener {
		return tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{p.server}, MinVersion: tls.VersionTLS12})
	})
}

func limiterFor(t *testing.T, url string, o RedisOptions, cfg LimiterConfig, mode FailureMode) *Limiter {
	t.Helper()
	rc, err := NewRedisCounterOpts(url, o)
	if err != nil {
		t.Fatal(err)
	}
	rc.CmdTimeout, rc.DialTimeout = 500*time.Millisecond, 500*time.Millisecond
	l := NewLimiter(cfg)
	l.UseShared(rc, mode)
	return l
}

// ---- keys / slots ----

func TestKeySlotVectors(t *testing.T) {
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Fatalf("crc16 = %#x", got)
	}
	if got := keySlot("foo"); got != 12182 { // documented in the Redis Cluster spec
		t.Fatalf("slot(foo) = %d", got)
	}
	if keySlot("{user1000}.following") != keySlot("{user1000}.followers") {
		t.Fatal("hash tag not honoured")
	}
	if keySlot(rateKey(tenant)) != keySlot(concKey(tenant)) {
		t.Fatal("a tenant's two keys must share a slot")
	}
	// a hostile id with braces cannot end the tag early or collide with a plain id
	if k := rateKey("a}b{c"); !strings.HasPrefix(k, "q:rate:{a%7Db%7Bc}") {
		t.Fatalf("tag not escaped: %q", k)
	}
	if keySlot(rateKey("}x")) != keySlot(concKey("}x")) {
		t.Fatal("brace tenant splits slots")
	}
}

// ---- TLS (rediss://) ----

func TestRedissAgainstFakeTLS(t *testing.T) {
	pki := newTestPKI(t)
	f := newFakeRedisTLS(t, pki)
	cfg := LimiterConfig{RatePerSec: 1, Burst: 4, MaxConcurrent: 100}
	o := RedisOptions{CAFile: pki.caFile(t)}
	a := limiterFor(t, "rediss://"+f.addr(), o, cfg, FailClosed)
	b := limiterFor(t, "rediss://"+f.addr(), o, cfg, FailClosed)
	admitted := 0
	for i := 0; i < 20; i++ {
		l := []*Limiter{a, b}[i%2]
		if rel, _, err := l.Acquire(tenant); err == nil {
			admitted++
			rel()
		} else if err.Code != "rate_limited" {
			t.Fatalf("unexpected %v", err)
		}
	}
	if admitted != 4 {
		t.Fatalf("admitted %d over TLS across two instances, want burst 4", admitted)
	}
	// override of the verified name that matches the cert SAN
	c := limiterFor(t, "rediss://"+f.addr(), RedisOptions{CAFile: o.CAFile, ServerName: "redis.test"}, cfg, FailClosed)
	if rel, _, err := c.Acquire(tenantB); err != nil {
		t.Fatalf("server name override: %v", err)
	} else {
		rel()
	}
}

func TestRedissVerificationFailsClosed(t *testing.T) {
	pki := newTestPKI(t)
	other := newTestPKI(t) // different CA
	f := newFakeRedisTLS(t, pki)
	for name, tc := range map[string]RedisOptions{
		"system roots do not trust the test CA": {},
		"wrong CA":                              {CAFile: other.caFile(t)},
		"server name mismatch":                  {CAFile: pki.caFile(t), ServerName: "evil.example"},
	} {
		l := limiterFor(t, "rediss://"+f.addr(), tc, LimiterConfig{}, FailClosed)
		if _, _, err := l.Acquire(tenant); err == nil || err.status != 503 {
			t.Fatalf("%s: connection must fail, got %v", name, err)
		}
	}
	// plaintext client against a TLS port also fails (no silent downgrade)
	l := limiterFor(t, "redis://"+f.addr(), RedisOptions{}, LimiterConfig{}, FailClosed)
	if _, _, err := l.Acquire(tenant); err == nil {
		t.Fatal("plaintext against TLS server must fail")
	}
	// TLS client against a plaintext server fails too
	p := newFakeRedis(t)
	l = limiterFor(t, "rediss://"+p.addr(), RedisOptions{CAFile: pki.caFile(t)}, LimiterConfig{}, FailClosed)
	if _, _, err := l.Acquire(tenant); err == nil {
		t.Fatal("TLS against plaintext server must fail")
	}
}

func TestRedisTLSOptionValidation(t *testing.T) {
	pki := newTestPKI(t)
	bad := filepath.Join(t.TempDir(), "bad.pem")
	_ = os.WriteFile(bad, []byte("not pem"), 0o600)
	for name, tc := range map[string]struct {
		url string
		o   RedisOptions
	}{
		"CA with redis://":      {"redis://h:1", RedisOptions{CAFile: pki.caFile(t)}},
		"server name w/ redis:": {"redis://h:1", RedisOptions{ServerName: "x"}},
		"missing CA file":       {"rediss://h:1", RedisOptions{CAFile: "/nonexistent/ca.pem"}},
		"garbage CA file":       {"rediss://h:1", RedisOptions{CAFile: bad}},
		"bad node":              {"redis://h:1", RedisOptions{Nodes: []string{"h:x"}}},
	} {
		if _, err := NewRedisCounterOpts(tc.url, tc.o); err == nil {
			t.Fatalf("%s should fail", name)
		}
	}
	rc, err := NewRedisCounterOpts("rediss://h", RedisOptions{CAFile: pki.caFile(t), Nodes: []string{"h:6379", "x", "y:7000"}})
	if err != nil {
		t.Fatal(err)
	}
	if rc.tlsCfg == nil || rc.tlsCfg.InsecureSkipVerify || rc.tlsCfg.MinVersion != tls.VersionTLS12 {
		t.Fatal("TLS config must verify and require >= 1.2")
	}
	if got := strings.Join(rc.nodes, ","); got != "h:6379,x:6379,y:7000" {
		t.Fatalf("nodes = %s", got)
	}
	env := map[string]string{"QUERY_REDIS_URL": "rediss://h:6380", "QUERY_REDIS_CA_FILE": pki.caFile(t),
		"QUERY_REDIS_TLS_SERVER_NAME": "redis.test", "QUERY_REDIS_NODES": " a:1, b:2 ,"}
	sc, _, err := LoadShared(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if r := sc.(*RedisCounter); r.serverName != "redis.test" || len(r.nodes) != 3 {
		t.Fatalf("env not applied: %+v", r.nodes)
	}
}

// ---- Redis Cluster redirects ----

func movedTo(slot uint16, addr string) string { return fmt.Sprintf("-MOVED %d %s", slot, addr) }

func TestRedisFollowsMovedAndCachesSlot(t *testing.T) {
	a, b := newFakeRedis(t), newFakeRedis(t)
	a.hook = func([]string, bool) string { return movedTo(keySlot(rateKey(tenant)), b.addr()) }
	cfg := LimiterConfig{RatePerSec: 1, Burst: 3, MaxConcurrent: 1}
	l := limiterFor(t, "redis://"+a.addr(), RedisOptions{}, cfg, FailClosed)
	rel, _, err := l.Acquire(tenant)
	if err != nil {
		t.Fatalf("MOVED not followed: %v", err)
	}
	if a.keyed.Load() != 1 {
		t.Fatalf("first node saw %d keyed commands, want exactly 1 (the redirected one)", a.keyed.Load())
	}
	if _, _, err := l.Acquire(tenant); err == nil || err.Code != "too_many_concurrent_queries" {
		t.Fatalf("cap must be enforced on the new owner: %v", err)
	}
	rel() // ZREM goes straight to b
	if b.slots(concKey(tenant)) != 0 {
		t.Fatal("release did not reach the owning node")
	}
	for i := 0; i < 3; i++ {
		if r, _, err := l.Acquire(tenant); err == nil {
			r()
		}
	}
	if n := a.keyed.Load(); n != 1 {
		t.Fatalf("slot cache not used: first node saw %d keyed commands", n)
	}
	// a tenant in another slot is routed independently (cache is per slot)
	if keySlot(rateKey(tenantB)) == keySlot(rateKey(tenant)) {
		t.Skip("slot collision")
	}
	a.hook = nil
	if r, _, err := l.Acquire(tenantB); err != nil {
		t.Fatal(err)
	} else {
		r()
	}
	if a.keyed.Load() < 2 {
		t.Fatal("other slot should still start at the seed node")
	}
}

func TestRedisAskRetriesOnceWithAskingWithoutCaching(t *testing.T) {
	a, b := newFakeRedis(t), newFakeRedis(t)
	a.hook = func([]string, bool) string { return "-ASK 1 " + b.addr() }
	b.hook = func(_ []string, asking bool) string {
		if !asking {
			return "-ERR slot not served here without ASKING"
		}
		return ""
	}
	l := limiterFor(t, "redis://"+a.addr(), RedisOptions{}, LimiterConfig{RatePerSec: 100, Burst: 100, MaxConcurrent: 5}, FailClosed)
	for i := 0; i < 3; i++ {
		rel, _, err := l.Acquire(tenant)
		if err != nil {
			t.Fatalf("ASK not followed: %v", err)
		}
		rel()
	}
	if a.keyed.Load() < 6 { // 3 acquire attempts (+ NOSCRIPT retry) + 3 releases always go to the seed again
		t.Fatalf("ASK must not update the slot cache; first node saw %d", a.keyed.Load())
	}
	if b.asked.Load() != b.keyed.Load() || b.keyed.Load() == 0 {
		t.Fatalf("every redirected command needs ASKING: asked=%d keyed=%d", b.asked.Load(), b.keyed.Load())
	}
}

func TestRedisRedirectLoopIsBounded(t *testing.T) {
	a, b := newFakeRedis(t), newFakeRedis(t)
	a.hook = func([]string, bool) string { return movedTo(1, b.addr()) }
	b.hook = func([]string, bool) string { return movedTo(1, a.addr()) }
	l := limiterFor(t, "redis://"+a.addr(), RedisOptions{}, LimiterConfig{}, FailClosed)
	if _, _, err := l.Acquire(tenant); err == nil || err.status != 503 {
		t.Fatalf("redirect loop must end in an error, got %v", err)
	}
}

func TestRedisMovedEmptyHostUsesCurrentNode(t *testing.T) {
	a, b := newFakeRedis(t), newFakeRedis(t)
	_, port, _ := net.SplitHostPort(b.addr())
	a.hook = func([]string, bool) string { return fmt.Sprintf("-MOVED 5 :%s", port) }
	l := limiterFor(t, "redis://"+a.addr(), RedisOptions{}, LimiterConfig{}, FailClosed)
	if rel, _, err := l.Acquire(tenant); err != nil {
		t.Fatal(err)
	} else {
		rel()
	}
}

func TestRedisSeedNodeFallbackWhenFirstDown(t *testing.T) {
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	dead.Close()
	live := newFakeRedis(t)
	l := limiterFor(t, "redis://"+deadAddr, RedisOptions{Nodes: []string{live.addr()}}, LimiterConfig{}, FailClosed)
	if rel, _, err := l.Acquire(tenant); err != nil {
		t.Fatalf("seed fallback: %v", err)
	} else {
		rel()
	}
	l = limiterFor(t, "redis://"+deadAddr, RedisOptions{}, LimiterConfig{}, FailClosed)
	if _, _, err := l.Acquire(tenant); err == nil {
		t.Fatal("no seeds left must fail")
	}
}

func TestRedisClusterOverTLS(t *testing.T) {
	pki := newTestPKI(t)
	a, b := newFakeRedisTLS(t, pki), newFakeRedisTLS(t, pki)
	a.hook = func([]string, bool) string { return movedTo(1, b.addr()) }
	l := limiterFor(t, "rediss://"+a.addr(), RedisOptions{CAFile: pki.caFile(t)}, LimiterConfig{RatePerSec: 1, Burst: 2, MaxConcurrent: 9}, FailClosed)
	n := 0
	for i := 0; i < 6; i++ {
		if r, _, err := l.Acquire(tenant); err == nil {
			n++
			r()
		}
	}
	if n != 2 {
		t.Fatalf("admitted %d, want burst 2", n)
	}
}

// ---- lease renewal (fake with a real clock, short leases) ----

func realClockFake(t *testing.T) *fakeRedis {
	f := newFakeRedis(t)
	f.clk = time.Now
	return f
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestRenewKeepsLongQueryCountedAndKilledHolderExpires(t *testing.T) {
	f := realClockFake(t)
	cfg := LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 2, SlotLease: 300 * time.Millisecond}
	a := limiterFor(t, "redis://"+f.addr(), RedisOptions{}, cfg, FailClosed)
	b := limiterFor(t, "redis://"+f.addr(), RedisOptions{}, cfg, FailClosed)
	relLong, _, err := a.Acquire(tenant) // long query on A: renewed in the background
	if err != nil {
		t.Fatal(err)
	}
	killed, kerr := b.shared.Acquire(tenant, b.cfg) // replica B "killed": slot taken but never renewed or released
	if kerr != nil || !killed.Admitted {
		t.Fatal(kerr, killed)
	}
	if a.renewing.Load() != 1 {
		t.Fatalf("renewers = %d", a.renewing.Load())
	}
	time.Sleep(1000 * time.Millisecond) // > 3 leases
	if f.slots(concKey(tenant)) < 1 {
		t.Fatal("test setup")
	}
	// the killed slot expired; the long query still holds exactly one slot -> one more fits, then the cap bites
	r1, _, err := b.Acquire(tenant)
	if err != nil {
		t.Fatalf("killed holder's slot did not expire: %v", err)
	}
	if _, _, err := b.Acquire(tenant); err == nil || err.Code != "too_many_concurrent_queries" {
		t.Fatalf("long query lost its slot after the lease elapsed: %v", err)
	}
	r1()
	relLong()
	waitFor(t, "renewer to stop", time.Second, func() bool { return a.renewing.Load() == 0 })
	if r, _, err := b.Acquire(tenant); err != nil { // slot freed by release, not by expiry
		t.Fatalf("release did not free: %v", err)
	} else {
		r()
	}
}

func TestRenewStopsOnContextCancelAndSlotThenExpires(t *testing.T) {
	f := realClockFake(t)
	cfg := LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 1, SlotLease: 300 * time.Millisecond}
	l := limiterFor(t, "redis://"+f.addr(), RedisOptions{}, cfg, FailClosed)
	ctx, cancel := contextWithCancel()
	if _, _, err := l.AcquireCtx(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // renewed past one lease
	if _, _, err := l.Acquire(tenant); err == nil {
		t.Fatal("slot should still be held while ctx is live")
	}
	cancel()
	waitFor(t, "renewer to stop on cancel", time.Second, func() bool { return l.renewing.Load() == 0 })
	time.Sleep(450 * time.Millisecond) // lease (300ms) since the last renewal has passed
	if r, _, err := l.Acquire(tenant); err != nil {
		t.Fatalf("slot of a cancelled, unreleased request must expire: %v", err)
	} else {
		r()
	}
}

func TestRenewNeverResurrectsReleasedOrExpiredSlot(t *testing.T) {
	f := realClockFake(t)
	cfg := LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 1, SlotLease: 100 * time.Millisecond}
	rc, _ := NewRedisCounter("redis://" + f.addr())
	d, err := rc.Acquire(tenant, cfg)
	if err != nil || !d.Admitted {
		t.Fatal(err)
	}
	if ok, err := rc.Renew(tenant, d.Slot, cfg); err != nil || !ok {
		t.Fatalf("live renew: %v %v", ok, err)
	}
	_ = rc.Release(tenant, d.Slot)
	if ok, _ := rc.Renew(tenant, d.Slot, cfg); ok {
		t.Fatal("released slot resurrected")
	}
	d2, _ := rc.Acquire(tenant, cfg)
	time.Sleep(150 * time.Millisecond)
	if ok, _ := rc.Renew(tenant, d2.Slot, cfg); ok {
		t.Fatal("expired slot resurrected")
	}
}

func TestRenewFailureDoesNotBreakRequestAndLocalLimiterHasNoRenewer(t *testing.T) {
	f := realClockFake(t)
	cfg := LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 1, SlotLease: 90 * time.Millisecond}
	l := limiterFor(t, "redis://"+f.addr(), RedisOptions{}, cfg, FailLocal)
	rel, _, err := l.Acquire(tenant)
	if err != nil {
		t.Fatal(err)
	}
	f.stop() // Redis dies while the query runs: renewals fail, loop keeps retrying quietly
	time.Sleep(250 * time.Millisecond)
	rel() // release error is logged, never panics
	waitFor(t, "renewer to stop", time.Second, func() bool { return l.renewing.Load() == 0 })

	loc, _ := newTestLimiter(LimiterConfig{MaxConcurrent: 1})
	r, _, _ := loc.Acquire(tenant)
	if loc.renewing.Load() != 0 {
		t.Fatal("in-memory limiter must not renew")
	}
	r()
}

// ---- LIVE tests against real containers ----
//   QUERY_REDIS_IT_URL         redis://127.0.0.1:16390                 single node
//   QUERY_REDIS_IT_TLS_URL     rediss://127.0.0.1:16391 + QUERY_REDIS_IT_TLS_CA=<ca.pem>
//   QUERY_REDIS_IT_CLUSTER     host:port,host:port,...  3-master cluster seeds (run from a container on its network)

type liveMode struct {
	name string
	mk   func(t *testing.T) (url string, o RedisOptions)
}

func liveModes() []liveMode {
	return []liveMode{
		{"single", func(t *testing.T) (string, RedisOptions) {
			u := os.Getenv("QUERY_REDIS_IT_URL")
			if u == "" {
				t.Skip("QUERY_REDIS_IT_URL not set")
			}
			return u, RedisOptions{}
		}},
		{"tls", func(t *testing.T) (string, RedisOptions) {
			u := os.Getenv("QUERY_REDIS_IT_TLS_URL")
			if u == "" {
				t.Skip("QUERY_REDIS_IT_TLS_URL not set")
			}
			return u, RedisOptions{CAFile: os.Getenv("QUERY_REDIS_IT_TLS_CA")}
		}},
		{"cluster", func(t *testing.T) (string, RedisOptions) {
			c := os.Getenv("QUERY_REDIS_IT_CLUSTER")
			if c == "" {
				t.Skip("QUERY_REDIS_IT_CLUSTER not set")
			}
			nodes := strings.Split(c, ",")
			return "redis://" + nodes[0], RedisOptions{Nodes: nodes[1:]}
		}},
	}
}

func liveTenant(suffix string) string { return fmt.Sprintf("it-%d-%s", time.Now().UnixNano(), suffix) }

func TestLiveRedis(t *testing.T) {
	for _, m := range liveModes() {
		m := m
		t.Run(m.name, func(t *testing.T) {
			url, o := m.mk(t)
			mk := func(cfg LimiterConfig) *Limiter {
				rc, err := NewRedisCounterOpts(url, o)
				if err != nil {
					t.Fatal(err)
				}
				rc.CmdTimeout, rc.DialTimeout = 2*time.Second, 2*time.Second
				l := NewLimiter(cfg)
				l.UseShared(rc, FailClosed)
				return l
			}
			t.Run("combined rate not doubled", func(t *testing.T) { liveRate(t, mk) })
			t.Run("concurrent cap across instances", func(t *testing.T) { liveCap(t, mk) })
			t.Run("killed holder expires, long query keeps slot", func(t *testing.T) { liveRenew(t, mk) })
			t.Run("tenant isolation", func(t *testing.T) { liveIsolation(t, mk) })
			if m.name == "cluster" {
				t.Run("MOVED followed across slots", func(t *testing.T) { liveMoved(t, url, o) })
			}
		})
	}
}

func liveRate(t *testing.T, mk func(LimiterConfig) *Limiter) {
	cfg := LimiterConfig{RatePerSec: 20, Burst: 20, MaxConcurrent: 1000}
	a, b := mk(cfg), mk(cfg)
	ten := liveTenant("rate")
	var admitted atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for _, l := range []*Limiter{a, b} {
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(l *Limiter) {
				defer wg.Done()
				for time.Since(start) < 3*time.Second {
					rel, _, err := l.Acquire(ten)
					if err == nil {
						admitted.Add(1)
						rel()
					} else if err.status == 503 {
						t.Errorf("redis error: %v", err)
						return
					}
					time.Sleep(2 * time.Millisecond)
				}
			}(l)
		}
	}
	wg.Wait()
	el := time.Since(start).Seconds()
	want := 20 + 20*el
	got := float64(admitted.Load())
	t.Logf("admitted %.0f in %.2fs across 2 instances; expected ~%.0f (2x would be ~%.0f)", got, el, want, 2*want-20)
	if got > want+3 || got < want*0.8 {
		t.Fatalf("admitted %.0f, want ~%.0f", got, want)
	}
}

func liveCap(t *testing.T, mk func(LimiterConfig) *Limiter) {
	cfg := LimiterConfig{RatePerSec: 10000, Burst: 10000, MaxConcurrent: 3}
	a, b := mk(cfg), mk(cfg)
	ten := liveTenant("conc")
	var rels []func()
	for i := 0; i < 3; i++ {
		rel, _, err := []*Limiter{a, b}[i%2].Acquire(ten)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		rels = append(rels, rel)
	}
	for _, l := range []*Limiter{a, b} {
		if _, _, err := l.Acquire(ten); err == nil || err.Code != "too_many_concurrent_queries" {
			t.Fatalf("cap not enforced across instances: %v", err)
		}
	}
	rels[0]()
	if rel, _, err := b.Acquire(ten); err != nil {
		t.Fatalf("release on A did not free slot for B: %v", err)
	} else {
		rel()
	}
	for _, r := range rels[1:] {
		r()
	}
}

func liveRenew(t *testing.T, mk func(LimiterConfig) *Limiter) {
	cfg := LimiterConfig{RatePerSec: 10000, Burst: 10000, MaxConcurrent: 2, SlotLease: 2 * time.Second}
	a, b := mk(cfg), mk(cfg)
	ten := liveTenant("lease")
	relLong, _, err := a.Acquire(ten) // long query, renewed every ~667ms
	if err != nil {
		t.Fatal(err)
	}
	killed, kerr := b.shared.Acquire(ten, b.cfg) // killed replica: never renews or releases
	if kerr != nil || !killed.Admitted {
		t.Fatal(kerr, killed)
	}
	if _, _, err := b.Acquire(ten); err == nil {
		t.Fatal("cap not enforced")
	}
	time.Sleep(5 * time.Second) // 2.5 leases
	r1, _, err := b.Acquire(ten)
	if err != nil {
		t.Fatalf("killed holder's slot did not expire: %v", err)
	}
	if _, _, err := b.Acquire(ten); err == nil || err.Code != "too_many_concurrent_queries" {
		t.Fatalf("long query lost its slot after > lease: %v", err)
	}
	r1()
	relLong()
	if r, _, err := b.Acquire(ten); err != nil {
		t.Fatalf("release did not free the slot: %v", err)
	} else {
		r()
	}
}

func liveIsolation(t *testing.T, mk func(LimiterConfig) *Limiter) {
	cfg := LimiterConfig{RatePerSec: 0.1, Burst: 2, MaxConcurrent: 1}
	a, b := mk(cfg), mk(cfg)
	ta, tb := liveTenant("iso-a"), liveTenant("iso-b")
	for i := 0; i < 2; i++ {
		rel, _, err := a.Acquire(ta)
		if err != nil {
			t.Fatal(err)
		}
		rel()
	}
	if _, _, err := b.Acquire(ta); err == nil || err.Code != "rate_limited" {
		t.Fatalf("A should be rate limited via other instance: %v", err)
	}
	for i := 0; i < 2; i++ {
		rel, _, err := b.Acquire(tb)
		if err != nil {
			t.Fatalf("tenant B affected by A: %v", err)
		}
		rel()
	}
}

// liveMoved seeds a counter with a single node and drives many tenants so some slots live on other masters: every
// request must succeed (MOVED followed) and the slot cache must have learned at least one redirect.
func liveMoved(t *testing.T, url string, o RedisOptions) {
	rc, err := NewRedisCounterOpts(url, RedisOptions{CAFile: o.CAFile}) // only the URL node as seed
	if err != nil {
		t.Fatal(err)
	}
	rc.CmdTimeout, rc.DialTimeout = 2*time.Second, 2*time.Second
	cfg := LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 1}
	addrs := map[string]bool{}
	for i := 0; i < 30; i++ {
		ten := liveTenant(fmt.Sprintf("mv%d", i))
		d, err := rc.Acquire(ten, cfg)
		if err != nil || !d.Admitted {
			t.Fatalf("tenant %d: %v %+v", i, err, d)
		}
		if d2, _ := rc.Acquire(ten, cfg); d2.Admitted || d2.Reason != "concurrent" {
			t.Fatalf("tenant %d: cap not enforced after redirect: %+v", i, d2)
		}
		if err := rc.Release(ten, d.Slot); err != nil {
			t.Fatal(err)
		}
	}
	rc.mu.Lock()
	for _, a := range rc.slotAddr {
		addrs[a] = true
	}
	n := len(rc.slotAddr)
	rc.mu.Unlock()
	t.Logf("learned %d slot->node entries across %d distinct nodes from MOVED", n, len(addrs))
	if n == 0 || len(addrs) < 1 {
		t.Fatal("no MOVED was followed; are the seed and the key owners the same node?")
	}
}

func contextWithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}
