package query

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// LIVE failover test against a TLS-enabled, requirepass-protected 3-master + 3-replica Redis 7 Cluster.
//
//	QUERY_REDIS_IT_TLSAUTH  = name=ip:port,...   container-name=address of the 6 nodes
//	QUERY_REDIS_IT_PASS     = cluster password (requirepass/masterauth); QUERY_REDIS_IT_TLS_CA = CA PEM;
//	QUERY_REDIS_IT_TLS_NAME = DNS name in the node certificates (nodes are dialled by IP)
//	QUERY_REDIS_IT_CTL      = shared dir with the host-side "stop|start <container>" executor (see README)
//
// Four limiters watch the same outage with disjoint tenants: {local,closed} x {global cooldown (the behaviour before
// the per-slot change, emulated by hiding FailureDomain), per-domain cooldown (current code)}.

type foCount struct {
	SharedCounter
	mu       sync.Mutex
	ok       map[string]int
	errs     map[string]int // "<tenant class> <error>" -> count
	onVictim func(string) bool
	killAt   *time.Time
}

func (c *foCount) Acquire(t string, cfg LimiterConfig) (SharedDecision, error) {
	d, err := c.SharedCounter.Acquire(t, cfg)
	if err == nil {
		c.mu.Lock()
		c.ok[t]++
		c.mu.Unlock()
	}
	if err != nil {
		cls := "others"
		if c.onVictim != nil && c.onVictim(t) {
			cls = "victim"
		}
		k := fmt.Sprintf("%s %.90s", cls, err.Error())
		c.mu.Lock()
		c.errs[k]++
		c.mu.Unlock()
	}
	return d, err
}

type foCountDomain struct{ *foCount }

func (c foCountDomain) GlobalFailure(err error) bool {
	return c.foCount.SharedCounter.(DomainResolver).GlobalFailure(err)
}

func (c foCountDomain) FailureDomain(t string) string {
	return c.foCount.SharedCounter.(DomainResolver).FailureDomain(t)
}

func TestLiveTLSAuthFailoverCluster(t *testing.T) {
	v := os.Getenv("QUERY_REDIS_IT_TLSAUTH")
	pass := os.Getenv("QUERY_REDIS_IT_PASS")
	if v == "" || pass == "" || os.Getenv("QUERY_REDIS_IT_CTL") == "" {
		t.Skip("QUERY_REDIS_IT_TLSAUTH / _PASS / _CTL not set")
	}
	var nodes []foNode
	for _, p := range strings.Split(v, ",") {
		kv := strings.SplitN(p, "=", 2)
		nodes = append(nodes, foNode{kv[0], kv[1]})
	}
	opts := RedisOptions{CAFile: os.Getenv("QUERY_REDIS_IT_TLS_CA"), ServerName: os.Getenv("QUERY_REDIS_IT_TLS_NAME"), Nodes: addrs(nodes[1:])}
	url := "rediss://:" + pass + "@" + nodes[0].addr
	newRC := func(o RedisOptions, u string) *RedisCounter {
		rc, err := NewRedisCounterOpts(u, o)
		if err != nil {
			t.Fatal(err)
		}
		rc.CmdTimeout, rc.DialTimeout = 500*time.Millisecond, 500*time.Millisecond
		return rc
	}
	cfg := LimiterConfig{RatePerSec: 10, Burst: 10, MaxConcurrent: 1000, SlotLease: 5 * time.Second}

	// --- AUTH + TLS really enforced by the server (negative controls) ---
	probeCfg := LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 10}
	if _, err := newRC(opts, "rediss://"+nodes[0].addr).Acquire("neg", probeCfg); err == nil {
		t.Error("no password: expected NOAUTH error")
	} else {
		t.Logf("no password -> %v", err)
	}
	if _, err := newRC(opts, "rediss://:wrong@"+nodes[0].addr).Acquire("neg", probeCfg); err == nil {
		t.Error("wrong password accepted")
	} else {
		t.Logf("wrong password -> %v", err)
	}
	if _, err := newRC(RedisOptions{Nodes: opts.Nodes}, "redis://:"+pass+"@"+nodes[0].addr).Acquire("neg", probeCfg); err == nil {
		t.Error("plaintext against TLS port accepted")
	} else {
		t.Logf("plaintext against TLS port -> %v", err)
	}
	bad := opts
	bad.ServerName = "evil.example"
	if _, err := newRC(bad, url).Acquire("neg", probeCfg); err == nil {
		t.Error("wrong TLS name accepted")
	} else {
		t.Logf("wrong server name -> %v", err)
	}

	// --- topology refresh over TLS+AUTH ---
	rc0 := newRC(opts, url)
	rc0.minGap = 0
	if !rc0.refresh() || len(rc0.ranges) != 3 {
		t.Fatalf("CLUSTER SLOTS over TLS+AUTH: ranges=%v", rc0.ranges)
	}
	t.Logf("CLUSTER SLOTS over TLS+AUTH ok: %d master ranges %v", len(rc0.ranges), rc0.known)

	type variant struct {
		name   string
		mode   FailureMode
		domain bool
		l      *Limiter
		rc     *RedisCounter
		cnt    *foCount
		ld     *foLoad
		tn     []string
	}
	var vs []*variant
	for _, mode := range []FailureMode{FailLocal, FailClosed} {
		for _, dom := range []bool{false, true} {
			vs = append(vs, &variant{name: fmt.Sprintf("%s/%s", mode, map[bool]string{false: "global-cooldown", true: "per-slot"}[dom]), mode: mode, domain: dom})
		}
	}
	const perVariant = 12
	for i, x := range vs {
		x.rc = newRC(opts, url)
		x.rc.minGap = 0
		x.rc.refresh()
		x.rc.minGap = time.Second
		x.cnt = &foCount{SharedCounter: x.rc, ok: map[string]int{}, errs: map[string]int{}}
		x.l = NewLimiter(cfg)
		if x.domain {
			x.l.UseShared(foCountDomain{x.cnt}, x.mode)
		} else {
			x.l.UseShared(x.cnt, x.mode) // embeds only SharedCounter: no FailureDomain -> one global cooldown
		}
		x.tn = foTenants(fmt.Sprintf("tls%d", i), 0, perVariant, 0)
	}
	// victim: the master owning most of variant 0's tenants (same hash => same layout for every variant)
	own := map[string]int{}
	for _, tn := range vs[0].tn {
		own[vs[0].rc.owner(keySlot(rateKey(tn)))]++
	}
	victimAddr := ""
	for a, n := range own {
		if n > own[victimAddr] {
			victimAddr = a
		}
	}
	victim := foName(nodes, victimAddr)
	t.Logf("tenants per master: %v; victim %s", own, victim)

	onVictim := map[string]bool{}
	for _, x := range vs {
		for _, tn := range x.tn {
			onVictim[tn] = x.rc.owner(keySlot(rateKey(tn))) == victimAddr
		}
	}
	for _, x := range vs {
		x := x
		x.cnt.onVictim = func(tn string) bool { return onVictim[tn] }
	}
	probeTenant := ""
	for i := 0; ; i++ { // a dedicated tenant whose slot lives on the victim
		probeTenant = fmt.Sprintf("probe-tls-%d", i)
		if vs[0].rc.owner(keySlot(rateKey(probeTenant))) == victimAddr {
			break
		}
	}
	probe := startProbe(vs[0].rc, probeTenant)
	for _, x := range vs {
		x.ld = startLoad(x.l, x.tn, 50*time.Millisecond) // 20 req/s attempted per tenant vs rate 10
	}
	time.Sleep(3 * time.Second)
	for _, x := range vs { // the harness itself must not degrade anything before the kill
		if fb, rj := x.l.SharedStats(); fb+rj != 0 {
			t.Fatalf("%s: %d fallbacks / %d 503 before the master was stopped (harness overloaded)", x.name, fb, rj)
		}
	}
	killAt := time.Now()
	foCtl(t, "stop "+victim)
	t.Logf("docker stop %s took %.1fs", victim, time.Since(killAt).Seconds())
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		if lf := probe.last.Load(); lf != 0 && time.Since(time.Unix(0, lf)) > 4*time.Second {
			break
		}
	}
	if probe.first.Load() == 0 {
		t.Fatal("probe never failed: was the master really killed?")
	}
	down := probe.outage()
	probe.end()
	t.Logf("Redis path for the victim's slots down %.1fs (first failure +%.1fs after docker stop), %d failed probes",
		down.Seconds(), time.Unix(0, probe.first.Load()).Sub(killAt).Seconds(), probe.fails.Load())

	for _, x := range vs {
		el := x.ld.finish()
		var victimT, otherT []string
		for _, tn := range x.tn {
			if onVictim[tn] {
				victimT = append(victimT, tn)
			} else {
				otherT = append(otherT, tn)
			}
		}
		degraded := func(ts []string) (affected, attempts int) {
			x.cnt.mu.Lock()
			defer x.cnt.mu.Unlock()
			x.ld.mu.Lock()
			defer x.ld.mu.Unlock()
			for _, tn := range ts {
				d := x.ld.tried[tn] - x.cnt.ok[tn]
				attempts += d
				if d > 0 {
					affected++
				}
			}
			return
		}
		va, vd := degraded(victimT)
		oa, od := degraded(otherT)
		x.cnt.mu.Lock()
		t.Logf("%s Redis errors seen: %v", x.name, x.cnt.errs)
		x.cnt.mu.Unlock()
		fb, rj := x.l.SharedStats()
		t.Logf("%-22s tenants on victim %d, on others %d | affected: victim %d, others %d (of %d) | degraded requests victim %d, others %d | fallbacks %d, 503 %d | elapsed %.1fs",
			x.name, len(victimT), len(otherT), va, oa, len(x.tn), vd, od, fb, rj, el.Seconds())
		fullCov := os.Getenv("QUERY_REDIS_IT_FULLCOV") == "yes" // cluster-require-full-coverage yes: the whole cluster answers CLUSTERDOWN
		if !fullCov && x.domain { // the global-cooldown variants are the "before" measurement: they are expected to violate this
			foCheck(t, "others/"+x.name, x.ld, otherT, cfg, el, 0, 0) // healthy masters: exact Redis limits
		}
		if x.domain && oa != 0 && !fullCov {
			t.Errorf("%s: %d tenants on healthy masters were degraded (%d requests)", x.name, oa, od)
		}
		extra := 0.0
		if x.mode == FailLocal {
			extra = cfg.withDefaults().FallbackFraction*float64(cfg.Burst) + cfg.RatePerSec*(down.Seconds()+float64(sharedCooldown/time.Second)+4)
		}
		foCheck(t, "victim/"+x.name, x.ld, victimT, cfg, el, 1, extra)
	}

	foCtl(t, "start "+victim)
	time.Sleep(8 * time.Second)
	for _, tn := range vs[0].tn {
		if onVictim[tn] {
			if got := vs[0].rc.owner(keySlot(rateKey(tn))); got == "" || got == victimAddr {
				t.Errorf("client still routes %s to the dead master (%q)", tn, got)
			} else {
				t.Logf("slot of %s now served by %s (promoted)", tn, foName(nodes, got))
			}
			break
		}
	}
}
