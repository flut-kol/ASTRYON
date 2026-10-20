package query

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// LIVE failover / resharding tests against a real 6-node Redis 7 Cluster (3 masters + 3 replicas).
//
//	QUERY_REDIS_IT_FAILOVER = n1=ip:port,n2=ip:port,...   container-name=address of every node (6)
//	QUERY_REDIS_IT_CTL      = directory shared with a host-side driver that executes "stop|start <container>"
//	                          (the test binary runs in a redis:7-alpine container on the cluster network and has
//	                          redis-cli, but no docker access). See README for the exact procedure.
//
// Tolerance stated by the assertions: per tenant, admitted <= rate*elapsed + burst (Redis bucket) plus, where an
// event can reset state, one more burst (replica missing the last writes / new bucket on the promoted master) plus,
// in FailLocal mode, burst + rate*downtime from the per-instance fallback bucket.

type foNode struct{ name, addr string }

func foNodes(t *testing.T) []foNode {
	v := os.Getenv("QUERY_REDIS_IT_FAILOVER")
	if v == "" || os.Getenv("QUERY_REDIS_IT_CTL") == "" {
		t.Skip("QUERY_REDIS_IT_FAILOVER / QUERY_REDIS_IT_CTL not set")
	}
	var out []foNode
	for _, p := range strings.Split(v, ",") {
		kv := strings.SplitN(p, "=", 2)
		out = append(out, foNode{kv[0], kv[1]})
	}
	return out
}

func foCtl(t *testing.T, action string) {
	t.Helper()
	dir := os.Getenv("QUERY_REDIS_IT_CTL")
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(dir, id+".tmp"), []byte(action), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = os.Rename(filepath.Join(dir, id+".tmp"), filepath.Join(dir, id+".req"))
	for i := 0; i < 600; i++ {
		if _, err := os.Stat(filepath.Join(dir, id+".done")); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("ctl %q not acknowledged", action)
}

func foCli(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("redis-cli", args...).CombinedOutput()
	if err != nil {
		t.Logf("redis-cli %v: %v\n%s", args, err, out)
	}
	return string(out)
}

type foNodeInfo struct {
	id, addr, master string // master = id of the master when this is a replica
	isMaster         bool
	slots            []string
}

// foTopology asks live nodes (first that answers) for CLUSTER NODES.
func foTopology(t *testing.T, nodes []foNode, skip string) []foNodeInfo {
	for _, n := range nodes {
		if n.name == skip {
			continue
		}
		h, p, _ := splitAddr(n.addr)
		out, err := exec.Command("redis-cli", "-h", h, "-p", fmt.Sprint(p), "CLUSTER", "NODES").Output()
		if err != nil || len(out) == 0 {
			continue
		}
		var res []foNodeInfo
		for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			f := strings.Fields(ln)
			if len(f) < 8 || strings.Contains(f[2], "fail") || strings.Contains(f[2], "noaddr") {
				continue
			}
			ni := foNodeInfo{id: f[0], addr: strings.SplitN(f[1], "@", 2)[0], isMaster: strings.Contains(f[2], "master"), master: f[3]}
			if ni.isMaster {
				ni.slots = f[8:]
			}
			res = append(res, ni)
		}
		return res
	}
	t.Fatal("no node answered CLUSTER NODES")
	return nil
}

func foName(nodes []foNode, addr string) string {
	for _, n := range nodes {
		if n.addr == addr {
			return n.name
		}
	}
	return addr
}

type foLoad struct {
	mu                                    sync.Mutex
	admitted, limited, unavail, otherErrs map[string]int
	start                                 time.Time
	stop                                  chan struct{}
	wg                                    sync.WaitGroup
}

func startLoad(l *Limiter, tenants []string, every time.Duration) *foLoad {
	ld := &foLoad{admitted: map[string]int{}, limited: map[string]int{}, unavail: map[string]int{}, otherErrs: map[string]int{},
		start: time.Now(), stop: make(chan struct{})}
	for _, tn := range tenants {
		tn := tn
		ld.wg.Add(1)
		go func() {
			defer ld.wg.Done()
			tk := time.NewTicker(every)
			defer tk.Stop()
			for {
				select {
				case <-ld.stop:
					return
				case <-tk.C:
				}
				rel, _, err := l.Acquire(tn)
				ld.mu.Lock()
				switch {
				case err == nil:
					ld.admitted[tn]++
				case err.Code == "rate_limited":
					ld.limited[tn]++
				case err.Code == "limiter_unavailable":
					ld.unavail[tn]++
				default:
					ld.otherErrs[tn]++
				}
				ld.mu.Unlock()
				if err == nil {
					rel()
				}
			}
		}()
	}
	return ld
}

func (ld *foLoad) finish() time.Duration {
	close(ld.stop)
	ld.wg.Wait()
	return time.Since(ld.start)
}

// foProbe hits the counter directly (no limiter cooldown) and records the outage window of the Redis path.
type foProbe struct {
	fails       atomic.Int64
	first, last atomic.Int64 // unix nanos of first / last failed probe
	oks         atomic.Int64
	stop        chan struct{}
	done        chan struct{}
}

func startProbe(rc *RedisCounter, tenant string) *foProbe {
	p := &foProbe{stop: make(chan struct{}), done: make(chan struct{})}
	cfg := LimiterConfig{RatePerSec: 1e6, Burst: 1000000, MaxConcurrent: 1000000}
	go func() {
		defer close(p.done)
		for {
			select {
			case <-p.stop:
				return
			case <-time.After(50 * time.Millisecond):
			}
			if _, err := rc.Acquire(tenant, cfg); err != nil {
				now := time.Now().UnixNano()
				p.first.CompareAndSwap(0, now)
				p.last.Store(now)
				p.fails.Add(1)
			} else {
				p.oks.Add(1)
			}
		}
	}()
	return p
}

func (p *foProbe) outage() time.Duration {
	if p.first.Load() == 0 {
		return 0
	}
	return time.Duration(p.last.Load()-p.first.Load()) + 50*time.Millisecond
}
func (p *foProbe) end() { close(p.stop); <-p.done }

// foTenants returns n tenants whose slot is below maxSlot (when maxSlot>0) plus n spread ones.
func foTenants(prefix string, low, spread int, maxSlot uint16) []string {
	var out []string
	for i := 0; len(out) < low; i++ {
		tn := fmt.Sprintf("%s-low%d", prefix, i)
		if keySlot(rateKey(tn)) < maxSlot {
			out = append(out, tn)
		}
	}
	for i := 0; i < spread; i++ {
		out = append(out, fmt.Sprintf("%s-sp%d", prefix, i))
	}
	return out
}

func foNewLimiter(t *testing.T, nodes []foNode, mode FailureMode, cfg LimiterConfig) (*Limiter, *RedisCounter) {
	rc, err := NewRedisCounterOpts("redis://"+nodes[0].addr, RedisOptions{Nodes: addrs(nodes[1:])})
	if err != nil {
		t.Fatal(err)
	}
	rc.CmdTimeout, rc.DialTimeout = 500*time.Millisecond, 500*time.Millisecond
	l := NewLimiter(cfg)
	l.UseShared(rc, mode)
	return l, rc
}

func addrs(n []foNode) (o []string) {
	for _, x := range n {
		o = append(o, x.addr)
	}
	return
}

// foCheck asserts the per-tenant admission bound and returns the worst (admitted - bound) for logging.
func foCheck(t *testing.T, what string, ld *foLoad, tenants []string, cfg LimiterConfig, elapsed time.Duration, extraBursts int, extra float64) {
	t.Helper()
	bound := cfg.RatePerSec*elapsed.Seconds() + float64(cfg.Burst)*float64(1+extraBursts) + extra
	var tot, mx, un, lim int
	ld.mu.Lock()
	defer ld.mu.Unlock()
	for _, tn := range tenants {
		a := ld.admitted[tn]
		tot += a
		un += ld.unavail[tn]
		lim += ld.limited[tn]
		if a > mx {
			mx = a
		}
		if float64(a) > bound {
			t.Errorf("%s: tenant %s admitted %d > bound %.0f (rate %.0f/s * %.1fs + %d bursts of %d + %.0f)",
				what, tn, a, bound, cfg.RatePerSec, elapsed.Seconds(), 1+extraBursts, cfg.Burst, extra)
		}
		if ld.otherErrs[tn] > 0 {
			t.Errorf("%s: tenant %s saw %d unexpected errors", what, tn, ld.otherErrs[tn])
		}
	}
	t.Logf("%s: %d tenants, elapsed %.1fs, admitted total %d (max/tenant %d, bound/tenant %.0f), rate_limited %d, 503 limiter_unavailable %d",
		what, len(tenants), elapsed.Seconds(), tot, mx, bound, lim, un)
}

func TestLiveFailoverCluster(t *testing.T) {
	nodes := foNodes(t)
	cfg := LimiterConfig{RatePerSec: 20, Burst: 20, MaxConcurrent: 1000, SlotLease: 5 * time.Second}

	// ---------- (b) resharding while the limiter runs ----------
	t.Run("reshard", func(t *testing.T) {
		topo := foTopology(t, nodes, "")
		var masters []foNodeInfo
		for _, n := range topo {
			if n.isMaster {
				masters = append(masters, n)
			}
		}
		if len(masters) != 3 {
			t.Fatalf("want 3 masters, got %d", len(masters))
		}
		sort.Slice(masters, func(i, j int) bool { return masters[i].slots[0] < masters[j].slots[0] })
		src, dst := masters[0], masters[1] // src owns slot 0..: migrate its first 2000 slots
		tenants := foTenants("rs", 8, 8, 2000)
		for _, mode := range []FailureMode{FailClosed, FailLocal} {
			l, rc := foNewLimiter(t, nodes, mode, cfg)
			ld := startLoad(l, tenants, 25*time.Millisecond) // 40 req/s attempted per tenant vs rate 20
			time.Sleep(2 * time.Second)
			h, p, _ := splitAddr(dst.addr)
			from, to := src, dst
			if mode == FailLocal { // second round moves them back: both directions exercised
				from, to = dst, src
			}
			_ = h
			_ = p
			sh, sp, _ := splitAddr(masters[2].addr)
			args := []string{"--cluster", "reshard", fmt.Sprintf("%s:%d", sh, sp), "--cluster-from", from.id, "--cluster-to", to.id,
				"--cluster-slots", "2000", "--cluster-yes"}
			if mode == FailLocal {
				// slots 0..1999 now live on dst, but dst owns more; ask for the same count back
			}
			t0 := time.Now()
			out := foCli(t, args...)
			t.Logf("reshard %s -> %s (%v) took %.1fs", foName(nodes, from.addr), foName(nodes, to.addr), mode, time.Since(t0).Seconds())
			if !strings.Contains(out, "Moving slot") {
				t.Logf("reshard output tail: %.400s", out)
			}
			time.Sleep(2 * time.Second)
			el := ld.finish()
			f, r := l.SharedStats()
			t.Logf("mode %s: MOVED followed %d, ASK followed %d, fallbacks %d, rejected %d, topology refreshes %d", mode, rc.moved.Load(), rc.asked.Load(), f, r, rc.refreshes.Load())
			// A migrated key moves with its state (MIGRATE), so only a missing key on ASK can reset a bucket: allow 1 extra burst.
			foCheck(t, "reshard/"+string(mode), ld, tenants, cfg, el, 1, 0)
			if mode == FailClosed && r != 0 {
				t.Errorf("reshard must not cause 503s, got %d", r)
			}
			if rc.moved.Load() == 0 {
				t.Error("no MOVED seen while slots were moved")
			}
		}
	})

	// ---------- (a)+(c) master failure, promotion, rejoin ----------
	for _, mode := range []FailureMode{FailClosed, FailLocal} {
		mode := mode
		t.Run("failover/"+string(mode), func(t *testing.T) {
			topo := foTopology(t, nodes, "")
			byID := map[string]foNodeInfo{}
			for _, n := range topo {
				byID[n.id] = n
			}
			tenants := foTenants("fo"+string(mode), 0, 18, 0)
			l, rc := foNewLimiter(t, nodes, mode, cfg)
			// victim: the master that owns tenants[0]; make sure several tenants live on it
			victimAddr := ""
			// warm the slot map through MOVED/refresh, find owner via CLUSTER SLOTS-based refresh
			rc.minGap = 0
			rc.refresh()
			slot := keySlot(rateKey(tenants[0]))
			victimAddr = rc.owner(slot)
			victim := foName(nodes, victimAddr)
			var victimTenants, otherTenants []string
			for _, tn := range tenants {
				if rc.owner(keySlot(rateKey(tn))) == victimAddr {
					victimTenants = append(victimTenants, tn)
				} else {
					otherTenants = append(otherTenants, tn)
				}
			}
			rc.minGap = time.Second
			t.Logf("victim master %s (%s) owns %d of %d load tenants", victim, victimAddr, len(victimTenants), len(tenants))

			// a "killed holder": takes the only slot of a tenant on the victim and never renews or releases
			holderCfg := LimiterConfig{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 1, SlotLease: 10 * time.Second}
			var holderTenant string
			for i := 0; ; i++ {
				holderTenant = fmt.Sprintf("holder-%s-%d", mode, i)
				if rc.owner(keySlot(rateKey(holderTenant))) == victimAddr {
					break
				}
			}
			var holderAt time.Time

			probe := startProbe(rc, victimTenants[0]+"-probe")
			ld := startLoad(l, tenants, 25*time.Millisecond)
			time.Sleep(3 * time.Second)
			f0, r0 := l.SharedStats()
			if d, err := rc.Acquire(holderTenant, holderCfg); err != nil || !d.Admitted {
				t.Fatalf("holder: %v %+v", err, d)
			}
			holderAt = time.Now()
			time.Sleep(200 * time.Millisecond) // let the async replica receive the slot
			killAt := time.Now()
			foCtl(t, "stop "+victim)
			t.Logf("docker stop %s took %.1fs", victim, time.Since(killAt).Seconds())

			// wait until the Redis path recovered: 4s of consecutive successful probes after the first failure
			holderAdmit := func() (admitted bool, reached bool) {
				d, err := rc.Acquire(holderTenant, holderCfg)
				if err == nil && d.Admitted {
					_ = rc.Release(holderTenant, d.Slot)
				}
				return err == nil && d.Admitted, err == nil
			}
			var holderReached time.Time
			var holderAdmittedAtFirstReach bool
			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) {
				time.Sleep(200 * time.Millisecond)
				if holderReached.IsZero() {
					if adm, ok := holderAdmit(); ok {
						holderReached, holderAdmittedAtFirstReach = time.Now(), adm
					}
				}
				lastFail := probe.last.Load()
				if lastFail != 0 && time.Since(time.Unix(0, lastFail)) > 4*time.Second {
					break
				}
			}
			if probe.first.Load() == 0 {
				t.Fatal("probe never failed: was the master really killed?")
			}
			down := probe.outage()
			t.Logf("Redis path for the victim's slots was down %.1fs after docker stop (first failure +%.1fs), %d failed probes (50 ms apart)",
				down.Seconds(), time.Unix(0, probe.first.Load()).Sub(killAt).Seconds(), probe.fails.Load())

			// promoted?
			newTopo := foTopology(t, nodes, victim)
			promoted := ""
			for _, n := range newTopo {
				if o, ok := byID[n.id]; ok && n.isMaster && !o.isMaster {
					promoted = n.addr
				}
			}
			t.Logf("promoted replica: %s", foName(nodes, promoted))
			if promoted == "" {
				t.Fatal("no replica promoted")
			}
			if got := rc.owner(slot); got != promoted {
				t.Errorf("client routes slot %d to %s, promoted master is %s", slot, got, promoted)
			}

			// killed holder: when Redis is first reachable again after the failover, its slot must be gone iff the lease
			// has elapsed (before that, the async replica may or may not have the slot: both are logged, neither asserted).
			if holderReached.IsZero() {
				t.Fatal("holder tenant never reachable after failover")
			}
			age := holderReached.Sub(holderAt)
			t.Logf("killed holder: first reachable answer %.1fs after the slot was taken (lease %.0fs): tenant admitted=%v", age.Seconds(), holderCfg.SlotLease.Seconds(), holderAdmittedAtFirstReach)
			if age > holderCfg.SlotLease+500*time.Millisecond && !holderAdmittedAtFirstReach {
				t.Errorf("killed holder's slot still blocks the tenant %.1fs after its lease expired", (age - holderCfg.SlotLease).Seconds())
			}
			if !holderAdmittedAtFirstReach { // still inside the lease: it must free up within the lease
				time.Sleep(holderCfg.SlotLease)
				if adm, _ := holderAdmit(); !adm {
					t.Error("holder slot did not expire after a full lease on the promoted master")
				} else {
					t.Logf("killed holder: slot (replicated to the replica) expired on the promoted master within one lease")
				}
			}

			// (c) rejoin the old master: it must come back as a replica and never be routed to
			foCtl(t, "start "+victim)
			vh, vp, _ := splitAddr(victimAddr)
			rejoined := false
			for i := 0; i < 120 && !rejoined; i++ {
				time.Sleep(500 * time.Millisecond)
				out := foCli(t, "-h", vh, "-p", fmt.Sprint(vp), "ROLE")
				rejoined = strings.HasPrefix(strings.TrimSpace(out), "slave") && strings.Contains(out, "connected")
			}
			if !rejoined {
				t.Error("old master did not rejoin as a connected replica")
			}
			time.Sleep(4 * time.Second) // let the periodic/err-triggered refresh and the load run against the new layout
			before := foCli(t, "-h", vh, "-p", fmt.Sprint(vp), "INFO", "commandstats")
			time.Sleep(3 * time.Second)
			after := foCli(t, "-h", vh, "-p", fmt.Sprint(vp), "INFO", "commandstats")
			if strings.Contains(after, "cmdstat_evalsha") && before != after && strings.Contains(strings.SplitN(after, "cmdstat_evalsha", 2)[1][:60], "calls") {
				if grepCalls(before) != grepCalls(after) {
					t.Errorf("limiter commands reached the rejoined replica: %d -> %d EVALSHA calls", grepCalls(before), grepCalls(after))
				}
			}
			probe.end()
			el := ld.finish()
			f1, r1 := l.SharedStats()
			t.Logf("mode %s: during the whole event fallbacks=%d rejected(503)=%d (before kill: %d/%d), refreshes=%d, probe ok=%d fail=%d",
				mode, f1, r1, f0, r0, rc.refreshes.Load(), probe.oks.Load(), probe.fails.Load())
			if mode == FailClosed && r1 == r0 {
				t.Error("closed mode: expected 503s while the master was down")
			}
			if mode == FailLocal && r1 != 0 {
				t.Errorf("local mode returned %d 503s", r1)
			}
			if mode == FailLocal && f1 == f0 {
				t.Error("local mode: expected fallbacks while the master was down")
			}
			// Tolerance: Redis bucket (rate*T+burst) + replica lacking the last writes (+1 burst) + FailLocal per-instance
			// fallback bucket (burst + rate*down). closed mode never admits without Redis.
			extraBursts, extra := 1, 0.0
			if mode == FailLocal {
				extraBursts, extra = 2, cfg.RatePerSec*(down.Seconds()+float64(2*sharedCooldown/time.Second))
			}
			foCheck(t, "failover/"+string(mode), ld, tenants, cfg, el, extraBursts, extra)
			if len(otherTenants) > 0 {
				ld.mu.Lock()
				n503 := 0
				for _, tn := range otherTenants {
					n503 += ld.unavail[tn]
				}
				ld.mu.Unlock()
				t.Logf("tenants on surviving masters (%d): %d requests got 503 (limiter cooldown skips Redis for 2s after any failure, so they can be affected too)", len(otherTenants), n503)
			}
		})
	}
}

func grepCalls(s string) int {
	i := strings.Index(s, "cmdstat_evalsha:calls=")
	if i < 0 {
		return 0
	}
	var n int
	fmt.Sscanf(s[i+len("cmdstat_evalsha:calls="):], "%d", &n)
	return n
}

// TestLiveSentinelFailover: 1 master + 1 replica + 3 sentinels, SENTINEL failover while the limiter runs.
//
//	QUERY_REDIS_IT_SENTINELS=ip:port,ip:port,ip:port  QUERY_REDIS_IT_SENTINEL_MASTER=mymaster
func TestLiveSentinelFailover(t *testing.T) {
	sv := os.Getenv("QUERY_REDIS_IT_SENTINELS")
	if sv == "" {
		t.Skip("QUERY_REDIS_IT_SENTINELS not set")
	}
	sents := strings.Split(sv, ",")
	name := os.Getenv("QUERY_REDIS_IT_SENTINEL_MASTER")
	cfg := LimiterConfig{RatePerSec: 20, Burst: 20, MaxConcurrent: 1000, SlotLease: 5 * time.Second}
	for _, mode := range []FailureMode{FailClosed, FailLocal} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			if mode == FailLocal {
				time.Sleep(15 * time.Second) // Sentinel refuses a new failover within 2*failover-timeout (10s) of the last one
			}
			rc, err := NewRedisCounterOpts("redis://"+sents[0], RedisOptions{Sentinels: sents, MasterName: name})
			if err != nil {
				t.Fatal(err)
			}
			rc.CmdTimeout, rc.DialTimeout = 500*time.Millisecond, 500*time.Millisecond
			l := NewLimiter(cfg)
			l.UseShared(rc, mode)
			tenants := foTenants("sn"+string(mode), 0, 10, 0)
			probe := startProbe(rc, "sentinel-probe-"+string(mode))
			ld := startLoad(l, tenants, 25*time.Millisecond)
			time.Sleep(3 * time.Second)
			before := rc.owner(0)
			sh, sp, _ := splitAddr(sents[0])
			t0 := time.Now()
			out := foCli(t, "-h", sh, "-p", fmt.Sprint(sp), "SENTINEL", "failover", name)
			t.Logf("SENTINEL failover: %s", strings.TrimSpace(out))
			deadline := time.Now().Add(40 * time.Second)
			for time.Now().Before(deadline) {
				time.Sleep(200 * time.Millisecond)
				if o := rc.owner(0); o != "" && o != before && time.Since(t0) > time.Second {
					break
				}
			}
			after := rc.owner(0)
			t.Logf("master %s -> %s, client switched %.1fs after the failover command", before, after, time.Since(t0).Seconds())
			if after == before {
				t.Error("client did not follow the Sentinel failover")
			}
			time.Sleep(4 * time.Second)
			probe.end()
			el := ld.finish()
			f, r := l.SharedStats()
			t.Logf("mode %s: Redis path outage %.1fs (%d failed probes), fallbacks=%d rejected=%d refreshes=%d", mode, probe.outage().Seconds(), probe.fails.Load(), f, r, rc.refreshes.Load())
			extraBursts, extra := 1, 0.0
			if mode == FailLocal {
				extraBursts, extra = 2, cfg.RatePerSec*(probe.outage().Seconds()+4)
			}
			foCheck(t, "sentinel/"+string(mode), ld, tenants, cfg, el, extraBursts, extra)
		})
	}
}
