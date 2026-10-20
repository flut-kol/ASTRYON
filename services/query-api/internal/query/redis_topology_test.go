package query

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// slotsReply builds a CLUSTER SLOTS reply: each entry {start,end,host(""=null),port}, plus a replica per range that
// must never be routed to.
func slotsReply(e ...[4]any) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(e))
	for _, x := range e {
		fmt.Fprintf(&b, "*4\r\n:%d\r\n:%d\r\n", x[0], x[1])
		for i, node := range []string{"master", "replica"} {
			host := x[2].(string)
			if i == 1 {
				host = "10.99.99.99"
			}
			fmt.Fprintf(&b, "*4\r\n")
			if host == "" {
				b.WriteString("$-1\r\n")
			} else {
				fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(host), host)
			}
			fmt.Fprintf(&b, ":%d\r\n$3\r\nid%s\r\n*0\r\n", x[3].(int), node[:1])
		}
	}
	return b.String()
}

func TestParseClusterSlotsQuirks(t *testing.T) {
	raw := slotsReply([4]any{0, 5460, "10.0.0.1", 7000}, [4]any{5461, 10922, "", 7001}, [4]any{10923, 16383, "redis-3.internal", 7002})
	rc, _ := NewRedisCounter("redis://127.0.0.1:1")
	f := newFakeRedis(t)
	f.slotsRaw = raw
	v, err := rc.doAt(f.addr(), false, "CLUSTER", "SLOTS")
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseClusterSlots(v, "192.168.5.5:7001")
	if err != nil || len(got) != 3 {
		t.Fatalf("%v %+v", err, got)
	}
	want := []string{"10.0.0.1:7000", "192.168.5.5:7001", "redis-3.internal:7002"} // null host -> asked node's host
	for i, g := range got {
		if g.addr != want[i] {
			t.Fatalf("range %d addr %q want %q", i, g.addr, want[i])
		}
		if strings.Contains(g.addr, "10.99.99.99") {
			t.Fatal("replica routed")
		}
	}
	if _, err := parseClusterSlots("x", "a:1"); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestRefreshInstallsMapRoutesToMasterOnly(t *testing.T) {
	a, b := newFakeRedis(t), newFakeRedis(t)
	_, pa, _ := splitAddr(a.addr())
	_, pb, _ := splitAddr(b.addr())
	slot := keySlot(rateKey(tenant))
	other := (slot + 1) % 16384
	lo, hi := slot, other
	if lo > hi {
		lo, hi = hi, lo
	}
	_ = lo
	// everything -> b except nothing; a is the seed and answers CLUSTER SLOTS saying b owns all slots
	a.slotsRaw = slotsReply([4]any{0, 16383, "127.0.0.1", pb})
	_ = pa
	rc, _ := NewRedisCounter("redis://" + a.addr())
	rc.CmdTimeout, rc.DialTimeout = time.Second, time.Second
	rc.minGap = 0
	if !rc.refresh() {
		t.Fatal("refresh failed")
	}
	if got := rc.owner(slot); got != b.addr() {
		t.Fatalf("owner %q want %q", got, b.addr())
	}
	cfg := LimiterConfig{RatePerSec: 1, Burst: 3, MaxConcurrent: 5}
	if d, err := rc.Acquire(tenant, cfg); err != nil || !d.Admitted {
		t.Fatalf("%v %+v", err, d)
	}
	if a.keyed.Load() != 0 || b.keyed.Load() == 0 {
		t.Fatalf("keyed a=%d b=%d: must go straight to the master from the map", a.keyed.Load(), b.keyed.Load())
	}
}

func splitAddr(a string) (string, int, error) {
	i := strings.LastIndexByte(a, ':')
	var p int
	_, err := fmt.Sscanf(a[i+1:], "%d", &p)
	return a[:i], p, err
}

func TestRefreshRateLimitedAndStandaloneStops(t *testing.T) {
	f := newFakeRedis(t) // answers "cluster support disabled"
	rc, _ := NewRedisCounter("redis://" + f.addr())
	rc.minGap = time.Hour
	rc.lastRefresh = time.Time{}
	rc.refresh()
	rc.refresh()
	rc.refresh()
	if n := f.slotsReq.Load(); n != 1 {
		t.Fatalf("refresh not rate limited / standalone not detected: %d requests", n)
	}
	rc.minGap = 0
	rc.refresh()
	if n := f.slotsReq.Load(); n != 1 {
		t.Fatalf("standalone must stop refreshing, got %d requests", n)
	}
}

func TestDialErrorTriggersRefreshAndReroutes(t *testing.T) {
	dead, live := newFakeRedis(t), newFakeRedis(t)
	seed := newFakeRedis(t)
	_, pl, _ := splitAddr(live.addr())
	seed.slotsRaw = slotsReply([4]any{0, 16383, "127.0.0.1", pl})
	deadAddr := dead.addr()
	dead.stop()
	rc, _ := NewRedisCounter("redis://" + seed.addr())
	rc.CmdTimeout, rc.DialTimeout, rc.minGap = time.Second, time.Second, 0
	rc.slotAddr[keySlot(rateKey(tenant))] = deadAddr // stale MOVED cache pointing at the failed master
	d, err := rc.Acquire(tenant, LimiterConfig{RatePerSec: 1, Burst: 2, MaxConcurrent: 2})
	if err != nil || !d.Admitted {
		t.Fatalf("%v %+v", err, d)
	}
	if live.keyed.Load() == 0 {
		t.Fatal("request did not land on the promoted master")
	}
}

func TestMovedStormForcesRefreshAndPeriodicTick(t *testing.T) {
	a := newFakeRedis(t)
	a.slotsRaw = slotsReply([4]any{0, 16383, "127.0.0.1", 1})
	rc, _ := NewRedisCounter("redis://" + a.addr())
	rc.minGap = 0
	for i := 0; i < movedStorm; i++ {
		rc.noteMoved()
	}
	if a.slotsReq.Load() != 1 {
		t.Fatalf("MOVED storm did not refresh: %d", a.slotsReq.Load())
	}
	rc.refreshEvery = 10 * time.Millisecond
	time.Sleep(30 * time.Millisecond)
	rc.topologyTick()
	waitFor(t, "periodic refresh", 2*time.Second, func() bool { return a.slotsReq.Load() == 2 })
	rc.refreshEvery = 0 // off
	time.Sleep(20 * time.Millisecond)
	rc.topologyTick()
	time.Sleep(50 * time.Millisecond)
	if a.slotsReq.Load() != 2 {
		t.Fatal("periodic refresh ran although disabled")
	}
}

func TestSentinelResolvesAndReresolvesOnReadonly(t *testing.T) {
	m1, m2, sen := newFakeRedis(t), newFakeRedis(t), newFakeRedis(t)
	_ = sen
	// a tiny sentinel: answered through the CLUSTER hook is not possible, so use a dedicated listener
	s := newSentinelFake(t, m1.addr())
	m1.hook = nil
	rc, err := NewRedisCounterOpts("redis://127.0.0.1:1", RedisOptions{Sentinels: []string{s.addr()}, MasterName: "mymaster"})
	if err != nil {
		t.Fatal(err)
	}
	rc.CmdTimeout, rc.DialTimeout, rc.minGap = time.Second, time.Second, 0
	cfg := LimiterConfig{RatePerSec: 1, Burst: 5, MaxConcurrent: 5}
	if d, err := rc.Acquire(tenant, cfg); err != nil || !d.Admitted || m1.keyed.Load() == 0 {
		t.Fatalf("%v %+v keyed=%d", err, d, m1.keyed.Load())
	}
	// failover: m1 becomes a replica (rejects writes), sentinel now points at m2
	m1.hook = func([]string, bool) string { return "-READONLY You can't write against a read only replica." }
	s.set(m2.addr())
	if d, err := rc.Acquire(tenant, cfg); err != nil || !d.Admitted || m2.keyed.Load() == 0 {
		t.Fatalf("after failover: %v %+v m2=%d", err, d, m2.keyed.Load())
	}
	if _, err := NewRedisCounterOpts("redis://x:1", RedisOptions{Sentinels: []string{"a:1"}}); err == nil {
		t.Fatal("sentinels without master name accepted")
	}
}

// sentinelFake answers SENTINEL get-master-addr-by-name with a settable master.
type sentinelFake struct {
	*fakeRedis
}

func newSentinelFake(t *testing.T, master string) *sentinelFake {
	f := newFakeRedis(t)
	s := &sentinelFake{f}
	s.set(master)
	return s
}

func (s *sentinelFake) set(master string) {
	h, p, _ := splitAddr(master)
	s.fakeRedis.mu.Lock()
	s.fakeRedis.sentinelRaw = fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n", len(h), h, len(fmt.Sprint(p)), p)
	s.fakeRedis.mu.Unlock()
}
