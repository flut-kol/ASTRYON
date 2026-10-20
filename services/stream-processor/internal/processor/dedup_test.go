package processor

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestWindowTTL(t *testing.T) {
	w := NewWindow(time.Minute, 100)
	w.Add("a", t0)
	if !w.Seen("a", t0.Add(59*time.Second)) {
		t.Fatal("inside ttl must be seen")
	}
	if w.Seen("a", t0.Add(time.Minute)) {
		t.Fatal("at ttl must be expired")
	}
	w.Add("b", t0.Add(2*time.Minute)) // expires a
	if s := w.Stats(); s.Entries != 1 || s.EvictedTTL != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestWindowNoRefreshOnHit(t *testing.T) {
	w := NewWindow(time.Minute, 100)
	w.Add("a", t0)
	w.Add("a", t0.Add(50*time.Second)) // live: ignored
	if w.Seen("a", t0.Add(61*time.Second)) {
		t.Fatal("re-add must not extend the window")
	}
	w.Add("a", t0.Add(61*time.Second)) // expired: re-added
	if !w.Seen("a", t0.Add(62*time.Second)) || w.Stats().Entries != 1 {
		t.Fatal("expired key must be re-addable")
	}
}

func TestWindowCapEvictsOldest(t *testing.T) {
	w := NewWindow(time.Hour, 3)
	for i := 0; i < 5; i++ {
		w.Add(fmt.Sprint(i), t0.Add(time.Duration(i)*time.Second))
	}
	now := t0.Add(10 * time.Second)
	if w.Seen("0", now) || w.Seen("1", now) || !w.Seen("2", now) || !w.Seen("4", now) {
		t.Fatal("oldest must be evicted first")
	}
	if s := w.Stats(); s.Entries != 3 || s.EvictedCap != 2 {
		t.Fatalf("stats %+v", s)
	}
}

// DPP-027: state growth is bounded by min(max_entries, rate*ttl) regardless of
// how many distinct keys flow through.
func TestStateGrowthBounded(t *testing.T) {
	const max = 10_000
	w := NewWindow(time.Hour, max) // cap-bound regime
	peak := 0
	for i := 0; i < 200_000; i++ {
		w.Add(fmt.Sprintf("t/s/%08d/v1", i), t0.Add(time.Duration(i)*time.Millisecond))
		if n := w.Stats().Entries; n > peak {
			peak = n
		}
	}
	if peak > max {
		t.Fatalf("peak %d exceeds cap %d", peak, max)
	}
	s := w.Stats()
	if s.Entries != max || s.EvictedCap != 200_000-max {
		t.Fatalf("stats %+v", s)
	}
	// TTL-bound regime: 1000 keys/s, ttl 10s => ~10_000 live entries.
	w2 := NewWindow(10*time.Second, 1_000_000)
	for i := 0; i < 100_000; i++ {
		w2.Add(fmt.Sprintf("k%d", i), t0.Add(time.Duration(i)*time.Millisecond))
	}
	if n := w2.Stats().Entries; n < 9_000 || n > 10_001 {
		t.Fatalf("ttl-bound entries=%d, want ~10000", n)
	}
	// Idle: everything expires.
	w2.Expire(t0.Add(time.Hour))
	if s := w2.Stats(); s.Entries != 0 || s.ApproxBytes != 0 {
		t.Fatalf("idle state not released: %+v", s)
	}
}

// Real heap growth stays flat once the cap is reached.
func TestHeapFlatAtCap(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	w := NewWindow(time.Hour, 50_000)
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	fill := func(from, to int) {
		for i := from; i < to; i++ {
			w.Add(fmt.Sprintf("00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002/%032d/v1", i), t0.Add(time.Duration(i)*time.Millisecond))
		}
	}
	fill(0, 60_000)
	h1 := heap()
	fill(60_000, 400_000)
	h2 := heap()
	if h2 > h1+h1/2+(2<<20) {
		t.Fatalf("heap grew past cap: %d -> %d", h1, h2)
	}
	t.Logf("heap at cap: %d -> %d bytes; approx accounted %d", h1, h2, w.Stats().ApproxBytes)
}

func BenchmarkWindowAdd(b *testing.B) {
	for _, max := range []int{10_000, 1_000_000} {
		b.Run(fmt.Sprintf("max=%d", max), func(b *testing.B) {
			w := NewWindow(time.Hour, max)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				k := fmt.Sprintf("t/s/%d/v1", i)
				now := t0.Add(time.Duration(i) * time.Microsecond)
				if !w.Seen(k, now) {
					w.Add(k, now)
				}
			}
			s := w.Stats()
			b.ReportMetric(float64(s.Entries), "entries")
			b.ReportMetric(float64(s.ApproxBytes), "approx-bytes")
		})
	}
}
