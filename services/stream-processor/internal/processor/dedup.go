package processor

import (
	"container/list"
	"sync"
	"time"
)

// entryOverhead approximates per-entry memory beyond the key bytes
// (list element + entry struct + map slot + string header).
const entryOverhead = 112

// Window is a bounded dedup state (DPP-027, plan §5.3): a time-ordered FIFO of
// keys bounded by both a TTL and a maximum entry count. A key is "seen" if it was
// added less than TTL ago and has not been evicted. Hits do NOT refresh a key, so
// insertion order equals expiry order and eviction is O(1) from the oldest end.
// State can therefore never exceed MaxEntries, regardless of input cardinality.
type Window struct {
	mu    sync.Mutex
	ttl   time.Duration
	max   int
	ll    *list.List // front = oldest
	idx   map[string]*list.Element
	bytes int64

	evictedTTL uint64
	evictedCap uint64
}

type entry struct {
	key string
	at  time.Time
}

// NewWindow creates a window. ttl<=0 or max<=0 are rejected by Config.Validate;
// here they are clamped to 1 to keep the invariant (bounded) unconditional.
func NewWindow(ttl time.Duration, maxEntries int) *Window {
	if ttl <= 0 {
		ttl = time.Nanosecond
	}
	if maxEntries <= 0 {
		maxEntries = 1
	}
	return &Window{ttl: ttl, max: maxEntries, ll: list.New(), idx: make(map[string]*list.Element)}
}

// Seen reports whether key is inside the window at time now. It does not mutate state.
func (w *Window) Seen(key string, now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	el, ok := w.idx[key]
	if !ok {
		return false
	}
	return now.Sub(el.Value.(*entry).at) < w.ttl
}

// Add records key at time now (first insertion wins; a live key is not refreshed),
// expiring old entries and enforcing the size cap.
func (w *Window) Add(key string, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.expireLocked(now)
	if el, ok := w.idx[key]; ok {
		if now.Sub(el.Value.(*entry).at) < w.ttl {
			return
		}
		w.removeLocked(el)
	}
	w.idx[key] = w.ll.PushBack(&entry{key: key, at: now})
	w.bytes += int64(len(key)) + entryOverhead
	for w.ll.Len() > w.max {
		w.removeLocked(w.ll.Front())
		w.evictedCap++
	}
}

func (w *Window) expireLocked(now time.Time) {
	for el := w.ll.Front(); el != nil; el = w.ll.Front() {
		if now.Sub(el.Value.(*entry).at) < w.ttl {
			return
		}
		w.removeLocked(el)
		w.evictedTTL++
	}
}

func (w *Window) removeLocked(el *list.Element) {
	e := w.ll.Remove(el).(*entry)
	delete(w.idx, e.key)
	w.bytes -= int64(len(e.key)) + entryOverhead
}

// Expire drops expired entries now (call periodically so idle state shrinks).
func (w *Window) Expire(now time.Time) {
	w.mu.Lock()
	w.expireLocked(now)
	w.mu.Unlock()
}

// Stats is a point-in-time snapshot of window state.
type Stats struct {
	Entries     int
	ApproxBytes int64
	EvictedTTL  uint64
	EvictedCap  uint64
	MaxEntries  int
	TTLSeconds  float64
}

// Stats returns the current state-size gauges and eviction counters.
func (w *Window) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Stats{Entries: w.ll.Len(), ApproxBytes: w.bytes, EvictedTTL: w.evictedTTL,
		EvictedCap: w.evictedCap, MaxEntries: w.max, TTLSeconds: w.ttl.Seconds()}
}
