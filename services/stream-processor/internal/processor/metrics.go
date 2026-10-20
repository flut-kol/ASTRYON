package processor

import (
	"expvar"
	"sync/atomic"
)

// Metrics are exposed via expvar (/debug/vars) like the other services.
type Metrics struct {
	In, Out, Duplicates, Quarantined, DLQ, Batches, Retries atomic.Int64
	win                                                     *Window
}

func NewMetrics(w *Window) *Metrics { return &Metrics{win: w} }

func (m *Metrics) observe(b Batch) {
	m.Batches.Add(1)
	m.In.Add(int64(len(b.Results)))
	m.Out.Add(int64(b.Out))
	m.Duplicates.Add(int64(b.Duplicates))
	m.Quarantined.Add(int64(b.Quarantine))
	m.DLQ.Add(int64(b.DLQ))
}

// Publish registers the metrics under "streamproc" in expvar (call once).
func (m *Metrics) Publish() {
	expvar.Publish("streamproc", expvar.Func(func() any {
		s := m.win.Stats()
		return map[string]any{
			"in": m.In.Load(), "out": m.Out.Load(), "duplicates": m.Duplicates.Load(),
			"quarantined": m.Quarantined.Load(), "dlq": m.DLQ.Load(), "batches": m.Batches.Load(),
			"retries":                  m.Retries.Load(),
			"dedup_state_entries":      s.Entries,
			"dedup_state_bytes_approx": s.ApproxBytes,
			"dedup_evicted_ttl":        s.EvictedTTL,
			"dedup_evicted_cap":        s.EvictedCap,
			"dedup_max_entries":        s.MaxEntries,
			"dedup_ttl_seconds":        s.TTLSeconds,
		}
	}))
}
