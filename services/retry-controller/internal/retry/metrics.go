package retry

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// BacklogReader returns tenant-agnostic aggregates of the retry schedule.
type BacklogReader interface {
	Backlog(ctx context.Context) (Backlog, error)
}

// Backlog summarizes scheduled-but-unreleased retries.
type Backlog struct {
	Pending       int64   // scheduled, not fired, not cancelled
	OldestDueSecs float64 // age past retry_at of the oldest due retry; 0 if none due
}

// Metrics exposes bounded, tenant-free controller counters in Prometheus text format.
type Metrics struct {
	polls, released, errors atomic.Int64
	// Backlog, when set, is read on each scrape (bounded by a short timeout).
	// If the read fails the backlog gauges are omitted for that scrape and the
	// backlog read error counter is incremented.
	Backlog       BacklogReader
	backlogErrors atomic.Int64
}

func (m *Metrics) Observe(released int64, err error) {
	m.polls.Add(1)
	if released > 0 {
		m.released.Add(released)
	}
	if err != nil {
		m.errors.Add(1)
	}
}
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "astryon_retry_polls_total %d\nastryon_retry_released_total %d\nastryon_retry_errors_total %d\n", m.polls.Load(), m.released.Load(), m.errors.Load())
		if m.Backlog == nil {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		b, err := m.Backlog.Backlog(ctx)
		if err != nil {
			m.backlogErrors.Add(1)
		} else {
			fmt.Fprintf(w, "astryon_retry_backlog_pending %d\nastryon_retry_oldest_due_age_seconds %g\n", b.Pending, b.OldestDueSecs)
		}
		fmt.Fprintf(w, "astryon_retry_backlog_read_errors_total %d\n", m.backlogErrors.Load())
	})
}
