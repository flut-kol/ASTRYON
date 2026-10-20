package retry

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Metrics exposes bounded, tenant-free controller counters in Prometheus text format.
type Metrics struct{ polls, released, errors atomic.Int64 }

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
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "astryon_retry_polls_total %d\nastryon_retry_released_total %d\nastryon_retry_errors_total %d\n", m.polls.Load(), m.released.Load(), m.errors.Load())
	})
}
