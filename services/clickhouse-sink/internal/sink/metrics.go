package sink

import (
	"expvar"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
)

// Metrics are plain atomic counters, published through expvar.
type Metrics struct {
	Consumed, Inserted, Quarantined, Batches, InsertRetries, InsertFailures, Commits, CommitFailures atomic.Int64
	Buffered                                                                                         atomic.Int64
}

// Publish registers the counters under expvar name "clickhouse_sink".
func (m *Metrics) Publish() {
	expvar.Publish("clickhouse_sink", expvar.Func(func() any {
		return map[string]int64{
			"consumed": m.Consumed.Load(), "inserted_rows": m.Inserted.Load(), "quarantined": m.Quarantined.Load(),
			"batches": m.Batches.Load(), "insert_retries": m.InsertRetries.Load(), "insert_failures": m.InsertFailures.Load(),
			"commits": m.Commits.Load(), "commit_failures": m.CommitFailures.Load(), "buffered": m.Buffered.Load(),
		}
	}))
}

// WriteProm writes Prometheus text exposition (v0.0.4). Names match infra/observability alerts and dashboards.
func (m *Metrics) WriteProm(w io.Writer) {
	counters := []struct {
		name, help string
		v          int64
	}{
		{"consumed", "Events consumed from Kafka.", m.Consumed.Load()},
		{"inserted_rows", "Rows inserted into ClickHouse.", m.Inserted.Load()},
		{"quarantined", "Events sent to the quarantine topic.", m.Quarantined.Load()},
		{"batches", "Batches flushed.", m.Batches.Load()},
		{"insert_retries", "ClickHouse insert retries.", m.InsertRetries.Load()},
		{"insert_failures", "ClickHouse inserts that failed after retries.", m.InsertFailures.Load()},
		{"commits", "Kafka offset commits.", m.Commits.Load()},
		{"commit_failures", "Kafka offset commit failures.", m.CommitFailures.Load()},
	}
	for _, c := range counters {
		n := "astryon_clickhouse_sink_" + c.name + "_total"
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", n, c.help, n, n, c.v)
	}
	const g = "astryon_clickhouse_sink_buffered"
	fmt.Fprintf(w, "# HELP %s Events buffered awaiting flush.\n# TYPE %s gauge\n%s %d\n", g, g, g, m.Buffered.Load())
}

// Handler serves GET /metrics (Prometheus text format).
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.WriteProm(w)
	})
}

// Mux returns the observability mux: /metrics, /debug/vars, /healthz (liveness) and /readyz (200 once ready is true).
func (m *Metrics) Mux(ready *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready != nil && ready.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	return mux
}
