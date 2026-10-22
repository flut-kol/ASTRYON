package worker

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Failure codes (bounded; the only label of replays_failed_total).
const (
	FailPermanent  = "permanent"   // malformed job / invalid source data, recorded FAILED, not redelivered
	FailBaseRead   = "base_read"   // ClickHouse base version / page / existing check
	FailPublish    = "publish"     // Kafka publish to processed.events.v1
	FailStatus     = "status"      // PostgreSQL status update (RUNNING claim / COMPLETED)
	FailLease      = "lease_busy"  // RUNNING within its lease: not started, redelivered later
	FailInvalidJob = "invalid_job" // job failed validation
	FailOther      = "other"
)

var failCodes = []string{FailPermanent, FailBaseRead, FailPublish, FailStatus, FailLease, FailInvalidJob, FailOther}

// durationBuckets are upper bounds in seconds for replay_duration_seconds.
var durationBuckets = []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 7200}

// Metrics: counters, an active gauge and one histogram; Prometheus text exposition, hand-rolled like the sibling
// services. No tenant/source/job labels, ever. A nil *Metrics is a no-op.
type Metrics struct {
	Requested, Approved, Started, Completed, EventsReplayed, PublishErrors atomic.Int64
	Active                                                                 atomic.Int64

	mu      sync.Mutex
	failed  map[string]int64
	buckets []int64 // cumulative-at-write: count per bucket (non-cumulative), last = +Inf
	sum     float64
	count   int64
}

// Failed counts a job that ended FAILED (or could not start) under a bounded code.
func (m *Metrics) Fail(code string) {
	if m == nil {
		return
	}
	ok := false
	for _, c := range failCodes {
		ok = ok || c == code
	}
	if !ok {
		code = FailOther
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failed == nil {
		m.failed = map[string]int64{}
	}
	m.failed[code]++
}

// ObserveDuration records the wall time of one executed replay (completed or failed).
func (m *Metrics) ObserveDuration(d time.Duration) {
	if m == nil {
		return
	}
	s := d.Seconds()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.buckets == nil {
		m.buckets = make([]int64, len(durationBuckets)+1)
	}
	i := 0
	for i < len(durationBuckets) && s > durationBuckets[i] {
		i++
	}
	m.buckets[i]++
	m.sum += s
	m.count++
}

// discard absorbs the counts of a Worker without Metrics.
var discard Metrics

func (m *Metrics) add(c *atomic.Int64, n int64) { c.Add(n) }

// stageError tags an Execute error with its failure code without changing the message.
type stageError struct {
	code string
	err  error
}

func (e *stageError) Error() string { return e.err.Error() }
func (e *stageError) Unwrap() error { return e.err }

func stage(code string, err error) error { return &stageError{code: code, err: err} }

// FailureCode maps an error to its bounded code.
func FailureCode(err error) string {
	var se *stageError
	switch {
	case errors.Is(err, ErrRunningRecently):
		return FailLease
	case errors.Is(err, ErrPermanent):
		return FailPermanent
	case errors.As(err, &se):
		return se.code
	}
	return FailOther
}

func (m *Metrics) WritePrometheus(w io.Writer) {
	const p = "astryon_replay_worker_"
	if m == nil {
		return
	}
	for _, c := range []struct {
		name, help string
		v          int64
	}{
		{"replays_requested_total", "Replay jobs consumed that passed validation", m.Requested.Load()},
		{"replays_approved_total", "Replay jobs whose request row was claimed RUNNING (or any valid job when status tracking is off)", m.Approved.Load()},
		{"replays_started_total", "Replay executions started", m.Started.Load()},
		{"replays_completed_total", "Replay executions completed", m.Completed.Load()},
		{"events_replayed_total", "Envelopes published to processed.events.v1 by replays", m.EventsReplayed.Load()},
		{"publish_errors_total", "Failed publishes of replayed events", m.PublishErrors.Load()},
	} {
		fmt.Fprintf(w, "# HELP %s%s %s\n# TYPE %s%s counter\n%s%s %d\n", p, c.name, c.help, p, c.name, p, c.name, c.v)
	}
	fmt.Fprintf(w, "# HELP %sactive_replays Replays executing now\n# TYPE %sactive_replays gauge\n%sactive_replays %d\n", p, p, p, m.Active.Load())
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Fprintf(w, "# HELP %sreplays_failed_total Replays that failed or could not start, by bounded code\n# TYPE %sreplays_failed_total counter\n", p, p)
	for _, c := range failCodes {
		fmt.Fprintf(w, "%sreplays_failed_total{code=%q} %d\n", p, c, m.failed[c])
	}
	fmt.Fprintf(w, "# HELP %sreplay_duration_seconds Wall time of executed replays\n# TYPE %sreplay_duration_seconds histogram\n", p, p)
	var cum int64
	for i, ub := range durationBuckets {
		if m.buckets != nil {
			cum += m.buckets[i]
		}
		fmt.Fprintf(w, "%sreplay_duration_seconds_bucket{le=\"%g\"} %d\n", p, ub, cum)
	}
	fmt.Fprintf(w, "%sreplay_duration_seconds_bucket{le=\"+Inf\"} %d\n%sreplay_duration_seconds_sum %g\n%sreplay_duration_seconds_count %d\n", p, m.count, p, m.sum, p, m.count)
}

// MetricsListenAddr maps the env value to a listen address: empty = off (ok=false); a bare ":port" binds loopback only,
// an explicit host (e.g. a pod IP or 0.0.0.0) is honoured.
func MetricsListenAddr(v string) (addr string, ok bool) {
	if v == "" {
		return "", false
	}
	host, port, err := net.SplitHostPort(v)
	if err != nil {
		return v, true // let ListenAndServe report it
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), true
}

// MetricsHandler serves GET /metrics.
func MetricsHandler(write func(io.Writer)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		write(w)
	})
	return mux
}

// ServeMetrics starts the opt-in listener (nothing when env is empty); returns nil when off.
func ServeMetrics(env string, write func(io.Writer), logf func(string, ...any)) *http.Server {
	addr, ok := MetricsListenAddr(env)
	if !ok {
		return nil
	}
	srv := &http.Server{Addr: addr, Handler: MetricsHandler(write), ReadHeaderTimeout: 5 * time.Second}
	go func() { logf("metrics: %v", srv.ListenAndServe()) }()
	return srv
}
