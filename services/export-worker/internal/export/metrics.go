package export

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

// Metrics are process-wide, low-cardinality counters rendered as Prometheus text (no tenant or export id labels).
// Failed exports are labelled by errCode, a bounded set; queue errors by a fixed operation name.
type metrics struct {
	started, succeeded, rows, bytes atomic.Int64
	mu                              sync.Mutex
	failed                          map[string]int64
	queueErr                        map[string]int64
	buckets                         [9]int64 // last = +Inf, non-cumulative
	sumSeconds                      float64
	count                           int64
	auditStats                      func() AuditWriterStats
}

// AuditWriterStats mirrors the library's monotonic writer counters without
// exposing tenant or resource identifiers as metric labels.
type AuditWriterStats struct{ Queued, Written, Dropped, Rejected uint64 }

var durationBuckets = [8]float64{1, 5, 15, 60, 300, 900, 1800, 3600}

// Metrics is the singleton behind WritePrometheus.
var Metrics = &metrics{failed: map[string]int64{}, queueErr: map[string]int64{}}

func (m *metrics) exportDone(code string, rows, bytes int64, seconds float64) {
	if code == "" {
		m.succeeded.Add(1)
	}
	m.rows.Add(rows)
	m.bytes.Add(bytes)
	m.mu.Lock()
	defer m.mu.Unlock()
	if code != "" {
		m.failed[code]++
	}
	i := sort.SearchFloat64s(durationBuckets[:], seconds)
	m.buckets[i]++
	m.sumSeconds += seconds
	m.count++
}

// queueError counts a failed queue operation; op is one of claim, start, finish, heartbeat, recover.
func (m *metrics) queueError(op string) {
	m.mu.Lock()
	m.queueErr[op]++
	m.mu.Unlock()
}

// SetAuditStatsProvider connects a PG audit writer to the existing metrics endpoint.
func (m *metrics) SetAuditStatsProvider(fn func() AuditWriterStats) {
	m.mu.Lock()
	m.auditStats = fn
	m.mu.Unlock()
}

// WritePrometheus renders the metrics in Prometheus text format 0.0.4.
func (m *metrics) WritePrometheus(w io.Writer) {
	fmt.Fprintf(w, "# TYPE astryon_export_started_total counter\nastryon_export_started_total %d\n", m.started.Load())
	fmt.Fprintf(w, "# TYPE astryon_export_succeeded_total counter\nastryon_export_succeeded_total %d\n", m.succeeded.Load())
	fmt.Fprintf(w, "# TYPE astryon_export_rows_total counter\nastryon_export_rows_total %d\n", m.rows.Load())
	fmt.Fprintf(w, "# TYPE astryon_export_bytes_total counter\nastryon_export_bytes_total %d\n", m.bytes.Load())
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.auditStats != nil {
		s := m.auditStats()
		fmt.Fprint(w, "# TYPE astryon_audit_queued_total counter\n")
		fmt.Fprintf(w, "astryon_audit_queued_total %d\n", s.Queued)
		fmt.Fprint(w, "# TYPE astryon_audit_written_total counter\n")
		fmt.Fprintf(w, "astryon_audit_written_total %d\n", s.Written)
		fmt.Fprint(w, "# TYPE astryon_audit_dropped_total counter\n")
		fmt.Fprintf(w, "astryon_audit_dropped_total %d\n", s.Dropped)
		fmt.Fprint(w, "# TYPE astryon_audit_rejected_total counter\n")
		fmt.Fprintf(w, "astryon_audit_rejected_total %d\n", s.Rejected)
	}
	fmt.Fprint(w, "# TYPE astryon_export_failed_total counter\n")
	writeLabelled(w, "astryon_export_failed_total", "code", m.failed)
	fmt.Fprint(w, "# TYPE astryon_export_queue_errors_total counter\n")
	writeLabelled(w, "astryon_export_queue_errors_total", "op", m.queueErr)
	fmt.Fprint(w, "# TYPE astryon_export_duration_seconds histogram\n")
	var cum int64
	for i, le := range durationBuckets {
		cum += m.buckets[i]
		fmt.Fprintf(w, "astryon_export_duration_seconds_bucket{le=\"%g\"} %d\n", le, cum)
	}
	cum += m.buckets[len(durationBuckets)]
	fmt.Fprintf(w, "astryon_export_duration_seconds_bucket{le=\"+Inf\"} %d\n", cum)
	fmt.Fprintf(w, "astryon_export_duration_seconds_sum %g\nastryon_export_duration_seconds_count %d\n", m.sumSeconds, m.count)
}

func writeLabelled(w io.Writer, name, label string, vals map[string]int64) {
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s{%s=%q} %d\n", name, label, k, vals[k])
	}
}
