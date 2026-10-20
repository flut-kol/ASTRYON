package gateway

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

// ackBuckets are histogram upper bounds in seconds; 0.25 is required by the p95 ack SLO alert.
var ackBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Metrics holds gateway Prometheus counters (hand-written text exposition, stdlib only).
// Labels are low-cardinality only: route (fixed set) and status code. Never tenant_id.
type Metrics struct {
	mu       sync.Mutex
	requests map[[2]string]uint64 // {route, code}

	eventsAccepted atomic.Uint64

	// ack histogram (route-agnostic): per-bucket counts (non-cumulative), sum in seconds, count.
	ackBucketCounts [11]uint64 // len(ackBuckets); guarded by mu
	ackInf          uint64
	ackSum          float64
}

func NewMetrics() *Metrics { return &Metrics{requests: map[[2]string]uint64{}} }

// ObserveRequest counts a finished request.
func (m *Metrics) ObserveRequest(route string, code int) {
	m.mu.Lock()
	m.requests[[2]string{route, fmt.Sprint(code)}]++
	m.mu.Unlock()
}

// ObserveAck records time-to-ack (request start to response) for an ingest request.
func (m *Metrics) ObserveAck(seconds float64) {
	m.mu.Lock()
	m.ackSum += seconds
	placed := false
	for i, b := range ackBuckets {
		if seconds <= b {
			m.ackBucketCounts[i]++
			placed = true
			break
		}
	}
	if !placed {
		m.ackInf++
	}
	m.mu.Unlock()
}

// AddAccepted counts events durably accepted (broker-acked).
func (m *Metrics) AddAccepted(n int) {
	if n > 0 {
		m.eventsAccepted.Add(uint64(n))
	}
}

// WriteProm writes Prometheus text exposition (v0.0.4).
func (m *Metrics) WriteProm(w io.Writer) {
	m.mu.Lock()
	keys := make([][2]string, 0, len(m.requests))
	reqs := make(map[[2]string]uint64, len(m.requests))
	for k, v := range m.requests {
		keys = append(keys, k)
		reqs[k] = v
	}
	bc, inf, sum := m.ackBucketCounts, m.ackInf, m.ackSum
	m.mu.Unlock()
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})

	fmt.Fprint(w, "# HELP astryon_gateway_requests_total HTTP requests handled by the gateway.\n# TYPE astryon_gateway_requests_total counter\n")
	for _, k := range keys {
		fmt.Fprintf(w, "astryon_gateway_requests_total{route=%q,code=%q} %d\n", k[0], k[1], reqs[k])
	}
	fmt.Fprintf(w, "# HELP astryon_gateway_events_accepted_total Events durably accepted (broker-acked).\n# TYPE astryon_gateway_events_accepted_total counter\nastryon_gateway_events_accepted_total %d\n", m.eventsAccepted.Load())

	const h = "astryon_gateway_ack_duration_seconds"
	fmt.Fprintf(w, "# HELP %s Time from ingest request start to response (broker ack).\n# TYPE %s histogram\n", h, h)
	var cum uint64
	for i, b := range ackBuckets {
		cum += bc[i]
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", h, b, cum)
	}
	cum += inf
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", h, cum, h, sum, h, cum)
}

// Handler serves Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.WriteProm(w)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) {
	if s.code == 0 {
		s.code = c
	}
	s.ResponseWriter.WriteHeader(c)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// routeLabel maps a request path to a fixed label set.
func routeLabel(path string) string {
	switch path {
	case "/v1/events", "/v1/events/batch", "/healthz":
		return path
	}
	return "other"
}
