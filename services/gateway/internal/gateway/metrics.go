package gateway

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// ackBuckets are the upper bounds (seconds) of astryon_gateway_ack_duration_seconds, sized for an ACK SLO of a few hundred ms.
var ackBuckets = [...]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// Outcomes label the histogram; fixed set keeps cardinality at 3 series sets.
var ackOutcomes = [...]string{"ok", "client_error", "error"}

// hist is a lock-free Prometheus histogram: per-bucket (non-cumulative) counts, sum in nanoseconds.
type hist struct {
	buckets [len(ackBuckets) + 1]atomic.Int64 // last = +Inf
	sumNs   atomic.Int64
	count   atomic.Int64
}

func (h *hist) observe(d time.Duration) {
	sec := d.Seconds()
	i := 0
	for i < len(ackBuckets) && sec > ackBuckets[i] {
		i++
	}
	h.buckets[i].Add(1)
	h.sumNs.Add(int64(d))
	h.count.Add(1)
}

// metrics counts responses of the ingestion endpoints. An ACK is any final response to an event write;
// an ACK error is a 5xx (the event was not durably accepted). 4xx are client errors and are counted separately.
type metrics struct {
	ack, ackErrors, clientErrors atomic.Int64
	dur                          [len(ackOutcomes)]hist
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// count wraps an ingestion handler.
func (m *metrics) count(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		h(rec, r)
		d := time.Since(start)
		m.ack.Add(1)
		o := 0
		switch {
		case rec.status >= 500:
			m.ackErrors.Add(1)
			o = 2
		case rec.status >= 400:
			m.clientErrors.Add(1)
			o = 1
		}
		m.dur[o].observe(d)
	}
}

func (m *metrics) handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, c := range []struct {
		name, help string
		v          int64
	}{
		{"ack_total", "Final responses to event write requests", m.ack.Load()},
		{"ack_errors_total", "Event write requests answered with 5xx (not durably accepted)", m.ackErrors.Load()},
		{"client_errors_total", "Event write requests answered with 4xx", m.clientErrors.Load()},
	} {
		fmt.Fprintf(w, "# HELP astryon_gateway_%s %s\n# TYPE astryon_gateway_%s counter\nastryon_gateway_%s %d\n", c.name, c.help, c.name, c.name, c.v)
	}
	const n = "astryon_gateway_ack_duration_seconds"
	fmt.Fprintf(w, "# HELP %s Time from request start to final response of event write requests, by outcome\n# TYPE %s histogram\n", n, n)
	for i, o := range ackOutcomes {
		h := &m.dur[i]
		var cum int64
		for j, le := range ackBuckets {
			cum += h.buckets[j].Load()
			fmt.Fprintf(w, "%s_bucket{outcome=%q,le=\"%g\"} %d\n", n, o, le, cum)
		}
		cum += h.buckets[len(ackBuckets)].Load()
		fmt.Fprintf(w, "%s_bucket{outcome=%q,le=\"+Inf\"} %d\n", n, o, cum)
		fmt.Fprintf(w, "%s_sum{outcome=%q} %g\n%s_count{outcome=%q} %d\n", n, o, float64(h.sumNs.Load())/1e9, n, o, cum)
	}
}
