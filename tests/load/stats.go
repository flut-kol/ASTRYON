package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Percentile returns the nearest-rank percentile (p in (0,100]) of sorted
// ascending samples. Empty input yields 0.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	rank := int(math.Ceil(p / 100 * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

// Stats collects results from concurrent workers.
type Stats struct {
	mu          sync.Mutex
	requests    int64
	httpStatus  map[int]int64
	sent        int64
	accepted    int64
	quarantined int64
	failed      int64
	bytes       int64
	latencies   []time.Duration
	errs        map[string]int64
	acceptedIDs map[string]struct{} // nil unless tracking
	acceptedDup int64               // accepted acks for an event_id already accepted before
}

// NewStats creates a collector; trackIDs enables unique accepted event_id tracking.
func NewStats(trackIDs bool) *Stats {
	s := &Stats{httpStatus: map[int]int64{}, errs: map[string]int64{}}
	if trackIDs {
		s.acceptedIDs = map[string]struct{}{}
	}
	return s
}

// Outcome is one request's result.
type Outcome struct {
	Events      int
	Bytes       int
	Latency     time.Duration
	HTTPStatus  int // 0 on transport error
	Accepted    int
	Quarantined int
	Failed      int
	AcceptedIDs []string
	Err         string
}

// Record adds an outcome.
func (s *Stats) Record(o Outcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	s.sent += int64(o.Events)
	s.accepted += int64(o.Accepted)
	s.quarantined += int64(o.Quarantined)
	s.failed += int64(o.Failed)
	s.bytes += int64(o.Bytes)
	s.httpStatus[o.HTTPStatus]++
	if o.Err != "" {
		s.errs[o.Err]++
	}
	if o.HTTPStatus != 0 {
		s.latencies = append(s.latencies, o.Latency)
	}
	if s.acceptedIDs != nil {
		for _, id := range o.AcceptedIDs {
			if _, ok := s.acceptedIDs[id]; ok {
				s.acceptedDup++
			}
			s.acceptedIDs[id] = struct{}{}
		}
	}
}

// IDs returns the unique accepted event ids (nil when not tracking).
func (s *Stats) IDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acceptedIDs == nil {
		return nil
	}
	ids := make([]string, 0, len(s.acceptedIDs))
	for id := range s.acceptedIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Report is the final result (also the JSON schema).
type Report struct {
	DurationSec     float64            `json:"duration_sec"`
	Requests        int64              `json:"requests"`
	Sent            int64              `json:"sent"`
	Accepted        int64              `json:"accepted"`
	Quarantined     int64              `json:"quarantined"`
	Failed          int64              `json:"failed"`
	UniqueAccepted  *int64             `json:"unique_accepted,omitempty"`
	AcceptedRepeats *int64             `json:"accepted_repeats,omitempty"`
	EventsPerSec    float64            `json:"events_per_sec"`
	AcceptedPerSec  float64            `json:"accepted_per_sec"`
	MBPerSec        float64            `json:"mb_per_sec"`
	BytesSent       int64              `json:"bytes_sent"`
	LatencyMs       map[string]float64 `json:"ack_latency_ms"`
	HTTPStatus      map[string]int64   `json:"http_status"`
	Errors          map[string]int64   `json:"errors,omitempty"`
	Config          map[string]any     `json:"config,omitempty"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// Report snapshots the results for an elapsed wall time.
func (s *Stats) Report(elapsed time.Duration) Report {
	s.mu.Lock()
	defer s.mu.Unlock()
	lat := append([]time.Duration(nil), s.latencies...)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	sec := elapsed.Seconds()
	if sec <= 0 {
		sec = 1e-9
	}
	r := Report{
		DurationSec: sec, Requests: s.requests, Sent: s.sent, Accepted: s.accepted,
		Quarantined: s.quarantined, Failed: s.failed, BytesSent: s.bytes,
		EventsPerSec: float64(s.sent) / sec, AcceptedPerSec: float64(s.accepted) / sec,
		MBPerSec: float64(s.bytes) / sec / (1 << 20),
		LatencyMs: map[string]float64{"min": 0, "p50": ms(Percentile(lat, 50)), "p95": ms(Percentile(lat, 95)),
			"p99": ms(Percentile(lat, 99)), "max": 0},
		HTTPStatus: map[string]int64{},
	}
	if len(lat) > 0 {
		r.LatencyMs["min"], r.LatencyMs["max"] = ms(lat[0]), ms(lat[len(lat)-1])
	}
	for k, v := range s.httpStatus {
		key := "transport_error"
		if k != 0 {
			key = itoa(k)
		}
		r.HTTPStatus[key] = v
	}
	if len(s.errs) > 0 {
		r.Errors = map[string]int64{}
		for k, v := range s.errs {
			r.Errors[k] = v
		}
	}
	if s.acceptedIDs != nil {
		u, d := int64(len(s.acceptedIDs)), s.acceptedDup
		r.UniqueAccepted, r.AcceptedRepeats = &u, &d
	}
	return r
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

// Text renders the report for humans.
func (r Report) Text() string {
	s := fmt.Sprintf("duration      %.2fs (%d requests)\n", r.DurationSec, r.Requests)
	s += fmt.Sprintf("sent          %d\naccepted      %d\nquarantined   %d\nfailed        %d\n", r.Sent, r.Accepted, r.Quarantined, r.Failed)
	if r.UniqueAccepted != nil {
		s += fmt.Sprintf("unique acc.   %d (repeat accepts %d)\n", *r.UniqueAccepted, *r.AcceptedRepeats)
	}
	s += fmt.Sprintf("events/s      %.1f sent, %.1f accepted\nMB/s          %.3f (%d bytes)\n", r.EventsPerSec, r.AcceptedPerSec, r.MBPerSec, r.BytesSent)
	s += fmt.Sprintf("ack latency   p50=%.2fms p95=%.2fms p99=%.2fms (min %.2f, max %.2f)\n",
		r.LatencyMs["p50"], r.LatencyMs["p95"], r.LatencyMs["p99"], r.LatencyMs["min"], r.LatencyMs["max"])
	s += fmt.Sprintf("http status   %v\n", r.HTTPStatus)
	if len(r.Errors) > 0 {
		s += fmt.Sprintf("errors        %v\n", r.Errors)
	}
	return s
}
