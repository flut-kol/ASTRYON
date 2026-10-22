package query

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// Stats counts final responses of /v1/query/* by outcome. Process-wide because one query-api serves one API;
// labels are a fixed set so cardinality stays bounded.
var Stats requestStats

type requestStats struct {
	ok, clientErr, throttled, backendErr atomic.Int64
}

func (r *requestStats) record(status int) {
	switch {
	case status == http.StatusTooManyRequests:
		r.throttled.Add(1)
	case status >= 500:
		r.backendErr.Add(1)
	case status >= 400:
		r.clientErr.Add(1)
	default:
		r.ok.Add(1)
	}
}

// WritePrometheus renders astryon_query_api_requests_total{outcome=...}.
func (r *requestStats) WritePrometheus(w io.Writer) {
	fmt.Fprint(w, "# HELP astryon_query_api_requests_total Query requests by outcome (ok, client_error, throttled, backend_error)\n# TYPE astryon_query_api_requests_total counter\n")
	for _, c := range []struct {
		o string
		v int64
	}{{"ok", r.ok.Load()}, {"client_error", r.clientErr.Load()}, {"throttled", r.throttled.Load()}, {"backend_error", r.backendErr.Load()}} {
		fmt.Fprintf(w, "astryon_query_api_requests_total{outcome=%q} %d\n", c.o, c.v)
	}
}

// Route groups and auth-failure reasons are fixed sets (the caller passes constants, never request data);
// no tenant or path labels. Durations cover the whole handler incl. auth, limiter wait and the backend call.
var routeGroups = []string{"events", "aggregate", "metrics_summary", "quality_summary"}
var authReasons = []string{"unauthenticated", "missing_scope", "invalid_tenant"}
var auditGapReasons = []string{"unauthenticated_no_tenant", "invalid_tenant", "disabled", "submit_error"}
var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

type durHist struct {
	buckets [13]atomic.Int64 // per-bucket (non-cumulative); last is +Inf
	count   atomic.Int64
	sumNs   atomic.Int64
}

var (
	durations [4]durHist
	authFails [3]atomic.Int64
	auditGaps [4]atomic.Int64
)

func indexOf(set []string, v string) int {
	for i, x := range set {
		if x == v {
			return i
		}
	}
	return -1
}

func (r *requestStats) observe(group string, d time.Duration) {
	i := indexOf(routeGroups, group)
	if i < 0 {
		return
	}
	h := &durations[i]
	sec := d.Seconds()
	b := len(durationBuckets)
	for j, le := range durationBuckets {
		if sec <= le {
			b = j
			break
		}
	}
	h.buckets[b].Add(1)
	h.count.Add(1)
	h.sumNs.Add(int64(d))
}

func (r *requestStats) authFailure(reason string) {
	if i := indexOf(authReasons, reason); i >= 0 {
		authFails[i].Add(1)
	}
}

func (r *requestStats) auditGap(reason string) {
	if i := indexOf(auditGapReasons, reason); i >= 0 {
		auditGaps[i].Add(1)
	}
}

// WriteDurations renders the histogram and auth-failure counter.
func (r *requestStats) WriteDurations(w io.Writer) {
	fmt.Fprint(w, "# HELP astryon_query_api_request_duration_seconds Query request duration by route group\n# TYPE astryon_query_api_request_duration_seconds histogram\n")
	for i, g := range routeGroups {
		h := &durations[i]
		var cum int64
		for j, le := range durationBuckets {
			cum += h.buckets[j].Load()
			fmt.Fprintf(w, "astryon_query_api_request_duration_seconds_bucket{route=%q,le=\"%g\"} %d\n", g, le, cum)
		}
		cum += h.buckets[len(durationBuckets)].Load()
		fmt.Fprintf(w, "astryon_query_api_request_duration_seconds_bucket{route=%q,le=\"+Inf\"} %d\n", g, cum)
		fmt.Fprintf(w, "astryon_query_api_request_duration_seconds_sum{route=%q} %g\n", g, float64(h.sumNs.Load())/1e9)
		fmt.Fprintf(w, "astryon_query_api_request_duration_seconds_count{route=%q} %d\n", g, h.count.Load())
	}
	fmt.Fprint(w, "# HELP astryon_query_api_auth_failures_total Rejected authentication/authorization by reason (unauthenticated, missing_scope, invalid_tenant)\n# TYPE astryon_query_api_auth_failures_total counter\n")
	for i, reason := range authReasons {
		fmt.Fprintf(w, "astryon_query_api_auth_failures_total{reason=%q} %d\n", reason, authFails[i].Load())
	}
	fmt.Fprint(w, "# HELP astryon_query_api_audit_gap_total Security denials not submitted to audit storage by bounded reason\n# TYPE astryon_query_api_audit_gap_total counter\n")
	for i, reason := range auditGapReasons {
		fmt.Fprintf(w, "astryon_query_api_audit_gap_total{reason=%q} %d\n", reason, auditGaps[i].Load())
	}
}
