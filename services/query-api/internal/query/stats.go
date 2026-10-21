package query

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
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
