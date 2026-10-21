package gateway

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// metrics counts responses of the ingestion endpoints. An ACK is any final response to an event write;
// an ACK error is a 5xx (the event was not durably accepted). 4xx are client errors and are counted separately.
type metrics struct {
	ack, ackErrors, clientErrors atomic.Int64
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
		h(rec, r)
		m.ack.Add(1)
		switch {
		case rec.status >= 500:
			m.ackErrors.Add(1)
		case rec.status >= 400:
			m.clientErrors.Add(1)
		}
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
}
