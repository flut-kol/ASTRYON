package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposition(t *testing.T) {
	m := NewMetrics()
	m.ObserveRequest("/v1/events", 202)
	m.ObserveRequest("/v1/events", 202)
	m.ObserveRequest("/v1/events", 401)
	m.AddAccepted(3)
	m.ObserveAck(0.2)
	m.ObserveAck(0.3)
	m.ObserveAck(20)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	out := rec.Body.String()
	for _, want := range []string{
		`astryon_gateway_requests_total{route="/v1/events",code="202"} 2`,
		`astryon_gateway_requests_total{route="/v1/events",code="401"} 1`,
		`astryon_gateway_events_accepted_total 3`,
		`astryon_gateway_ack_duration_seconds_bucket{le="0.25"} 1`,
		`astryon_gateway_ack_duration_seconds_bucket{le="0.5"} 2`,
		`astryon_gateway_ack_duration_seconds_bucket{le="+Inf"} 3`,
		`astryon_gateway_ack_duration_seconds_count 3`,
		`# TYPE astryon_gateway_ack_duration_seconds histogram`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestServerInstrumentsRequests(t *testing.T) {
	s := NewServer(Config{MaxBodyBytes: 1 << 20, MaxBatchBytes: 1 << 20, MaxBatchEvents: 10}, DevStaticAuthenticator{Token: "t", Principal: Principal{TenantID: "11111111-1111-4111-8111-111111111111", Scopes: []string{ScopeEventsWrite}}}, &FakePublisher{})
	h := s.Handler()
	// unauthenticated -> 401
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/events", strings.NewReader("{}")))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/healthz", nil))
	// authenticated batch with an invalid (quarantined) event -> 207, none accepted
	req := httptest.NewRequest("POST", "/v1/events/batch", strings.NewReader(`{"events":[{}]}`))
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body)
	}
	mrec := httptest.NewRecorder()
	s.Metrics().Handler().ServeHTTP(mrec, httptest.NewRequest("GET", "/metrics", nil))
	out := mrec.Body.String()
	for _, want := range []string{
		`route="/v1/events",code="401"} 1`,
		`route="/healthz",code="200"} 1`,
		`route="/v1/events/batch",code="207"} 1`,
		`astryon_gateway_ack_duration_seconds_count 2`,
		`astryon_gateway_events_accepted_total 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// public API must not serve /metrics
	pr := httptest.NewRecorder()
	h.ServeHTTP(pr, httptest.NewRequest("GET", "/metrics", nil))
	if pr.Code != http.StatusNotFound && pr.Code != http.StatusMethodNotAllowed {
		t.Errorf("public /metrics code=%d", pr.Code)
	}
}
