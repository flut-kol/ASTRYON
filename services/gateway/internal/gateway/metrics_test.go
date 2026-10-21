package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsCountResponses(t *testing.T) {
	var m metrics
	for _, code := range []int{202, 400, 503, 207} {
		code := code
		m.count(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) })(httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil))
	}
	rec := httptest.NewRecorder()
	m.handler(rec, nil)
	for _, want := range []string{"astryon_gateway_ack_total 4\n", "astryon_gateway_ack_errors_total 1\n", "astryon_gateway_client_errors_total 1\n"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("missing %q in:\n%s", want, rec.Body.String())
		}
	}
}
