package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsListenAddr(t *testing.T) {
	for in, want := range map[string]string{"": "", ":9100": "127.0.0.1:9100", "0.0.0.0:9100": "0.0.0.0:9100", "[::1]:9": "[::1]:9"} {
		if got, err := metricsListenAddr(in); err != nil || got != want {
			t.Errorf("%q -> %q %v, want %q", in, got, err, want)
		}
	}
	if _, err := metricsListenAddr("nope"); err == nil {
		t.Error("expected error")
	}
}

func TestMetricsHandler(t *testing.T) {
	w := httptest.NewRecorder()
	metricsHandler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "astryon_quarantine_browse_requests_total") {
		t.Fatal(w.Code, w.Body.String())
	}
}
