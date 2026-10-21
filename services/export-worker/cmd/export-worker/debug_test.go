package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/astryon/export-worker/internal/export"
)

func TestLoopbackAddr(t *testing.T) {
	for a, ok := range map[string]bool{"127.0.0.1:6060": true, "[::1]:6060": true, "localhost:6060": true,
		":6060": false, "0.0.0.0:6060": false, "10.1.2.3:6060": false, "example.com:6060": false, "6060": false} {
		if err := loopbackAddr(a); (err == nil) != ok {
			t.Errorf("%s: err=%v want ok=%v", a, err, ok)
		}
	}
}

func TestPprofOnlyOnDebugHandler(t *testing.T) {
	srv := httptest.NewServer(debugHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/debug/pprof/goroutine?debug=1")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("debug handler: %v %v", resp, err)
	}
	_ = resp.Body.Close()
	// The public handler never routes /debug/*.
	pub := (&export.Server{Auth: denyAll{}}).Handler()
	rr := httptest.NewRecorder()
	pub.ServeHTTP(rr, httptest.NewRequest("GET", "/debug/pprof/", nil))
	if rr.Code == 200 {
		t.Fatalf("public handler serves pprof: %d", rr.Code)
	}
}
