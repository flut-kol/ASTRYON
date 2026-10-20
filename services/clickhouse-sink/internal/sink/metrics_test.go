package sink

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPromExposition(t *testing.T) {
	m := &Metrics{}
	m.Consumed.Add(3)
	m.Inserted.Add(2)
	m.Quarantined.Add(1)
	m.InsertFailures.Add(4)
	m.CommitFailures.Add(5)
	m.Buffered.Store(7)
	var ready atomic.Bool
	mux := m.Mux(&ready)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"# TYPE astryon_clickhouse_sink_consumed_total counter\nastryon_clickhouse_sink_consumed_total 3\n",
		"astryon_clickhouse_sink_inserted_rows_total 2\n",
		"astryon_clickhouse_sink_quarantined_total 1\n",
		"astryon_clickhouse_sink_insert_failures_total 4\n",
		"astryon_clickhouse_sink_commit_failures_total 5\n",
		"# HELP astryon_clickhouse_sink_buffered ",
		"# TYPE astryon_clickhouse_sink_buffered gauge\nastryon_clickhouse_sink_buffered 7\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
	m.Consumed.Add(1)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rec.Body.String(), "consumed_total 4\n") {
		t.Error("counter increment not reflected")
	}
}

func TestHealthReady(t *testing.T) {
	var ready atomic.Bool
	mux := (&Metrics{}).Mux(&ready)
	code := func(p string) int {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", p, nil))
		return rec.Code
	}
	if code("/healthz") != http.StatusOK || code("/readyz") != http.StatusServiceUnavailable {
		t.Fatal("unexpected before ready")
	}
	ready.Store(true)
	if code("/readyz") != http.StatusOK || code("/debug/vars") != http.StatusOK {
		t.Fatal("unexpected after ready")
	}
}
