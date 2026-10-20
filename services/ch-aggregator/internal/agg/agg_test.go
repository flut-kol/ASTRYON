package agg

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHourWindow(t *testing.T) {
	w, err := HourWindow(time.Date(2026, 10, 20, 10, 30, 0, 0, time.UTC), time.Date(2026, 10, 20, 12, 5, 0, 0, time.UTC))
	if err != nil || !w.From.Equal(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)) || !w.To.Equal(time.Date(2026, 10, 20, 13, 0, 0, 0, time.UTC)) {
		t.Fatalf("%v %v", w, err)
	}
	if _, err := HourWindow(w.To, w.From); err == nil {
		t.Fatal("empty window accepted")
	}
}

func TestRecomputeSendsRevisionedStatements(t *testing.T) {
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
	}))
	defer srv.Close()
	c, _ := New(srv.URL, "astryon", "", "")
	w, _ := HourWindow(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC), time.Date(2026, 10, 20, 11, 0, 0, 0, time.UTC))
	if err := c.Recompute(context.Background(), w, 42); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 4 {
		t.Fatalf("statements=%d", len(bodies))
	}
	for _, b := range bodies {
		if !strings.Contains(b, "LIMIT 1 BY") && !strings.Contains(b, "ARRAY JOIN") {
			t.Fatalf("no dedup in %s", b)
		}
		if strings.Contains(b, "FINAL") {
			t.Fatal("must not rely on merge-time semantics")
		}
		if !strings.Contains(b, "2026-10-20 10:00:00") || !strings.Contains(b, " 42,") {
			t.Fatalf("window/revision missing: %s", b)
		}
	}
}

func TestRecomputeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "Code: 47", 500) }))
	defer srv.Close()
	c, _ := New(srv.URL, "", "", "")
	w, _ := HourWindow(time.Now().Add(-time.Hour), time.Now())
	if err := c.Recompute(context.Background(), w, 1); err == nil {
		t.Fatal("expected error")
	}
	if _, err := New("", "x; DROP", "", ""); err == nil {
		t.Fatal("identifier injection accepted")
	}
}

func TestReconcile(t *testing.T) {
	var q string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		q = string(b)
		_, _ = w.Write([]byte(`{"tenant_id":"t","source_id":"s","batch_id":"b","processing_version":1,"rows":"3","unique_events":"2"}` + "\n"))
	}))
	defer srv.Close()
	c, _ := New(srv.URL, "", "", "")
	w, _ := HourWindow(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC), time.Date(2026, 10, 20, 11, 0, 0, 0, time.UTC))
	d, err := c.Reconcile(context.Background(), w, 10)
	if err != nil || len(d) != 1 || d[0].Rows != 3 || d[0].Unique != 2 || !strings.Contains(q, "HAVING rows != unique_events") {
		t.Fatalf("%v %v %s", d, err, q)
	}
}

func TestAggregateDriftAndGapsSQL(t *testing.T) {
	var qs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		qs = append(qs, string(b))
		if strings.Contains(string(b), "WITH FILL") {
			_, _ = w.Write([]byte(`{"tenant_id":"t","source_id":"s","processing_version":1,"hour":"2026-10-20 11:00:00"}` + "\n"))
			return
		}
		_, _ = w.Write([]byte(`{"tenant_id":"t","source_id":"s","processing_version":1,"hour":"2026-10-20 10:00:00","stored":"5","actual":"7"}` + "\n"))
	}))
	defer srv.Close()
	c, _ := New(srv.URL, "", "", "")
	w, _ := HourWindow(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC), time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC))
	d, err := c.AggregateDrift(context.Background(), w, 10)
	if err != nil || len(d) != 1 || d[0].Stored != 5 || d[0].Actual != 7 {
		t.Fatalf("%v %v", d, err)
	}
	g, err := c.Gaps(context.Background(), w, 10)
	if err != nil || len(g) != 1 || g[0].Hour != "2026-10-20 11:00:00" {
		t.Fatalf("%v %v", g, err)
	}
	if !strings.Contains(qs[0], "argMax(events_count, revision)") || !strings.Contains(qs[0], "LIMIT 1 BY") || strings.Contains(qs[0], "FINAL") {
		t.Fatalf("drift SQL must use latest revision and de-duplicated events: %s", qs[0])
	}
}
