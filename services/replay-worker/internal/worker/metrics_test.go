package worker

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func scrape(m *Metrics) string {
	var b bytes.Buffer
	m.WritePrometheus(&b)
	return b.String()
}

func mustHave(t *testing.T, out string, lines ...string) {
	t.Helper()
	for _, l := range lines {
		if !strings.Contains(out, l+"\n") {
			t.Errorf("missing %q in\n%s", l, out)
		}
	}
}

func TestMetricsSuccess(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	ch.rows[ch.key(tenantA, 1)] = mkRows(tenantA, 10)
	src := &fakeSource{msgs: [][]byte{jobJSON(t, nil)}}
	w := newWorker(src, ch, &fakeSink{}, &fakeStatus{})
	w.Metrics = &Metrics{}
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	out := scrape(w.Metrics)
	mustHave(t, out,
		"astryon_replay_worker_replays_requested_total 1", "astryon_replay_worker_replays_approved_total 1",
		"astryon_replay_worker_replays_started_total 1", "astryon_replay_worker_replays_completed_total 1",
		"astryon_replay_worker_events_replayed_total 10", "astryon_replay_worker_active_replays 0",
		"astryon_replay_worker_publish_errors_total 0",
		"astryon_replay_worker_replay_duration_seconds_count 1", `astryon_replay_worker_replay_duration_seconds_bucket{le="+Inf"} 1`,
		`astryon_replay_worker_replays_failed_total{code="publish"} 0`)
	for _, bad := range []string{tenantA, srcID, "job1"} {
		if strings.Contains(out, bad) {
			t.Errorf("identifier %q leaked", bad)
		}
	}
}

func TestMetricsPublishFailure(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	ch.rows[ch.key(tenantA, 1)] = mkRows(tenantA, 6)
	src := &fakeSource{msgs: [][]byte{jobJSON(t, nil)}}
	w := newWorker(src, ch, &fakeSink{err: errors.New("broker down")}, &fakeStatus{})
	w.Metrics = &Metrics{}
	if _, err := w.Step(context.Background()); err == nil {
		t.Fatal("want error")
	}
	mustHave(t, scrape(w.Metrics),
		`astryon_replay_worker_replays_failed_total{code="publish"} 1`, "astryon_replay_worker_publish_errors_total 1",
		"astryon_replay_worker_replays_completed_total 0", "astryon_replay_worker_active_replays 0",
		"astryon_replay_worker_replay_duration_seconds_count 1")
}

func TestMetricsInvalidAndLeaseAndNotRunnable(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	src := &fakeSource{msgs: [][]byte{jobJSON(t, func(j *Job) { j.Ranges = nil })}}
	w := newWorker(src, ch, &fakeSink{}, &fakeStatus{})
	w.Metrics = &Metrics{}
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustHave(t, scrape(w.Metrics), `astryon_replay_worker_replays_failed_total{code="invalid_job"} 1`, "astryon_replay_worker_replays_requested_total 0")

	src = &fakeSource{msgs: [][]byte{jobJSON(t, nil)}}
	w = newWorker(src, ch, &fakeSink{}, &fakeStatus{err: map[string]error{StatusRunning: ErrRunningRecently}})
	w.Metrics = &Metrics{}
	if _, err := w.Step(context.Background()); !errors.Is(err, ErrRunningRecently) {
		t.Fatal(err)
	}
	mustHave(t, scrape(w.Metrics), `astryon_replay_worker_replays_failed_total{code="lease_busy"} 1`, "astryon_replay_worker_replays_started_total 0")
}

func TestFailureCodeAndHistogram(t *testing.T) {
	if FailureCode(permanent("x")) != FailPermanent || FailureCode(stage(FailBaseRead, errors.New("x"))) != FailBaseRead || FailureCode(errors.New("x")) != FailOther {
		t.Fatal("codes")
	}
	m := &Metrics{}
	m.Fail("some-unbounded-" + srcID)
	m.ObserveDuration(500 * time.Millisecond)
	m.ObserveDuration(30 * time.Second)
	m.ObserveDuration(3 * time.Hour)
	mustHave(t, scrape(m), `astryon_replay_worker_replays_failed_total{code="other"} 1`,
		`astryon_replay_worker_replay_duration_seconds_bucket{le="1"} 1`, `astryon_replay_worker_replay_duration_seconds_bucket{le="60"} 2`,
		`astryon_replay_worker_replay_duration_seconds_bucket{le="7200"} 2`, `astryon_replay_worker_replay_duration_seconds_bucket{le="+Inf"} 3`,
		"astryon_replay_worker_replay_duration_seconds_count 3")
	var nm *Metrics
	nm.Fail("x")
	nm.ObserveDuration(time.Second)
	w := &Worker{} // no Metrics configured: counting must not panic
	w.m().add(&w.m().Started, 1)
}

func TestMetricsListenerOptIn(t *testing.T) {
	for in, want := range map[string]string{":9108": "127.0.0.1:9108", "0.0.0.0:9108": "0.0.0.0:9108"} {
		if got, ok := MetricsListenAddr(in); !ok || got != want {
			t.Errorf("%q -> %q %v", in, got, ok)
		}
	}
	if _, ok := MetricsListenAddr(""); ok || ServeMetrics("", nil, nil) != nil {
		t.Fatal("must be off when unset")
	}
	m := &Metrics{}
	rec := httptest.NewRecorder()
	MetricsHandler(m.WritePrometheus).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "# TYPE astryon_replay_worker_active_replays gauge") {
		t.Fatal(rec.Code, rec.Body.String())
	}
}
