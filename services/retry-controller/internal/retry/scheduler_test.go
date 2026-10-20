package retry

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClaimer struct {
	calls atomic.Int32
	err   error
}

func (f *fakeClaimer) ClaimDue(context.Context, int) (int64, error) { f.calls.Add(1); return 0, f.err }

func TestSchedulerPollsAndStops(t *testing.T) {
	f := &fakeClaimer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- (Scheduler{Store: f, Interval: time.Millisecond, Batch: 2}).Run(ctx) }()
	time.Sleep(5 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if f.calls.Load() < 2 {
		t.Fatalf("calls=%d", f.calls.Load())
	}
}
func TestSchedulerReportsDatabaseFailure(t *testing.T) {
	f := &fakeClaimer{err: errors.New("down")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan error, 1)
	go (Scheduler{Store: f, Interval: time.Hour, Batch: 1, OnError: func(e error) { got <- e }}).Run(ctx)
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("error not reported")
	}
}

func TestSchedulerRejectsInvalidConfiguration(t *testing.T) {
	f := &fakeClaimer{}
	for _, s := range []Scheduler{{}, {Store: f, Batch: 1}, {Store: f, Interval: time.Second}} {
		if err := s.Validate(); err == nil {
			t.Fatal("invalid scheduler accepted")
		}
	}
}

func TestMetricsAreTenantFreePrometheusCounters(t *testing.T) {
	m := &Metrics{}
	m.Observe(3, nil)
	m.Observe(0, errors.New("database down"))
	r := httptest.NewRecorder()
	m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	body := r.Body.String()
	for _, want := range []string{"astryon_retry_polls_total 2", "astryon_retry_released_total 3", "astryon_retry_errors_total 1"} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in %q", want, body)
		}
	}
	if strings.Contains(body, "database down") || strings.Contains(body, "tenant") {
		t.Fatalf("sensitive label in %q", body)
	}
}
