package gateway

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestAckDurationHistogramExposition(t *testing.T) {
	var m metrics
	h := func(code int, d time.Duration) {
		m.count(func(w http.ResponseWriter, _ *http.Request) { time.Sleep(d); w.WriteHeader(code) })(httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil))
	}
	h(202, 0)
	h(202, 30*time.Millisecond)
	h(400, 0)
	h(503, 0)
	rec := httptest.NewRecorder()
	m.handler(rec, nil)
	body := rec.Body.String()
	for _, want := range []string{
		"# TYPE astryon_gateway_ack_duration_seconds histogram\n",
		`astryon_gateway_ack_duration_seconds_bucket{outcome="ok",le="0.005"} 1` + "\n",
		`astryon_gateway_ack_duration_seconds_bucket{outcome="ok",le="0.05"} 2` + "\n",
		`astryon_gateway_ack_duration_seconds_bucket{outcome="ok",le="+Inf"} 2` + "\n",
		`astryon_gateway_ack_duration_seconds_count{outcome="ok"} 2` + "\n",
		`astryon_gateway_ack_duration_seconds_count{outcome="client_error"} 1` + "\n",
		`astryon_gateway_ack_duration_seconds_count{outcome="error"} 1` + "\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
	if strings.Contains(body, "tenant") || strings.Contains(body, "source") {
		t.Fatal("high-cardinality label present")
	}
}

func TestAckDurationBucketsMonotonic(t *testing.T) {
	for i := 1; i < len(ackBuckets); i++ {
		if ackBuckets[i] <= ackBuckets[i-1] {
			t.Fatalf("bounds not increasing at %d", i)
		}
	}
	var m metrics
	for _, d := range []time.Duration{time.Millisecond, 7 * time.Millisecond, 80 * time.Millisecond, 3 * time.Second, time.Minute} {
		m.dur[0].observe(d)
	}
	rec := httptest.NewRecorder()
	m.handler(rec, nil)
	prev, buckets := int64(-1), 0
	for _, l := range strings.Split(rec.Body.String(), "\n") {
		if !strings.HasPrefix(l, `astryon_gateway_ack_duration_seconds_bucket{outcome="ok"`) {
			continue
		}
		var v int64
		fmt.Sscan(l[strings.LastIndex(l, " ")+1:], &v)
		if v < prev {
			t.Fatalf("cumulative bucket decreased: %s", l)
		}
		prev = v
		buckets++
	}
	if buckets != len(ackBuckets)+1 || prev != 5 {
		t.Fatalf("buckets=%d last=%d", buckets, prev)
	}
}

func TestAckDurationRace(t *testing.T) {
	var m metrics
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				m.count(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(202) })(httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil))
				m.handler(httptest.NewRecorder(), nil)
			}
		}()
	}
	wg.Wait()
	if got := m.dur[0].count.Load(); got != 1600 {
		t.Fatalf("count=%d", got)
	}
}
