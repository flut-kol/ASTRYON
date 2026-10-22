package replay

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type failPub struct{}

func (failPub) Publish(context.Context, ReplayJob) error { return errors.New("kafka down") }

func metricsFixture(pub JobPublisher) (http.Handler, *Metrics) {
	m := &Metrics{}
	pl := &Planner{Alloc: NewMemAllocator(), Counter: FixedCounter{N: 1}, Publisher: MeteredPublisher{Next: pub, M: m}}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Scopes: []string{ScopeReplayWrite}}}
	return (&Handler{Auth: auth, Planner: pl, Metrics: m}).Routes(), m
}

func post(h http.Handler, tok, b string) int {
	r := httptest.NewRequest("POST", "/v1/replays", strings.NewReader(b))
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code
}

func scrape(m *Metrics) string {
	var b bytes.Buffer
	m.WritePrometheus(&b)
	return b.String()
}

func TestMetricsCountersAndNoIdentifiers(t *testing.T) {
	h, m := metricsFixture(&MemPublisher{})
	if c := post(h, "tok", body("", false)); c != 202 {
		t.Fatal(c)
	}
	post(h, "tok", body("", true))
	post(h, "tok", `{"source_id":"bad"}`)
	post(h, "", body("", true))
	out := scrape(m)
	for _, want := range []string{
		"astryon_replay_controller_replays_requested_total 2", // execute + the invalid one (decoded before validation)
		"astryon_replay_controller_replays_dry_run_total 1",
		"astryon_replay_controller_replays_approved_total 1",
		`astryon_replay_controller_replays_rejected_total{code="invalid_request"} 1`,
		`astryon_replay_controller_replays_rejected_total{code="unauthenticated"} 1`,
		"astryon_replay_controller_publish_errors_total 0",
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	for _, bad := range []string{tenantA, srcX} {
		if strings.Contains(out, bad) {
			t.Errorf("identifier %s leaked into metrics", bad)
		}
	}
}

func TestMetricsPublishError(t *testing.T) {
	h, m := metricsFixture(failPub{})
	if c := post(h, "tok", body("", false)); c != 502 {
		t.Fatal(c)
	}
	out := scrape(m)
	if !strings.Contains(out, "publish_errors_total 1\n") || !strings.Contains(out, `{code="upstream_error"} 1`) || strings.Contains(out, "approved_total 1") {
		t.Fatal(out)
	}
}

func TestMetricsNilSafeAndConcurrent(t *testing.T) {
	var nm *Metrics
	nm.IncRequested(false)
	nm.IncApproved()
	nm.IncRejected("x")
	nm.IncActivation("activate")
	m := &Metrics{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.IncRejected("bogus-" + strings.Repeat("x", i)) // unbounded input collapses to "other"
			m.IncActivation("weird")
			_ = scrape(m)
		}()
	}
	wg.Wait()
	if !strings.Contains(scrape(m), `replays_rejected_total{code="other"} 50`) || !strings.Contains(scrape(m), `activations_total{action="other"} 50`) {
		t.Fatal(scrape(m))
	}
}

func TestMetricsListenAddrLoopbackDefault(t *testing.T) {
	for in, want := range map[string]string{":9107": "127.0.0.1:9107", "0.0.0.0:9107": "0.0.0.0:9107", "10.1.2.3:1": "10.1.2.3:1"} {
		if got, ok := MetricsListenAddr(in); !ok || got != want {
			t.Errorf("%q -> %q %v", in, got, ok)
		}
	}
	if _, ok := MetricsListenAddr(""); ok {
		t.Fatal("empty must be off")
	}
	if ServeMetrics("", nil, nil) != nil {
		t.Fatal("off must not start")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	m := &Metrics{}
	w := httptest.NewRecorder()
	MetricsHandler(m.WritePrometheus).ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") || !strings.Contains(w.Body.String(), "# TYPE astryon_replay_controller_replays_requested_total counter") {
		t.Fatal(w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	MetricsHandler(m.WritePrometheus).ServeHTTP(w, httptest.NewRequest("POST", "/metrics", nil))
	if w.Code == 200 {
		t.Fatal("POST must not be served")
	}
}
