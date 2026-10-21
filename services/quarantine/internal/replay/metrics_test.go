package replay

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsCountOutcomesWithoutIdentityLabels(t *testing.T) {
	read := func() map[string]int64 {
		return map[string]int64{
			"ok": Metrics.browse[0].Load(), "unauth": Metrics.browse[1].Load(),
			"pub":   Metrics.publish[idx(publishOutcomes, "published")].Load(),
			"audit": Metrics.publish[idx(publishOutcomes, "audit_unavailable")].Load(),
			"fail":  Metrics.publish[idx(publishOutcomes, "publish_failed")].Load(),
			"big":   Metrics.publish[idx(publishOutcomes, "scope_too_large")].Load(),
			"rej":   Metrics.publish[idx(publishOutcomes, "rejected")].Load(),
			"dry":   Metrics.plans[0].Load(), "cand": Metrics.candCnt.Load(),
		}
	}
	b := read()
	pub := func(a BrowserAuthenticator, p *replayPlanner, pu *replayPublisher, au *replayAudit) {
		publishHandler(a, p, pu, au).ServeHTTP(httptest.NewRecorder(), publishRequest(validReplayJSON))
	}
	auth := browserAuth{TenantID: tenantA, Scopes: map[string]bool{ReplayPublishScope: true, ReplayScope: true}}
	ok := &replayPlanner{plan: ReplayDryRunPlan{CandidateCount: 2}}
	pub(auth, ok, &replayPublisher{}, &replayAudit{})
	pub(auth, ok, &replayPublisher{}, &replayAudit{err: errors.New("x")})
	pub(auth, ok, &replayPublisher{err: errors.New("x")}, &replayAudit{})
	pub(auth, &replayPlanner{plan: ReplayDryRunPlan{CandidateCountIsLowerBound: true}}, &replayPublisher{}, &replayAudit{})
	pub(browserAuth{TenantID: tenantA}, ok, &replayPublisher{}, &replayAudit{})
	publishHandler(auth, ok, &replayPublisher{}, &replayAudit{}).ServeHTTP(httptest.NewRecorder(), replayRequest(validReplayJSON))
	BrowserHandler{Auth: browserAuth{TenantID: tenantA}}.Routes().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/quarantine", nil))
	a := read()
	for k, d := range map[string]int64{"pub": 1, "audit": 1, "fail": 1, "big": 1, "rej": 1, "dry": 1, "cand": 5} {
		if a[k]-b[k] != d {
			t.Errorf("%s delta %d want %d", k, a[k]-b[k], d)
		}
	}
	var sb strings.Builder
	Metrics.WritePrometheus(&sb)
	out := sb.String()
	if !strings.Contains(out, `astryon_quarantine_replay_publishes_total{outcome="audit_unavailable"}`) ||
		!strings.Contains(out, `astryon_quarantine_replay_candidates_bucket{le="+Inf"}`) || strings.Contains(out, tenantA) {
		t.Fatalf("bad exposition:\n%s", out)
	}
}
