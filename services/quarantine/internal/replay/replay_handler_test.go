package replay

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type replayPlanner struct {
	tenant  string
	request ReplayDryRunRequest
	plan    ReplayDryRunPlan
	err     error
	calls   int
}

type replayPublisher struct {
	jobs []ReplayJob
	err  error
}

func (p *replayPublisher) PublishReplayJob(_ context.Context, job ReplayJob) error {
	p.jobs = append(p.jobs, job)
	return p.err
}

type replayAudit struct {
	events []ReplayAuditEvent
	err    error
}

type replayMirror struct{ events []MirrorEvent }

func (m *replayMirror) RecordMirror(_ context.Context, event MirrorEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (a *replayAudit) RecordReplayAudit(_ context.Context, event ReplayAuditEvent) error {
	if a.err != nil {
		return a.err
	}
	a.events = append(a.events, event)
	return nil
}

func (p *replayPlanner) PlanReplay(_ context.Context, tenant string, request ReplayDryRunRequest) (ReplayDryRunPlan, error) {
	p.calls++
	p.tenant, p.request = tenant, request
	return p.plan, p.err
}

const validReplayJSON = `{"source_id":"00000000-0000-4000-8000-000000000001","occurred_from":"2026-10-01T10:00:00+03:00","occurred_to":"2026-10-02T10:00:00+03:00","pipeline_version":2}`

func replayRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/quarantine/replays/dry-run", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestReplayDryRunUsesClaimTenantAndReturnsReadOnlyPlan(t *testing.T) {
	planner := &replayPlanner{plan: ReplayDryRunPlan{CandidateCount: 42}}
	publisher, audit := &replayPublisher{}, &replayAudit{}
	h := BrowserHandler{
		Auth:          browserAuth{TenantID: tenantA, Scopes: map[string]bool{ReplayScope: true}},
		ReplayPlanner: planner, ReplayJobs: publisher, Audit: audit,
	}.Routes()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, replayRequest(validReplayJSON))
	if w.Code != http.StatusOK || planner.calls != 1 || planner.tenant != tenantA {
		t.Fatalf("status=%d calls=%d tenant=%q body=%s", w.Code, planner.calls, planner.tenant, w.Body.String())
	}
	if planner.request.PipelineVersion != 2 || planner.request.OccurredFrom.Location() != time.UTC {
		t.Fatalf("request=%+v", planner.request)
	}
	body := w.Body.String()
	for _, expected := range []string{`"dry_run":true`, `"candidate_count":42`, `"executable":true`, `"occurred_from":"2026-10-01T07:00:00Z"`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %s in %s", expected, body)
		}
	}
	if len(publisher.jobs) != 0 || len(audit.events) != 0 {
		t.Fatalf("dry-run caused side effects: jobs=%d audit=%d", len(publisher.jobs), len(audit.events))
	}
}

func TestReplayDryRunRequiresAuthenticationTenantAndScope(t *testing.T) {
	planner := &replayPlanner{}
	tests := []struct {
		name string
		auth BrowserAuthenticator
		want int
	}{
		{"unauthenticated", browserAuthError{}, http.StatusUnauthorized},
		{"invalid tenant", browserAuth{TenantID: "tenant", Scopes: map[string]bool{ReplayScope: true}}, http.StatusForbidden},
		{"missing scope", browserAuth{TenantID: tenantA, Scopes: map[string]bool{}}, http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			BrowserHandler{Auth: tt.auth, ReplayPlanner: planner}.Routes().ServeHTTP(w, replayRequest(validReplayJSON))
			if w.Code != tt.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	if planner.calls != 0 {
		t.Fatalf("planner called %d times", planner.calls)
	}
}

type browserAuthError struct{}

func (browserAuthError) Authenticate(*http.Request) (BrowserPrincipal, error) {
	return BrowserPrincipal{}, ErrUnauthenticated
}

func TestReplayDryRunStrictInputValidation(t *testing.T) {
	planner := &replayPlanner{}
	h := BrowserHandler{Auth: browserAuth{TenantID: tenantA, Scopes: map[string]bool{ReplayScope: true}}, ReplayPlanner: planner}.Routes()
	tests := []string{
		`{}`,
		strings.TrimSuffix(validReplayJSON, "}") + `,"tenant_id":"` + tenantB + `"}`,
		validReplayJSON + `{}`,
		strings.Replace(validReplayJSON, `"pipeline_version":2`, `"pipeline_version":0`, 1),
		strings.Replace(validReplayJSON, `2026-10-02T10:00:00+03:00`, `2026-12-02T10:00:00+03:00`, 1),
		strings.Replace(validReplayJSON, `2026-10-01T10:00:00+03:00`, `not-a-time`, 1),
		strings.Replace(validReplayJSON, `00000000-0000-4000-8000-000000000001`, `not-a-uuid`, 1),
	}
	for i, body := range tests {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, replayRequest(body))
		if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), `"code":"INVALID_REQUEST"`) {
			t.Fatalf("case %d: status=%d body=%s", i, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	r := replayRequest(validReplayJSON)
	r.Header.Set("Content-Type", "text/plain")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("content type status=%d", w.Code)
	}
	if planner.calls != 0 {
		t.Fatalf("planner called %d times", planner.calls)
	}
}

func TestReplayDryRunBoundedAndBackendFailureResponses(t *testing.T) {
	planner := &replayPlanner{plan: ReplayDryRunPlan{CandidateCount: maxReplayCandidates, CandidateCountIsLowerBound: true}}
	h := BrowserHandler{Auth: browserAuth{TenantID: tenantA, Scopes: map[string]bool{ReplayScope: true}}, ReplayPlanner: planner}.Routes()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, replayRequest(validReplayJSON))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"candidate_count_is_lower_bound":true`) ||
		!strings.Contains(w.Body.String(), `"executable":false`) || !strings.Contains(w.Body.String(), `"reason":"candidate_limit_exceeded"`) {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	planner.err = errors.New("storage secret")
	r := replayRequest(validReplayJSON)
	r.Header.Set("X-Request-ID", "replay-request-42")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("X-Request-ID") != "replay-request-42" ||
		strings.Contains(w.Body.String(), "storage secret") || !strings.Contains(w.Body.String(), `"code":"REPLAY_UNAVAILABLE"`) {
		t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body.String())
	}
}

func TestReplayDryRunFailsClosedWhenNotConfigured(t *testing.T) {
	w := httptest.NewRecorder()
	BrowserHandler{}.Routes().ServeHTTP(w, replayRequest(validReplayJSON))
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("X-Request-ID") == "" {
		t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body.String())
	}
}

func publishHandler(auth BrowserAuthenticator, planner *replayPlanner, publisher *replayPublisher, audit *replayAudit) http.Handler {
	return BrowserHandler{
		Auth: auth, ReplayPlanner: planner, ReplayJobs: publisher, Audit: audit,
		Now: func() time.Time { return time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC) },
	}.Routes()
}

func publishRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/quarantine/replays", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Request-ID", "request-42")
	return r
}

func TestReplayPublishRequiresAuthenticationAndSeparateScope(t *testing.T) {
	planner := &replayPlanner{plan: ReplayDryRunPlan{CandidateCount: 2}}
	publisher, audit := &replayPublisher{}, &replayAudit{}
	tests := []struct {
		name string
		auth BrowserAuthenticator
		want int
	}{
		{"unauthenticated", browserAuthError{}, http.StatusUnauthorized},
		{"invalid tenant", browserAuth{TenantID: "tenant", Scopes: map[string]bool{ReplayPublishScope: true}}, http.StatusForbidden},
		{"dry-run scope only", browserAuth{TenantID: tenantA, Scopes: map[string]bool{ReplayScope: true}}, http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			publishHandler(tt.auth, planner, publisher, audit).ServeHTTP(w, publishRequest(validReplayJSON))
			if w.Code != tt.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	if planner.calls != 0 || len(publisher.jobs) != 0 || len(audit.events) != 0 {
		t.Fatal("unauthorized request reached replay dependencies")
	}
}

func TestReplayPublishMirrorsOnlyValidatedDeniedAndOutcomes(t *testing.T) {
	planner := &replayPlanner{plan: ReplayDryRunPlan{CandidateCount: 2}}
	publisher, audit, mirror := &replayPublisher{}, &replayAudit{}, &replayMirror{}
	auth := browserAuth{TenantID: tenantA, Subject: "operator", ClientID: "portal", ACR: "mfa", Scopes: map[string]bool{}}
	h := BrowserHandler{Auth: auth, ReplayPlanner: planner, ReplayJobs: publisher, Audit: audit, AuditMirror: mirror, Now: func() time.Time { return time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC) }}.Routes()
	h.ServeHTTP(httptest.NewRecorder(), publishRequest(validReplayJSON))
	if len(mirror.events) != 1 || mirror.events[0].Action != "auth.denied" || mirror.events[0].TenantID != tenantA || mirror.events[0].ClientID != "portal" || mirror.events[0].ACR != "mfa" {
		t.Fatalf("mirror denied=%+v", mirror.events)
	}

	auth.Scopes[ReplayPublishScope] = true
	w := httptest.NewRecorder()
	h.ServeHTTP(w, publishRequest(validReplayJSON))
	if w.Code != http.StatusAccepted || len(mirror.events) != 2 || mirror.events[1].Outcome != "success" {
		t.Fatalf("status=%d mirror=%+v", w.Code, mirror.events)
	}
}

func TestReplayPublishRejectsIdentityTamperingAndUsesClaimTenant(t *testing.T) {
	planner := &replayPlanner{plan: ReplayDryRunPlan{CandidateCount: 2}}
	publisher, audit := &replayPublisher{}, &replayAudit{}
	auth := browserAuth{TenantID: tenantA, Subject: "operator", Scopes: map[string]bool{ReplayPublishScope: true}}
	h := publishHandler(auth, planner, publisher, audit)

	tampered := strings.TrimSuffix(validReplayJSON, "}") + `,"tenant_id":"` + tenantB + `"}`
	w := httptest.NewRecorder()
	h.ServeHTTP(w, publishRequest(tampered))
	if w.Code != http.StatusBadRequest || planner.calls != 0 || len(publisher.jobs) != 0 {
		t.Fatalf("tampered status=%d plan=%d jobs=%d", w.Code, planner.calls, len(publisher.jobs))
	}

	w = httptest.NewRecorder()
	claimBound := publishRequest(validReplayJSON)
	claimBound.URL.RawQuery = "tenant_id=" + tenantB
	h.ServeHTTP(w, claimBound)
	if w.Code != http.StatusAccepted || planner.tenant != tenantA || len(publisher.jobs) != 1 {
		t.Fatalf("status=%d tenant=%q jobs=%d body=%s", w.Code, planner.tenant, len(publisher.jobs), w.Body.String())
	}
	job := publisher.jobs[0]
	if job.TenantID != tenantA || job.SourceID != planner.request.SourceID || job.CandidateCount != 2 || job.JobID == "" {
		t.Fatalf("job=%+v", job)
	}
	if len(audit.events) != 2 || audit.events[0].Phase != "prepared" || audit.events[1].Phase != "committed" ||
		audit.events[0].TenantID != tenantA || audit.events[0].Subject != "operator" || audit.events[0].RequestID != "request-42" {
		t.Fatalf("audit=%+v", audit.events)
	}
}

func TestReplayPublishRefusesUnboundedPlan(t *testing.T) {
	planner := &replayPlanner{plan: ReplayDryRunPlan{CandidateCount: maxReplayCandidates, CandidateCountIsLowerBound: true}}
	publisher, audit := &replayPublisher{}, &replayAudit{}
	auth := browserAuth{TenantID: tenantA, Scopes: map[string]bool{ReplayPublishScope: true}}
	w := httptest.NewRecorder()
	publishHandler(auth, planner, publisher, audit).ServeHTTP(w, publishRequest(validReplayJSON))
	if w.Code != http.StatusUnprocessableEntity || len(publisher.jobs) != 0 || len(audit.events) != 0 ||
		!strings.Contains(w.Body.String(), `"code":"REPLAY_SCOPE_TOO_LARGE"`) {
		t.Fatalf("status=%d jobs=%d audit=%d body=%s", w.Code, len(publisher.jobs), len(audit.events), w.Body.String())
	}
}

func TestReplayPublishFailsClosedOnAuditFailure(t *testing.T) {
	planner := &replayPlanner{plan: ReplayDryRunPlan{CandidateCount: 2}}
	publisher, audit := &replayPublisher{}, &replayAudit{err: errors.New("audit down")}
	auth := browserAuth{TenantID: tenantA, Scopes: map[string]bool{ReplayPublishScope: true}}
	w := httptest.NewRecorder()
	publishHandler(auth, planner, publisher, audit).ServeHTTP(w, publishRequest(validReplayJSON))
	if w.Code != http.StatusServiceUnavailable || len(publisher.jobs) != 0 ||
		!strings.Contains(w.Body.String(), `"code":"AUDIT_UNAVAILABLE"`) {
		t.Fatalf("status=%d jobs=%d body=%s", w.Code, len(publisher.jobs), w.Body.String())
	}

	w = httptest.NewRecorder()
	BrowserHandler{Auth: auth, ReplayPlanner: planner, ReplayJobs: publisher}.Routes().ServeHTTP(w, publishRequest(validReplayJSON))
	if w.Code != http.StatusServiceUnavailable || len(publisher.jobs) != 0 {
		t.Fatalf("missing audit status=%d jobs=%d", w.Code, len(publisher.jobs))
	}
}

func TestReplayPublishReportsPublisherErrorAndAuditsAbort(t *testing.T) {
	planner := &replayPlanner{plan: ReplayDryRunPlan{CandidateCount: 2}}
	publisher, audit := &replayPublisher{err: errors.New("broker down")}, &replayAudit{}
	auth := browserAuth{TenantID: tenantA, Scopes: map[string]bool{ReplayPublishScope: true}}
	w := httptest.NewRecorder()
	publishHandler(auth, planner, publisher, audit).ServeHTTP(w, publishRequest(validReplayJSON))
	if w.Code != http.StatusBadGateway || len(publisher.jobs) != 1 || len(audit.events) != 2 ||
		audit.events[0].Phase != "prepared" || audit.events[1].Phase != "aborted" ||
		!strings.Contains(w.Body.String(), `"code":"REPLAY_PUBLISH_FAILED"`) || strings.Contains(w.Body.String(), "broker down") {
		t.Fatalf("status=%d jobs=%d audit=%+v body=%s", w.Code, len(publisher.jobs), audit.events, w.Body.String())
	}
}
