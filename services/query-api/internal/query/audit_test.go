package query

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/astryon/audit"
)

type captureAudit struct {
	events []audit.Event
	err    error
}

func (c *captureAudit) Submit(e audit.Event) error {
	c.events = append(c.events, e)
	return c.err
}

func auditRequest(t *testing.T, s *Server, token, body string) int {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/query/events?private=query", strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("X-Request-ID", "req-123")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w.Code
}

func TestAuditDeniedUsesOnlyTrustedPrincipalMetadata(t *testing.T) {
	sink := &captureAudit{}
	p := Principal{TenantID: tenant, Subject: "subject-1", ClientID: "client-1", ACR: "mfa",
		AMR: []string{"pwd", "otp"}}
	s := &Server{Auth: DevStaticAuthenticator{Token: "good", Principal: p}, Audit: sink}
	body := `{"password":"must-not-appear","query":"also-private"}`
	if code := auditRequest(t, s, "good", body); code != http.StatusForbidden {
		t.Fatalf("status = %d", code)
	}
	if len(sink.events) != 1 {
		t.Fatalf("events = %d", len(sink.events))
	}
	e := sink.events[0]
	if e.TenantID != tenant || e.Subject != "subject-1" || e.ClientID != "client-1" || e.ACR != "mfa" ||
		e.Action != "auth.denied" || e.Category != audit.CategorySecurity || e.Outcome != audit.OutcomeDenied ||
		e.Resource.Type != "query_api" || e.RequestID != "req-123" {
		t.Fatalf("event = %+v", e)
	}
	if len(e.Details) != 2 || e.Details["reason"] != "missing_scope" || e.Details["route"] != "events" {
		t.Fatalf("details = %#v", e.Details)
	}
	dump := strings.ToLower(e.Subject + e.ClientID + e.ACR + e.Resource.ID + e.TraceID + e.RequestID + e.Details["reason"] + e.Details["route"])
	for _, forbidden := range []string{"password", "must-not-appear", "also-private", "pwd", "otp", "private=query"} {
		if strings.Contains(dump, forbidden) {
			t.Fatalf("audit event leaked %q: %+v", forbidden, e)
		}
	}
}

func TestAuditSkipsUntrustedTenantAnd401(t *testing.T) {
	sink := &captureAudit{}
	s := &Server{Auth: DevStaticAuthenticator{Token: "good", Principal: Principal{TenantID: "not-a-uuid", Subject: "s"}}, Audit: sink}
	unauthBefore := auditGaps[indexOf(auditGapReasons, "unauthenticated_no_tenant")].Load()
	invalidBefore := auditGaps[indexOf(auditGapReasons, "invalid_tenant")].Load()
	if code := auditRequest(t, s, "bad", `{}`); code != http.StatusUnauthorized {
		t.Fatalf("401 status = %d", code)
	}
	if code := auditRequest(t, s, "good", `{}`); code != http.StatusForbidden {
		t.Fatalf("403 status = %d", code)
	}
	if len(sink.events) != 0 {
		t.Fatalf("untrusted requests produced audit events: %+v", sink.events)
	}
	if auditGaps[indexOf(auditGapReasons, "unauthenticated_no_tenant")].Load() != unauthBefore+1 ||
		auditGaps[indexOf(auditGapReasons, "invalid_tenant")].Load() != invalidBefore+1 {
		t.Fatal("expected bounded audit gaps to increment")
	}
}

func TestAuditSubmitFailureIsVisible(t *testing.T) {
	sink := &captureAudit{err: errors.New("queue unavailable")}
	s := &Server{Auth: DevStaticAuthenticator{Token: "good", Principal: Principal{TenantID: tenant, Subject: "s"}}, Audit: sink}
	before := auditGaps[indexOf(auditGapReasons, "submit_error")].Load()
	if code := auditRequest(t, s, "good", `{}`); code != http.StatusForbidden {
		t.Fatalf("status = %d", code)
	}
	if auditGaps[indexOf(auditGapReasons, "submit_error")].Load() != before+1 {
		t.Fatal("submit failure not counted")
	}
}
