package api

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/astryon/audit"
)

type auditCollector struct {
	mu     sync.Mutex
	events []audit.Event
	stats  audit.Stats
}

func (c *auditCollector) Submit(e audit.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
	c.stats.Queued++
	return nil
}

func (c *auditCollector) Stats() audit.Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *auditCollector) snapshot() []audit.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]audit.Event(nil), c.events...)
}

func auditPrincipal(scopes ...string) Principal {
	return Principal{TenantID: tenant, Subject: "aas-user", ClientID: "portal", ACR: "urn:astryon:acr:mfa", AMR: []string{"pwd", "otp"}, Scopes: scopes}
}

func TestUploadAuditUsesOnlyValidatedPrincipalAndResourceID(t *testing.T) {
	c := &auditCollector{}
	s, _, _, _ := newServer(auditPrincipal(ScopeUploadsWrite))
	s.Audit = c
	r := httptest.NewRequest("POST", "/v1/files/uploads", strings.NewReader(okBody))
	r.Header.Set("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	r.Header.Set("X-Request-ID", "request-1")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}

	events := c.snapshot()
	if len(events) != 1 {
		t.Fatalf("events = %+v", events)
	}
	e := events[0]
	if e.Action != "upload.create" || e.Outcome != audit.OutcomeSuccess || e.Category != audit.CategoryAudit ||
		e.TenantID != tenant || e.Subject != "aas-user" || e.ClientID != "portal" || e.ACR != "urn:astryon:acr:mfa" ||
		e.Resource != (audit.Resource{Type: "upload", ID: fileID}) || e.TraceID != "0123456789abcdef0123456789abcdef" || e.RequestID != "request-1" {
		t.Fatalf("event = %+v", e)
	}
	if len(e.Details) != 0 {
		t.Fatalf("upload metadata leaked to details: %+v", e.Details)
	}
	if _, err := e.Normalize(time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatalf("event does not satisfy audit contract: %v", err)
	}
}

func TestCompleteAuditAndAuthorizationDenials(t *testing.T) {
	c := &auditCollector{}
	s, fs, _, _ := prepared(t, okBody)
	s.Auth = auth{auditPrincipal(ScopeUploadsWrite)}
	s.Audit = c
	s.Objs = objs{body: ""}
	fs.f.Size = -1
	w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/complete", completeBody)
	if w.Code != 200 {
		t.Fatalf("complete: %d %s", w.Code, w.Body.String())
	}
	events := c.snapshot()
	if len(events) != 1 || events[0].Action != "upload.complete" || events[0].Resource.ID != fileID || len(events[0].Details) != 0 {
		t.Fatalf("events = %+v", events)
	}

	denied, _, _, _ := newServer(auditPrincipal("some:other:scope"))
	denied.Audit = c
	_ = call(denied.Handler(), "/v1/files/uploads", okBody)
	events = c.snapshot()
	last := events[len(events)-1]
	if last.Action != "auth.denied" || last.Category != audit.CategorySecurity || last.Outcome != audit.OutcomeDenied ||
		last.TenantID != tenant || last.Details["reason"] != "missing_scope" {
		t.Fatalf("denied event = %+v", last)
	}

	before := len(events)
	unauthenticated, _, _, _ := newServer(Principal{})
	unauthenticated.Audit = c
	_ = call(unauthenticated.Handler(), "/v1/files/uploads", okBody)
	if got := len(c.snapshot()); got != before {
		t.Fatalf("invalid token produced tenant audit event: before=%d after=%d", before, got)
	}
}

func TestAuditStatsAreExposedWithoutActorOrMetadataLabels(t *testing.T) {
	c := &auditCollector{stats: audit.Stats{Queued: 4, Written: 3, Dropped: 1, Rejected: 2}}
	s, _, _, _ := newServer(auditPrincipal(ScopeUploadsWrite))
	s.Audit = c
	var out bytes.Buffer
	s.WritePrometheus(&out)
	for _, line := range []string{
		"astryon_file_api_audit_queued_total 4",
		"astryon_file_api_audit_written_total 3",
		"astryon_file_api_audit_dropped_total 1",
		"astryon_file_api_audit_rejected_total 2",
	} {
		if !strings.Contains(out.String(), line) {
			t.Fatalf("missing %q in:\n%s", line, out.String())
		}
	}
	if strings.Contains(out.String(), tenant) || strings.Contains(out.String(), "aas-user") {
		t.Fatalf("identity leaked into metric labels: %s", out.String())
	}
}
