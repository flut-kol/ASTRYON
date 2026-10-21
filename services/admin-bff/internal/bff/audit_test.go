package bff

import (
	"io"
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
	e, err := e.Normalize(time.Now())
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.stats.Rejected++
		return err
	}
	c.events = append(c.events, e)
	c.stats.Queued++
	c.stats.Written++
	return nil
}

func (c *auditCollector) Stats() audit.Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *auditCollector) take() []audit.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]audit.Event(nil), c.events...)
	c.events = nil
	return out
}

func setupAudited(t *testing.T) (*env, *auditCollector) {
	t.Helper()
	c := &auditCollector{}
	e := setup(t, func(o *Options) { o.Audit = c })
	return e, c
}

func TestAuditLoginLogoutUsesValidatedClaimsWithoutSessionSecrets(t *testing.T) {
	e, c := setupAudited(t)
	e.login(t)
	events := c.take()
	if len(events) != 1 {
		t.Fatalf("login audit events = %d", len(events))
	}
	login := events[0]
	if login.Action != "session.login" || login.Outcome != audit.OutcomeSuccess || login.Category != audit.CategorySecurity {
		t.Fatalf("login event = %+v", login)
	}
	if login.TenantID != "11111111-1111-4111-8111-111111111111" || login.Subject != "user-1" || login.ClientID != "astryon-portal" || login.ACR != "mfa" {
		t.Fatalf("login claims = %+v", login)
	}
	if login.Resource.ID != "" || len(login.Details) != 0 {
		t.Fatalf("session identifier or details leaked: %+v", login)
	}
	res, _ := e.do(t, "POST", "/auth/logout", map[string]string{CSRFHeader: CSRFValue})
	if res.StatusCode != 200 {
		t.Fatalf("logout = %d", res.StatusCode)
	}
	events = c.take()
	if len(events) != 1 || events[0].Action != "session.logout" || events[0].Outcome != audit.OutcomeSuccess {
		t.Fatalf("logout events = %+v", events)
	}
}

func TestAuditExpiryAndBackchannelLogout(t *testing.T) {
	e, c := setupAudited(t)
	now := time.Now()
	e.srv.oc.now = func() time.Time { return now }
	e.srv.o.SessionMaxAge = time.Hour
	e.login(t)
	c.take()
	now = now.Add(2 * time.Hour)
	if res, _ := e.do(t, "GET", "/api/query/x", nil); res.StatusCode != 401 {
		t.Fatalf("expired request = %d", res.StatusCode)
	}
	events := c.take()
	if len(events) != 1 || events[0].Action != "session.expired" {
		t.Fatalf("expiry events = %+v", events)
	}

	// Reset the OIDC clock because logout-token iat validation uses it too.
	e.srv.oc.now = time.Now
	e.login(t)
	c.take()
	if status := e.bcl(t, e.aas.logoutToken(t, nil)); status != 200 {
		t.Fatalf("back-channel logout = %d", status)
	}
	events = c.take()
	if len(events) != 1 || events[0].Action != "session.backchannel_logout" || events[0].TenantID == "" {
		t.Fatalf("back-channel events = %+v", events)
	}
}

func TestLoginDenialWithoutValidatedTenantIsMetricOnly(t *testing.T) {
	e, c := setupAudited(t)
	if res, _ := e.do(t, "GET", "/auth/callback?code=x&state=y", nil); res.StatusCode != 400 {
		t.Fatalf("forged callback = %d", res.StatusCode)
	}
	if events := c.take(); len(events) != 0 {
		t.Fatalf("synthetic-tenant event emitted: %+v", events)
	}
	rr := httptest.NewRecorder()
	e.srv.MetricsHandler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rr.Result().Body)
	if !strings.Contains(string(body), "astryon_admin_bff_audit_login_denied_unrecordable_total 1") {
		t.Fatalf("missing gap metric:\n%s", body)
	}
}

func TestLoginRejectsMissingTenantWithoutSyntheticAudit(t *testing.T) {
	e, c := setupAudited(t)
	e.aas.tenant = ""
	res, _ := e.do(t, "GET", "/auth/login", nil)
	if res.StatusCode != 401 {
		t.Fatalf("login without tenant_id = %d", res.StatusCode)
	}
	if events := c.take(); len(events) != 0 {
		t.Fatalf("synthetic-tenant event emitted: %+v", events)
	}
	rr := httptest.NewRecorder()
	e.srv.MetricsHandler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rr.Body.String(), "astryon_admin_bff_audit_login_denied_unrecordable_total 1") {
		t.Fatalf("missing gap metric:\n%s", rr.Body.String())
	}
}

func TestAuditMetricsExposeWriterStats(t *testing.T) {
	e, c := setupAudited(t)
	c.stats = audit.Stats{Queued: 7, Written: 5, Dropped: 2, Rejected: 1}
	rr := httptest.NewRecorder()
	e.srv.MetricsHandler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{
		"astryon_admin_bff_audit_enabled 1",
		"astryon_admin_bff_audit_queued_total 7",
		"astryon_admin_bff_audit_written_total 5",
		"astryon_admin_bff_audit_dropped_total 2",
		"astryon_admin_bff_audit_rejected_total 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q:\n%s", want, body)
		}
	}
}
