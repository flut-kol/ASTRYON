package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/astryon/audit"
)

type collectingAudit struct {
	mu     sync.Mutex
	events []audit.Event
	stats  audit.Stats
}

func (c *collectingAudit) Submit(e audit.Event) error {
	n, err := e.Normalize(time.Now())
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.stats.Rejected++
		return err
	}
	c.events = append(c.events, n)
	c.stats.Queued++
	return nil
}

func (c *collectingAudit) Stats() audit.Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *collectingAudit) snapshot() []audit.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]audit.Event(nil), c.events...)
}

func auditedServer(scopes []string, a *collectingAudit) *httptest.Server {
	cfg := Config{MaxBodyBytes: 4096, MaxBatchBytes: 16384, MaxBatchEvents: 3}
	authn := DevStaticAuthenticator{Token: token, Principal: Principal{
		TenantID: tenantA, Subject: "aas-subject", ClientID: "client", ACR: "mfa", AMR: []string{"pwd", "otp"}, Scopes: scopes,
	}}
	return httptest.NewServer(NewServer(cfg, authn, &FakePublisher{}, a).Handler())
}

func TestAuditDeniedAndTenantMismatchUseValidatedPrincipal(t *testing.T) {
	a := &collectingAudit{}
	ts := auditedServer([]string{"astryon:events:read"}, a)
	resp, _ := do(t, ts.URL+"/v1/events", evt(""), map[string]string{"X-Request-ID": "request-one"})
	ts.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("missing scope status=%d", resp.StatusCode)
	}

	ts = auditedServer(write, a)
	resp, _ = do(t, ts.URL+"/v1/events", evt(`,"tenant_id":"`+tenantB+`"`), map[string]string{"X-Request-ID": "request-two"})
	ts.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("mismatch status=%d", resp.StatusCode)
	}

	events := a.snapshot()
	if len(events) != 2 {
		t.Fatalf("events=%d", len(events))
	}
	for _, e := range events {
		if e.TenantID != tenantA || e.Subject != "aas-subject" || e.ClientID != "client" || e.ACR != "mfa" || e.Action != "auth.denied" || e.Outcome != audit.OutcomeDenied {
			t.Fatalf("event not claim-derived: %+v", e)
		}
		if strings.Contains(e.Resource.ID, tenantB) {
			t.Fatal("foreign body tenant leaked into audit resource")
		}
	}
	if events[0].Details["reason"] != "missing_scope" || events[1].Details["reason"] != "tenant_mismatch" {
		t.Fatalf("reasons: %+v %+v", events[0].Details, events[1].Details)
	}
}

func TestInvalidTokenIsNotAssignedTenantForAudit(t *testing.T) {
	a := &collectingAudit{}
	ts := auditedServer(write, a)
	defer ts.Close()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/events", strings.NewReader(evt("")))
	req.Header.Set("Authorization", "Bearer invalid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || len(a.snapshot()) != 0 {
		t.Fatalf("status=%d events=%d", resp.StatusCode, len(a.snapshot()))
	}
	m, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Body.Close()
	b, _ := io.ReadAll(m.Body)
	if !strings.Contains(string(b), "astryon_gateway_audit_token_invalid_unrecordable_total 1\n") {
		t.Fatalf("missing unrecordable metric:\n%s", b)
	}
}

func TestAuditConfigBounds(t *testing.T) {
	env := map[string]string{
		"ASTRYON_AUDIT_PG_URL":         "postgres://audit:secret@db/astryon",
		"ASTRYON_AUDIT_QUEUE_SIZE":     "8",
		"ASTRYON_AUDIT_BATCH_SIZE":     "4",
		"ASTRYON_AUDIT_FLUSH_INTERVAL": "20ms",
		"ASTRYON_AUDIT_WRITE_TIMEOUT":  "2s",
		"ASTRYON_AUDIT_RETRIES":        "0",
		"ASTRYON_AUDIT_DRAIN_TIMEOUT":  "3s",
		"ASTRYON_AUDIT_PG_MAX_CONNS":   "2",
	}
	cfg, err := LoadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AuditQueueSize != 8 || cfg.AuditBatchSize != 4 || cfg.AuditFlushInterval != 20*time.Millisecond || cfg.AuditRetries != 0 || cfg.AuditPGMaxConns != 2 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	for k, v := range map[string]string{
		"ASTRYON_AUDIT_QUEUE_SIZE": "0", "ASTRYON_AUDIT_FLUSH_INTERVAL": "never", "ASTRYON_AUDIT_RETRIES": "-1", "ASTRYON_AUDIT_PG_MAX_CONNS": "33",
	} {
		bad := map[string]string{k: v}
		if _, err := LoadConfig(func(k string) string { return bad[k] }); err == nil {
			t.Errorf("%s=%s accepted", k, v)
		}
	}
}
