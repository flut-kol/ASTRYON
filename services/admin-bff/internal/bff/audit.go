package bff

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/astryon/audit"
)

// AuditSink is deliberately the small subset used by the request path. The production implementation is the
// bounded PostgreSQL writer from libs/audit; tests can use an in-memory collector without a database.
type AuditSink interface {
	Submit(audit.Event) error
	Stats() audit.Stats
}

type auditMetrics struct {
	loginDeniedUnrecordable atomic.Uint64
	sessionUnrecordable     atomic.Uint64
}

func (r Record) auditClaims() audit.Claims {
	return audit.Claims{TenantID: r.Tenant, Subject: r.Sub, ClientID: r.Client, ACR: r.ACR}
}

func (s *Server) auditSession(r Record, action string, outcome audit.Outcome) {
	if s.audit == nil {
		s.auditMetrics.sessionUnrecordable.Add(1)
		return
	}
	e := audit.NewEvent(r.auditClaims(), audit.CategorySecurity, action, outcome, audit.Resource{Type: "session"})
	if err := s.audit.Submit(e); err != nil {
		// The writer's bounded counters and OnDrop hook expose queue/storage loss. Invalid records are also visible
		// here without logging any actor, tenant, session, token or other identifying value.
		s.o.Logger.Printf("audit submit failed: action=%s reason_type=%T", action, err)
	}
}

func (s *Server) loginDeniedBeforeClaims() {
	s.auditMetrics.loginDeniedUnrecordable.Add(1)
}

// MetricsHandler exposes bounded audit-writer counters and explicit gaps where no validated tenant existed.
// It is intended for the dedicated BFF_METRICS_ADDR listener, not the public BFF listener.
func (s *Server) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		var stats audit.Stats
		enabled := 0
		if s.audit != nil {
			stats = s.audit.Stats()
			enabled = 1
		}
		fmt.Fprintf(w, "# TYPE astryon_admin_bff_audit_enabled gauge\nastryon_admin_bff_audit_enabled %d\n", enabled)
		for _, metric := range []struct {
			name  string
			value uint64
		}{
			{"queued", stats.Queued}, {"written", stats.Written}, {"dropped", stats.Dropped}, {"rejected", stats.Rejected},
		} {
			fmt.Fprintf(w, "# TYPE astryon_admin_bff_audit_%s_total counter\nastryon_admin_bff_audit_%s_total %d\n", metric.name, metric.name, metric.value)
		}
		fmt.Fprint(w, "# HELP astryon_admin_bff_audit_login_denied_unrecordable_total Login failures not persisted because no validated tenant claim existed\n")
		fmt.Fprint(w, "# TYPE astryon_admin_bff_audit_login_denied_unrecordable_total counter\n")
		fmt.Fprintf(w, "astryon_admin_bff_audit_login_denied_unrecordable_total %d\n", s.auditMetrics.loginDeniedUnrecordable.Load())
		fmt.Fprint(w, "# HELP astryon_admin_bff_audit_session_unrecordable_total Session events not persisted because the audit writer was disabled\n")
		fmt.Fprint(w, "# TYPE astryon_admin_bff_audit_session_unrecordable_total counter\n")
		fmt.Fprintf(w, "astryon_admin_bff_audit_session_unrecordable_total %d\n", s.auditMetrics.sessionUnrecordable.Load())
	})
}
