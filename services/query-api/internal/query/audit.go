package query

import (
	"github.com/astryon/audit"
)

// AuditSink is the non-blocking subset of audit.Writer used on the request path.
type AuditSink interface {
	Submit(audit.Event) error
}

func (s *Server) auditDenied(p Principal, route, requestID string) {
	if s.Audit == nil {
		Stats.auditGap("disabled")
		return
	}
	e := audit.NewEvent(audit.Claims{
		TenantID: p.TenantID,
		Subject:  p.Subject,
		ClientID: p.ClientID,
		ACR:      p.ACR,
	}, audit.CategorySecurity, "auth.denied", audit.OutcomeDenied, audit.Resource{Type: "query_api"}).
		WithTrace("", requestID).
		WithDetails(map[string]string{"reason": "missing_scope", "route": route})
	// AMR deliberately is not copied: audit_log stores ACR evidence only and details must remain minimal.
	if err := s.Audit.Submit(e); err != nil {
		Stats.auditGap("submit_error")
	}
}
