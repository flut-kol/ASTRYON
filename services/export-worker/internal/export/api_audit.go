package export

import (
	"context"
	"time"

	"github.com/astryon/audit"
)

// AuditEventWriter is the subset of audit.Writer used by the HTTP API. It is
// deliberately small so required-audit behavior can be tested without a database.
type AuditEventWriter interface {
	Submit(audit.Event) error
	WriteSync(context.Context, audit.Event) error
}

// APIAudit records API state and data-access actions separately from the
// execution-outcome AuditSink used by Runner. RequiredCreate makes export.create
// durable before a job or quota reservation is created.
type APIAudit struct {
	Writer         AuditEventWriter
	RequiredCreate bool
	SyncTimeout    time.Duration
}

func auditClaims(p Principal) audit.Claims {
	return audit.Claims{TenantID: p.TenantID, Subject: p.Subject, ClientID: p.ClientID, ACR: p.ACR}
}

func apiAuditEvent(p Principal, action, exportID, requestID string, outcome audit.Outcome, details map[string]string) audit.Event {
	return audit.NewEvent(auditClaims(p), audit.CategoryAudit, action, outcome,
		audit.Resource{Type: "export", ID: exportID}).WithTrace("", requestID).WithDetails(details)
}

// BeforeExportCreate performs the required precondition write. Best-effort
// mode deliberately waits until the job has actually been created.
func (a *APIAudit) BeforeExportCreate(ctx context.Context, p Principal, exportID, requestID string) error {
	if a == nil || a.Writer == nil || !a.RequiredCreate {
		return nil
	}
	// This records only that the required audit intent was prepared; it does
	// not claim that export.create itself completed successfully.
	e := apiAuditEvent(p, "export.create.prepare", exportID, requestID, audit.OutcomeSuccess,
		map[string]string{"phase": "prepared"})
	to := a.SyncTimeout
	if to <= 0 {
		to = 5 * time.Second
	}
	writeCtx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	return a.Writer.WriteSync(writeCtx, e)
}

// ExportCreated queues the completed action after a successful job creation.
// In required mode it is the committed half of the prepared intent; in
// best-effort mode it is the sole export.create record.
func (a *APIAudit) ExportCreated(p Principal, exportID, requestID string) {
	if a == nil || a.Writer == nil {
		return
	}
	var details map[string]string
	if a.RequiredCreate {
		details = map[string]string{"phase": "committed"}
	}
	_ = a.Writer.Submit(apiAuditEvent(p, "export.create", exportID, requestID, audit.OutcomeSuccess, details))
}

// ExportCreateFailed resolves a durable prepared intent when quota reservation
// or job creation fails. Best-effort mode has no prepared intent to resolve.
func (a *APIAudit) ExportCreateFailed(p Principal, exportID, requestID string) {
	if a == nil || a.Writer == nil || !a.RequiredCreate {
		return
	}
	_ = a.Writer.Submit(apiAuditEvent(p, "export.create", exportID, requestID, audit.OutcomeFailure,
		map[string]string{"phase": "failed"}))
}

// ExportDownloadLink is ordinary best-effort audit: issuing a short-lived URL
// is not blocked by a full async queue. No URL, object key, spec or payload is recorded.
func (a *APIAudit) ExportDownloadLink(p Principal, exportID, requestID string) {
	if a == nil || a.Writer == nil {
		return
	}
	_ = a.Writer.Submit(apiAuditEvent(p, "export.download_link", exportID, requestID, audit.OutcomeSuccess, nil))
}
