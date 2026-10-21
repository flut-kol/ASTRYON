package replay

import (
	"context"
	"fmt"
	"log"

	commonaudit "github.com/astryon/audit"
)

// AuditMirrorSink keeps the Kafka audit.events.v1 stream authoritative for replay's two-phase
// protocol while optionally mirroring queryable records into audit_log. Mirror failures never
// change Kafka required-audit semantics or the committed state transition.
type AuditMirrorSink struct {
	Primary AuditSink
	Writer  *commonaudit.Writer
	Metrics *Metrics
}

func (s *AuditMirrorSink) Record(ctx context.Context, e AuditEvent) error {
	if s.Primary != nil {
		if err := s.Primary.Record(ctx, e); err != nil {
			return err
		}
	}
	if s.Writer != nil {
		if err := s.Writer.Submit(toLibraryEvent(e)); err != nil {
			if s.Metrics != nil {
				s.Metrics.AuditMirrorDropped.Add(1)
			}
			log.Printf("replay audit: audit_log mirror dropped: %v", err)
		}
	}
	return nil
}

func (s *AuditMirrorSink) RecordDenied(ctx context.Context, e DeniedEvent) error {
	if ds, ok := s.Primary.(DeniedSink); ok {
		if err := ds.RecordDenied(ctx, e); err != nil {
			return err
		}
	}
	// Invalid/unauthenticated requests have no trusted tenant and must not create a synthetic row.
	if s.Writer != nil && e.TenantID != "" {
		if err := s.Writer.Submit(toLibraryDenied(e)); err != nil {
			if s.Metrics != nil {
				s.Metrics.AuditMirrorDropped.Add(1)
			}
			log.Printf("replay audit: auth.denied audit_log mirror dropped: %v", err)
		}
	}
	return nil
}

func toLibraryEvent(e AuditEvent) commonaudit.Event {
	result := commonaudit.NewEvent(commonaudit.Claims{TenantID: e.TenantID, Subject: e.Subject, ClientID: e.ClientID, ACR: e.ACR}, commonaudit.CategoryAudit,
		e.Action, outcome(e.Outcome), commonaudit.Resource{Type: "replay", ID: e.ReplayID}).WithTrace("", e.RequestID).WithDetails(map[string]string{
		"phase":        e.Phase,
		"source_id":    e.SourceID,
		"from_version": versionString(e.FromVersion),
		"to_version":   versionString(e.ToVersion),
	})
	result.ID = eventUUID(e.EventID)
	result.OccurredAt = e.Time
	return result
}

func toLibraryDenied(e DeniedEvent) commonaudit.Event {
	result := commonaudit.NewEvent(commonaudit.Claims{TenantID: e.TenantID, Subject: e.Subject, ClientID: e.ClientID, ACR: e.ACR}, commonaudit.CategorySecurity,
		ActionAuthDenied, commonaudit.OutcomeDenied, commonaudit.Resource{Type: "http_request", ID: ""}).WithTrace("", e.RequestID).WithDetails(map[string]string{
		"reason":    e.Reason,
		"method":    e.Method,
		"path":      e.Path,
		"remote_ip": e.RemoteIP,
	})
	result.ID = eventUUID(e.EventID)
	result.OccurredAt = e.Time
	return result
}

func outcome(v string) commonaudit.Outcome {
	if v == OutcomeSuccess {
		return commonaudit.OutcomeSuccess
	}
	if v == OutcomeRejected {
		return commonaudit.OutcomeDenied
	}
	return commonaudit.OutcomeFailure
}

func versionString(v *int32) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%d", *v)
}

// eventUUID turns replay's 128-bit hex event id into the UUID shape required by audit_log.
func eventUUID(id string) string {
	if len(id) != 32 {
		return ""
	}
	return id[:8] + "-" + id[8:12] + "-" + id[12:16] + "-" + id[16:20] + "-" + id[20:]
}
