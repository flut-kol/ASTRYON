// Package audit defines the audit/security event record (DPP-053), a bounded asynchronous PostgreSQL writer
// and a tenant-scoped reader over the existing audit_log table (migrations 0007 + 0015).
//
// Rules enforced here: the tenant comes only from validated token claims (Claims), never from request data;
// events carry identifiers and short enumerated metadata only, never secrets, tokens or payload/PII content.
package audit

import (
	"crypto/rand"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Category separates data/state audit actions from authn/authz/session security events.
type Category string

const (
	CategoryAudit    Category = "audit"
	CategorySecurity Category = "security"
)

// Outcome of the audited action.
type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeDenied  Outcome = "denied"
	OutcomeFailure Outcome = "failure"
)

const (
	MaxDetailKeys   = 16
	MaxDetailValue  = 256
	maxFieldLen     = 256
	maxTraceIDLen   = 64
	defaultActionRe = `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_-]*){1,3}$`
)

var (
	actionRe = regexp.MustCompile(defaultActionRe)
	uuidRe   = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	// Detail keys that suggest secrets or content; rejected rather than silently stored.
	forbiddenKey = regexp.MustCompile(`(?i)(secret|password|passwd|token|authorization|cookie|api[_-]?key|credential|private|payload|body|email|phone|name|address|ssn)`)
)

// Claims is the subset of validated token claims an audit event derives from. Build it from the verified
// principal (internal/auth) only. TenantID must never be taken from headers, query or body.
type Claims struct {
	TenantID string // UUID, from the validated token
	Subject  string // sub
	ClientID string // client_id (service accounts)
	ACR      string // acr claim: authentication context class achieved at the IdP (2FA evidence)
}

// Resource is the object acted upon.
type Resource struct {
	Type string // e.g. "source", "export", "session"
	ID   string // UUID goes to entity_id; any other short id goes to resource_ref
}

// Event is one audit or security record. Zero ID/OccurredAt are filled by Normalize.
type Event struct {
	ID         string
	TenantID   string
	OccurredAt time.Time
	Category   Category
	Action     string // dotted lower-case, e.g. "source.create", "auth.denied", "session.logout"
	Outcome    Outcome
	Subject    string
	ClientID   string
	ACR        string
	Resource   Resource
	TraceID    string
	RequestID  string
	Details    map[string]string // short enumerated metadata only
}

// NewEvent builds an event whose tenant and actor come exclusively from claims.
func NewEvent(c Claims, cat Category, action string, outcome Outcome, res Resource) Event {
	return Event{TenantID: c.TenantID, Subject: c.Subject, ClientID: c.ClientID, ACR: c.ACR,
		Category: cat, Action: action, Outcome: outcome, Resource: res}
}

// WithTrace returns a copy carrying trace and request ids.
func (e Event) WithTrace(traceID, requestID string) Event {
	e.TraceID, e.RequestID = traceID, requestID
	return e
}

// WithDetails returns a copy carrying detail metadata (validated on Normalize).
func (e Event) WithDetails(d map[string]string) Event { e.Details = d; return e }

// ErrInvalid wraps every validation failure.
var ErrInvalid = errors.New("audit: invalid event")

func bad(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
}

// Normalize validates the event and fills ID and OccurredAt (UTC). It returns the normalized copy.
func (e Event) Normalize(now time.Time) (Event, error) {
	if !uuidRe.MatchString(e.TenantID) {
		return e, bad("tenant_id must be a UUID taken from token claims")
	}
	e.TenantID = strings.ToLower(e.TenantID)
	switch e.Category {
	case CategoryAudit, CategorySecurity:
	default:
		return e, bad("category %q", e.Category)
	}
	switch e.Outcome {
	case OutcomeSuccess, OutcomeDenied, OutcomeFailure:
	default:
		return e, bad("outcome %q", e.Outcome)
	}
	if !actionRe.MatchString(e.Action) || len(e.Action) > maxFieldLen {
		return e, bad("action %q must look like noun.verb", e.Action)
	}
	if e.Subject == "" && e.ClientID == "" {
		return e, bad("actor (subject or client_id) required")
	}
	if e.Resource.Type == "" {
		return e, bad("resource type required")
	}
	for name, v := range map[string]string{"subject": e.Subject, "client_id": e.ClientID, "acr": e.ACR,
		"resource.type": e.Resource.Type, "resource.id": e.Resource.ID, "request_id": e.RequestID} {
		if len(v) > maxFieldLen {
			return e, bad("%s too long", name)
		}
	}
	if len(e.TraceID) > maxTraceIDLen {
		return e, bad("trace_id too long")
	}
	if len(e.Details) > MaxDetailKeys {
		return e, bad("more than %d detail keys", MaxDetailKeys)
	}
	for k, v := range e.Details {
		if forbiddenKey.MatchString(k) {
			return e, bad("detail key %q not allowed (secrets/PII/payload)", k)
		}
		if len(v) > MaxDetailValue {
			return e, bad("detail %q longer than %d", k, MaxDetailValue)
		}
	}
	if e.ID == "" {
		e.ID = newUUID()
	} else if !uuidRe.MatchString(e.ID) {
		return e, bad("id must be a UUID")
	}
	if e.OccurredAt.IsZero() {
		e.OccurredAt = now
	}
	e.OccurredAt = e.OccurredAt.UTC()
	return e, nil
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// entity returns (entity_id uuid-or-nil, resource_ref) for the resource id.
func (r Resource) split() (entityID *string, ref *string) {
	if r.ID == "" {
		return nil, nil
	}
	if uuidRe.MatchString(r.ID) {
		return &r.ID, nil
	}
	return nil, &r.ID
}
