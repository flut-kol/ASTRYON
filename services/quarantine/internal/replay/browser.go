package replay

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	ScopeQuarantineRead        = "astryon:quarantine:read"
	ScopeQuarantineReadPayload = "astryon:quarantine:read-payload"
	defaultPageSize            = 50
	maxPageSize                = 100
)

var ErrInvalidPage = errors.New("invalid quarantine page")

var tenantUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// BrowserPrincipal is a validated caller identity. TenantID must originate in
// token claims; it is deliberately not accepted as a query parameter.
type BrowserPrincipal struct {
	TenantID string
	Scopes   map[string]bool
}

type BrowserAuthenticator interface {
	Authenticate(*http.Request) (BrowserPrincipal, error)
}

// QuarantineRecord contains only browser-safe metadata by default. Original
// is populated only after the separate payload-read authorization succeeds.
type QuarantineRecord struct {
	ID                string          `json:"id"`
	TenantID          string          `json:"-"`
	SourceID          string          `json:"source_id,omitempty"`
	BatchID           string          `json:"batch_id,omitempty"`
	JobID             string          `json:"job_id,omitempty"`
	EventID           string          `json:"event_id,omitempty"`
	ProcessingVersion uint32          `json:"processing_version,omitempty"`
	PipelineVersion   uint32          `json:"pipeline_version,omitempty"`
	ReasonCode        string          `json:"reason_code"`
	Reason            string          `json:"reason,omitempty"`
	Stage             string          `json:"stage"`
	SourceTopic       string          `json:"source_topic,omitempty"`
	SourcePartition   int32           `json:"source_partition,omitempty"`
	SourceOffset      int64           `json:"source_offset,omitempty"`
	QuarantinedAt     string          `json:"quarantined_at"`
	Original          json.RawMessage `json:"original,omitempty"`
	PayloadKind       string          `json:"payload_kind,omitempty"`
}

type BrowserPage struct {
	Records    []QuarantineRecord
	NextCursor string
}

// BrowserStore must enforce the tenant predicate at its storage boundary too.
// The handler supplies it from validated identity, never from the request.
type BrowserStore interface {
	ListQuarantine(ctx context.Context, tenantID, cursor string, limit int, includePayload bool) (BrowserPage, error)
}

type BrowserHandler struct {
	Auth  BrowserAuthenticator
	Store BrowserStore
}

func (h BrowserHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		traceID := browserRequestID(r)
		w.Header().Set("X-Request-ID", traceID)
		writeBrowserJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/quarantine", h.list)
	return mux
}

func (h BrowserHandler) list(w http.ResponseWriter, r *http.Request) {
	traceID := browserRequestID(r)
	w.Header().Set("X-Request-ID", traceID)
	if h.Auth == nil || h.Store == nil {
		writeBrowserError(w, http.StatusServiceUnavailable, "QUARANTINE_UNAVAILABLE", "quarantine browser is not configured", traceID)
		return
	}
	p, err := h.Auth.Authenticate(r)
	if err != nil || p.TenantID == "" {
		writeBrowserError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required", traceID)
		return
	}
	if !tenantUUID.MatchString(p.TenantID) {
		writeBrowserError(w, http.StatusForbidden, "FORBIDDEN", "principal has no valid tenant", traceID)
		return
	}
	p.TenantID = strings.ToLower(p.TenantID)
	if !p.Scopes[ScopeQuarantineRead] {
		writeBrowserError(w, http.StatusForbidden, "FORBIDDEN", "quarantine read scope required", traceID)
		return
	}
	includePayload := r.URL.Query().Get("include_original") == "true"
	if includePayload && !p.Scopes[ScopeQuarantineReadPayload] {
		writeBrowserError(w, http.StatusForbidden, "FORBIDDEN", "quarantine payload read scope required", traceID)
		return
	}
	limit, err := pageSize(r.URL.Query().Get("limit"))
	if err != nil {
		writeBrowserError(w, http.StatusBadRequest, "INVALID_PAGE", err.Error(), traceID)
		return
	}
	page, err := h.Store.ListQuarantine(r.Context(), p.TenantID, r.URL.Query().Get("cursor"), limit, includePayload)
	if err != nil {
		if errors.Is(err, ErrInvalidCursor) {
			writeBrowserError(w, http.StatusBadRequest, "INVALID_CURSOR", "cursor is malformed", traceID)
			return
		}
		writeBrowserError(w, http.StatusServiceUnavailable, "QUARANTINE_UNAVAILABLE", "quarantine records unavailable", traceID)
		return
	}
	records := make([]QuarantineRecord, 0, len(page.Records))
	boundaryViolation := false
	for i := range page.Records {
		// A store returning another tenant's record is a hard boundary failure:
		// omit it rather than ever leaking it to this principal.
		if page.Records[i].TenantID != p.TenantID {
			boundaryViolation = true
			continue
		}
		record := page.Records[i]
		if !includePayload {
			record.Original = nil
			record.PayloadKind = ""
		}
		records = append(records, record)
	}
	nextCursor := page.NextCursor
	if boundaryViolation {
		// A broken adapter must not smuggle foreign identifiers in its cursor.
		nextCursor = ""
	}
	writeBrowserJSON(w, http.StatusOK, map[string]any{"items": records, "next_cursor": nextCursor})
}

func pageSize(raw string) (int, error) {
	if raw == "" {
		return defaultPageSize, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 || v > maxPageSize {
		return 0, ErrInvalidPage
	}
	return v, nil
}

func browserRequestID(r *http.Request) string {
	if v := r.Header.Get("X-Request-ID"); v != "" && len(v) <= 64 {
		return v
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return fmt.Sprintf("%x", b[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

func writeBrowserError(w http.ResponseWriter, status int, code, message, traceID string) {
	writeBrowserJSON(w, status, map[string]any{"error": map[string]any{
		"code": code, "message": message, "details": nil, "trace_id": traceID,
	}})
}

func writeBrowserJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
