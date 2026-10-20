// Package export implements the execution core of export jobs (DPP-049): a tenant-scoped ClickHouse
// query streamed into an S3-compatible object store under per-tenant quota and audit, with pre-signed
// short-lived download URLs. It has no PostgreSQL/Kafka code; job persistence belongs to the caller.
package export

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	MaxRange       = 31 * 24 * time.Hour // same rule as services/query-api
	DefaultMaxRows = 1_000_000
	HardMaxRows    = 10_000_000
	chTime         = "2006-01-02 15:04:05.000"
)

var (
	uuidRe     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	exportIDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{7,63}$`)

	ErrInvalidSpec = errors.New("invalid export spec")
)

// Principal is the verified caller. TenantID must come from validated token claims only.
type Principal struct {
	TenantID string
	Subject  string
	Scopes   []string // used by the HTTP layer only
}

// ExportSpec is the export request body. There is intentionally no tenant field.
type ExportSpec struct {
	Format            string    `json:"format"` // csv | ndjson
	From              time.Time `json:"from"`
	To                time.Time `json:"to"`
	ProcessingVersion *uint32   `json:"processing_version"`
	SourceID          string    `json:"source_id,omitempty"`
	EventType         string    `json:"event_type,omitempty"`
	EntityID          string    `json:"entity_id,omitempty"`
	MaxRows           int64     `json:"max_rows,omitempty"`
}

func invalid(f string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSpec, fmt.Sprintf(f, a...))
}

// Normalize validates the spec and applies defaults, returning a copy.
func (s ExportSpec) Normalize() (ExportSpec, error) {
	switch s.Format {
	case "":
		s.Format = "ndjson"
	case "csv", "ndjson":
	default:
		return s, invalid("format must be csv or ndjson")
	}
	if s.From.IsZero() || s.To.IsZero() || !s.To.After(s.From) {
		return s, invalid("from and to are required and to must be after from")
	}
	if s.To.Sub(s.From) > MaxRange {
		return s, invalid("time range exceeds 31 days")
	}
	if s.ProcessingVersion == nil {
		return s, invalid("processing_version is required")
	}
	if s.SourceID != "" && !uuidRe.MatchString(s.SourceID) {
		return s, invalid("source_id must be a UUID")
	}
	if len(s.EventType) > 256 || len(s.EntityID) > 512 {
		return s, invalid("filter value too long")
	}
	if s.MaxRows < 0 || s.MaxRows > HardMaxRows {
		return s, invalid("max_rows must be 1..%d", HardMaxRows)
	}
	if s.MaxRows == 0 {
		s.MaxRows = DefaultMaxRows
	}
	s.From, s.To = s.From.UTC(), s.To.UTC()
	return s, nil
}

// Ext is the object key extension for the format.
func (s ExportSpec) Ext() string { return s.Format }

// Hash is a stable sha256 over the normalized spec (no tenant), for audit correlation.
func (s ExportSpec) Hash() string {
	b, _ := json.Marshal(s)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// ObjectKey returns exports/<tenant_id>/<export_id>.<ext>.
func ObjectKey(tenantID, exportID, ext string) string {
	return "exports/" + tenantID + "/" + exportID + "." + ext
}

// KeyBelongsToTenant reports whether key lies strictly under the tenant's prefix (no traversal).
func KeyBelongsToTenant(key, tenantID string) bool {
	if !uuidRe.MatchString(tenantID) {
		return false
	}
	p := "exports/" + tenantID + "/"
	rest, ok := strings.CutPrefix(key, p)
	return ok && rest != "" && !strings.Contains(rest, "/") && !strings.Contains(rest, "..")
}

const selectCols = "event_id, source_id, batch_id, processing_version, event_type, event_time, entity_id, attributes, payload, quality_flags"

// buildQuery mirrors services/query-api: every value is a bound ClickHouse parameter, tenant comes from
// the Principal, duplicates are removed with LIMIT 1 BY. One extra row is requested so a cap overflow is
// detectable (max_rows+1).
func buildQuery(table string, tenantID string, s ExportSpec) (string, url.Values) {
	params := url.Values{}
	var conds []string
	add := func(cond, name, val string) {
		conds = append(conds, cond)
		params.Set("param_"+name, val)
	}
	add("tenant_id = {p_tenant:UUID}", "p_tenant", tenantID)
	add("event_time >= {p_from:DateTime64(3, 'UTC')}", "p_from", s.From.UTC().Format(chTime))
	add("event_time < {p_to:DateTime64(3, 'UTC')}", "p_to", s.To.UTC().Format(chTime))
	add("processing_version = {p_pv:UInt32}", "p_pv", fmt.Sprint(*s.ProcessingVersion))
	if s.SourceID != "" {
		add("source_id = {p_source:UUID}", "p_source", s.SourceID)
	}
	if s.EventType != "" {
		add("event_type = {p_type:String}", "p_type", s.EventType)
	}
	if s.EntityID != "" {
		add("entity_id = {p_entity:String}", "p_entity", s.EntityID)
	}
	format := "JSONEachRow"
	if s.Format == "csv" {
		format = "CSVWithNames"
	}
	q := fmt.Sprintf(`SELECT %s FROM %s WHERE %s ORDER BY event_time, event_id
LIMIT 1 BY tenant_id, source_id, event_id, processing_version
LIMIT %d FORMAT %s`, selectCols, table, strings.Join(conds, " AND "), s.MaxRows+1, format)
	return q, params
}
