// Package eventcontracts defines the ASTRYON event envelope (DEVELOPMENT_PLAN §5.1),
// Kafka topic catalogue (§5.2) and dedup/partition key helpers (§5.2-5.3).
package eventcontracts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// PayloadFormat is the payload_format enum.
type PayloadFormat string

const (
	FormatJSON             PayloadFormat = "json"
	FormatProtobuf         PayloadFormat = "protobuf"
	FormatAvro             PayloadFormat = "avro"
	FormatParquetReference PayloadFormat = "parquet-reference"
)

// Valid reports whether f is a known format.
func (f PayloadFormat) Valid() bool {
	switch f {
	case FormatJSON, FormatProtobuf, FormatAvro, FormatParquetReference:
		return true
	}
	return false
}

// Limits.
const (
	MaxInlinePayloadBytes = 1 << 20 // 1 MiB; larger content must go via object_ref
	MaxEntityKeyLen       = 256
	MaxTraceIDLen         = 128
	MaxSchemaIDLen        = 256
	MaxObjectRefLen       = 2048
	MaxFutureSkew         = 5 * time.Minute
	MaxEventAge           = 366 * 24 * time.Hour * 10 // 10 years: guards zero/garbage timestamps
)

// Envelope is the base event envelope (§5.1). JSON field names are the wire contract.
type Envelope struct {
	EventID           string          `json:"event_id"`
	TenantID          string          `json:"tenant_id"`
	SourceID          string          `json:"source_id"`
	SchemaID          string          `json:"schema_id"`
	SchemaVersion     int             `json:"schema_version"`
	EventTime         *time.Time      `json:"event_time,omitempty"`
	EventTimeMissing  bool            `json:"event_time_missing,omitempty"` // explicit absence marker
	IngestedAt        time.Time       `json:"ingested_at"`
	EntityKey         string          `json:"entity_key,omitempty"` // must not be raw PII
	TraceID           string          `json:"trace_id"`
	PayloadFormat     PayloadFormat   `json:"payload_format"`
	Payload           json.RawMessage `json:"payload,omitempty"`
	ObjectRef         string          `json:"object_ref,omitempty"`
	Checksum          string          `json:"checksum,omitempty"` // sha256 hex (64 chars)
	ProcessingVersion int             `json:"processing_version"`
}

var (
	uuidRe     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	ulidRe     = regexp.MustCompile(`^[0-7][0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{25}$`)
	checksumRe = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// IsUUID reports whether s is a canonical textual UUID.
func IsUUID(s string) bool { return uuidRe.MatchString(s) }

// IsULID reports whether s is a 26-char Crockford base32 ULID.
func IsULID(s string) bool { return ulidRe.MatchString(s) }

// CanonicalID normalizes a UUID (lowercase) or ULID (uppercase). ok=false if neither.
func CanonicalID(s string) (string, bool) {
	switch {
	case IsUUID(s):
		return strings.ToLower(s), true
	case IsULID(s):
		return strings.ToUpper(s), true
	}
	return "", false
}

// Validate checks the envelope against the current wall clock.
func (e *Envelope) Validate() error { return e.ValidateAt(time.Now()) }

// ValidateAt checks the envelope using now as the reference clock. It returns
// nil or an Errors value listing every failure found.
func (e *Envelope) ValidateAt(now time.Time) error {
	var errs Errors
	add := func(c Code, field, msg string) { errs = append(errs, &Error{Code: c, Field: field, Message: msg}) }

	if _, ok := CanonicalID(e.EventID); !ok {
		add(ErrEventIDInvalid, "event_id", "must be a UUID or ULID")
	}
	if !IsUUID(e.TenantID) {
		add(ErrTenantIDInvalid, "tenant_id", "must be a UUID")
	}
	if !IsUUID(e.SourceID) {
		add(ErrSourceIDInvalid, "source_id", "must be a UUID")
	}
	if e.SchemaID == "" || len(e.SchemaID) > MaxSchemaIDLen || !validText(e.SchemaID) {
		add(ErrSchemaIDInvalid, "schema_id", "required, printable, max 256 bytes")
	}
	if e.SchemaVersion < 1 {
		add(ErrSchemaVersionInvalid, "schema_version", "must be >= 1")
	}

	// event_time: UTC, sane, or explicitly marked missing.
	switch {
	case e.EventTime == nil && !e.EventTimeMissing:
		add(ErrEventTimeMissing, "event_time", "absent without event_time_missing=true")
	case e.EventTime != nil && e.EventTimeMissing:
		add(ErrEventTimeMissing, "event_time", "event_time_missing=true conflicts with event_time value")
	case e.EventTime != nil:
		t := *e.EventTime
		if _, off := t.Zone(); off != 0 {
			add(ErrEventTimeNotUTC, "event_time", "must be UTC")
		}
		if t.After(now.Add(MaxFutureSkew)) {
			add(ErrEventTimeFuture, "event_time", "too far in the future")
		}
		if t.Before(now.Add(-MaxEventAge)) {
			add(ErrEventTimeTooOld, "event_time", "implausibly old")
		}
	}

	if e.IngestedAt.IsZero() {
		add(ErrIngestedAtInvalid, "ingested_at", "required (server time)")
	} else if _, off := e.IngestedAt.Zone(); off != 0 {
		add(ErrIngestedAtInvalid, "ingested_at", "must be UTC")
	} else if e.IngestedAt.After(now.Add(MaxFutureSkew)) {
		add(ErrIngestedAtInvalid, "ingested_at", "too far in the future")
	}

	if len(e.EntityKey) > MaxEntityKeyLen || !validOptText(e.EntityKey) {
		add(ErrEntityKeyInvalid, "entity_key", "printable, max 256 bytes")
	}
	if e.TraceID == "" || len(e.TraceID) > MaxTraceIDLen || !validText(e.TraceID) {
		add(ErrTraceIDInvalid, "trace_id", "required, printable, max 128 bytes")
	}
	if !e.PayloadFormat.Valid() {
		add(ErrPayloadFormatInvalid, "payload_format", "must be json|protobuf|avro|parquet-reference")
	}

	hasPayload := len(e.Payload) > 0
	hasRef := e.ObjectRef != ""
	if hasPayload == hasRef {
		add(ErrPayloadXorObjectRef, "payload", "exactly one of payload and object_ref is required")
	}
	if e.PayloadFormat == FormatParquetReference && !hasRef {
		add(ErrPayloadXorObjectRef, "object_ref", "parquet-reference requires object_ref")
	}
	if hasPayload {
		if len(e.Payload) > MaxInlinePayloadBytes {
			add(ErrPayloadTooLarge, "payload", "exceeds inline limit; use object_ref")
		} else if !json.Valid(e.Payload) || bytes.Equal(bytes.TrimSpace(e.Payload), []byte("null")) {
			add(ErrPayloadMalformed, "payload", "must be valid non-null JSON (binary formats: base64 JSON string)")
		}
	}
	if hasRef {
		if err := validateObjectRef(e.ObjectRef); err != "" {
			add(ErrObjectRefInvalid, "object_ref", err)
		}
		if e.Checksum == "" {
			add(ErrChecksumRequired, "checksum", "required with object_ref")
		}
	}
	if e.Checksum != "" {
		if !checksumRe.MatchString(e.Checksum) {
			add(ErrChecksumInvalid, "checksum", "must be 64 lowercase hex chars (SHA-256)")
		} else if hasPayload && len(e.Payload) <= MaxInlinePayloadBytes {
			sum := sha256.Sum256(e.Payload)
			if hex.EncodeToString(sum[:]) != e.Checksum {
				add(ErrChecksumMismatch, "checksum", "does not match SHA-256 of payload bytes")
			}
		}
	}
	if e.ProcessingVersion < 1 {
		add(ErrProcessingVersionInval, "processing_version", "must be >= 1")
	}

	if len(errs) == 0 {
		return nil
	}
	return errs
}

// ChecksumOf returns the lowercase SHA-256 hex of b.
func ChecksumOf(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func validateObjectRef(ref string) string {
	if len(ref) > MaxObjectRefLen {
		return "too long"
	}
	u, err := url.Parse(ref)
	if err != nil {
		return "unparseable URI"
	}
	if u.Scheme != "s3" {
		return "scheme must be s3"
	}
	if u.Host == "" || len(strings.TrimPrefix(u.Path, "/")) == 0 {
		return "must be s3://<bucket>/<key>"
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "userinfo, query and fragment are not allowed"
	}
	if strings.Contains(u.Path, "/../") || strings.HasSuffix(u.Path, "/..") {
		return "path traversal not allowed"
	}
	return ""
}

func validText(s string) bool {
	if s == "" || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validOptText(s string) bool { return s == "" || validText(s) }
