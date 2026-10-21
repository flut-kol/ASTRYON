package eventcontracts

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

// QuarantineContractVersionV1 is the only wire version understood by
// QuarantineRecordV1. A new incompatible shape must use a new version and type.
const QuarantineContractVersionV1 uint8 = 1

const (
	MaxQuarantineRecordJSONBytes = 128 << 10
	MaxQuarantineOriginalBytes   = 64 << 10
	MaxQuarantineReasonCodeBytes = 128
	MaxQuarantineStageBytes      = 64
	MaxQuarantineDiagnosticBytes = 2048
	MaxQuarantineSourceTopicLen  = 249
	MaxQuarantineIdentityBytes   = 1024
	MaxQuarantineAge             = 10 * 366 * 24 * time.Hour
)

const QuarantineOriginalEncodingBase64 = "base64"

var (
	quarantineReasonCodeRe = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
	kafkaTopicRe           = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
)

// QuarantineRecordV1 is the canonical events.quarantine.v1 value.
//
// Original is opaque and potentially sensitive. The contract makes no claim
// that those bytes are masked, safe to log, or safe to persist in an operator
// read model. Authorization and retention are consumer responsibilities.
type QuarantineRecordV1 struct {
	ContractVersion   uint8                  `json:"contract_version"`
	DeviationID       string                 `json:"deviation_id"`
	TenantID          string                 `json:"tenant_id"`
	SourceID          string                 `json:"source_id,omitempty"`
	EventID           string                 `json:"event_id,omitempty"`
	BatchID           string                 `json:"batch_id,omitempty"`
	JobID             string                 `json:"job_id,omitempty"`
	ProcessingVersion *uint32                `json:"processing_version,omitempty"`
	PipelineVersion   *uint32                `json:"pipeline_version,omitempty"`
	QuarantinedAt     time.Time              `json:"quarantined_at"`
	Stage             string                 `json:"stage"`
	ReasonCode        string                 `json:"reason_code"`
	Diagnostic        string                 `json:"diagnostic,omitempty"`
	Provenance        QuarantineProvenanceV1 `json:"provenance"`
	Original          *QuarantineOriginalV1  `json:"original,omitempty"`
}

// QuarantineProvenanceV1 identifies the input record that was rejected. Kafka
// partition and offset are either both present or both absent (for example, a
// gateway rejection before the event has been published).
type QuarantineProvenanceV1 struct {
	SourceTopic     string `json:"source_topic"`
	SourcePartition *int32 `json:"source_partition,omitempty"`
	SourceOffset    *int64 `json:"source_offset,omitempty"`
}

// QuarantineOriginalV1 contains a bounded byte-for-byte copy encoded for JSON.
// Truncated describes the producer's copy, not the sensitivity of its content.
type QuarantineOriginalV1 struct {
	Encoding  string `json:"encoding"`
	Data      string `json:"data"`
	Truncated bool   `json:"truncated"`
}

// ContractViolation identifies one invalid wire-contract field.
type ContractViolation struct {
	Field   string
	Message string
}

func (e ContractViolation) Error() string { return e.Field + ": " + e.Message }

// ContractViolations reports every validation failure found.
type ContractViolations []ContractViolation

func (e ContractViolations) Error() string {
	parts := make([]string, len(e))
	for i := range e {
		parts[i] = e[i].Error()
	}
	return strings.Join(parts, "; ")
}

// Validate checks a quarantine record against the current wall clock.
func (q *QuarantineRecordV1) Validate() error { return q.ValidateAt(time.Now()) }

// ValidateAt checks required fields, bounds, identifiers and provenance
// consistency using now as the reference clock.
func (q *QuarantineRecordV1) ValidateAt(now time.Time) error {
	if q == nil {
		return ContractViolations{{Field: "record", Message: "must not be null"}}
	}
	var violations ContractViolations
	add := func(field, message string) {
		violations = append(violations, ContractViolation{Field: field, Message: message})
	}

	if q.ContractVersion != QuarantineContractVersionV1 {
		add("contract_version", "must be 1")
	}
	if !IsUUID(q.DeviationID) {
		add("deviation_id", "must be a UUID")
	}
	if !IsUUID(q.TenantID) {
		add("tenant_id", "must be a UUID")
	}
	validateOptionalUUID := func(field, value string) {
		if value != "" && !IsUUID(value) {
			add(field, "must be a UUID when present")
		}
	}
	validateOptionalUUID("source_id", q.SourceID)
	validateOptionalUUID("event_id", q.EventID)
	validateOptionalUUID("batch_id", q.BatchID)
	validateOptionalUUID("job_id", q.JobID)
	if q.ProcessingVersion != nil && *q.ProcessingVersion == 0 {
		add("processing_version", "must be >= 1 when present")
	}
	if q.PipelineVersion != nil && *q.PipelineVersion == 0 {
		add("pipeline_version", "must be >= 1 when present")
	}

	if q.QuarantinedAt.IsZero() {
		add("quarantined_at", "is required")
	} else {
		if _, offset := q.QuarantinedAt.Zone(); offset != 0 {
			add("quarantined_at", "must be UTC")
		}
		if q.QuarantinedAt.After(now.Add(MaxFutureSkew)) {
			add("quarantined_at", "is too far in the future")
		}
		if q.QuarantinedAt.Before(now.Add(-MaxQuarantineAge)) {
			add("quarantined_at", "is implausibly old")
		}
	}
	if q.Stage == "" || len(q.Stage) > MaxQuarantineStageBytes || !validText(q.Stage) {
		add("stage", "must be printable and 1..64 bytes")
	}
	if len(q.ReasonCode) > MaxQuarantineReasonCodeBytes || !quarantineReasonCodeRe.MatchString(q.ReasonCode) {
		add("reason_code", "must match [A-Z][A-Z0-9_]{0,127}")
	}
	if len(q.Diagnostic) > MaxQuarantineDiagnosticBytes || !validOptText(q.Diagnostic) {
		add("diagnostic", "must be printable and at most 2048 bytes")
	}

	if issue := validateQuarantineTopic(q.Provenance.SourceTopic); issue != "" {
		add("provenance.source_topic", issue)
	}
	partitionPresent := q.Provenance.SourcePartition != nil
	offsetPresent := q.Provenance.SourceOffset != nil
	if partitionPresent != offsetPresent {
		add("provenance", "source_partition and source_offset must be present together")
	}
	if partitionPresent && *q.Provenance.SourcePartition < 0 {
		add("provenance.source_partition", "must be non-negative")
	}
	if offsetPresent && *q.Provenance.SourceOffset < 0 {
		add("provenance.source_offset", "must be non-negative")
	}

	if q.Original != nil {
		if q.Original.Encoding != QuarantineOriginalEncodingBase64 {
			add("original.encoding", `must be "base64"`)
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(q.Original.Data)
		if err != nil {
			add("original.data", "must be canonical padded base64")
		} else if len(decoded) > MaxQuarantineOriginalBytes {
			add("original.data", "decoded value exceeds 65536 bytes")
		} else if base64.StdEncoding.EncodeToString(decoded) != q.Original.Data {
			add("original.data", "must be canonical padded base64")
		}
	}

	if len(violations) != 0 {
		return violations
	}
	return nil
}

// DecodeQuarantineRecordV1 strictly decodes and validates one complete JSON
// value. Unknown fields, trailing JSON and oversized records are rejected.
func DecodeQuarantineRecordV1(data []byte) (QuarantineRecordV1, error) {
	return DecodeQuarantineRecordV1At(data, time.Now())
}

// DecodeQuarantineRecordV1At is DecodeQuarantineRecordV1 with an injectable
// validation clock for deterministic consumers and tests.
func DecodeQuarantineRecordV1At(data []byte, now time.Time) (QuarantineRecordV1, error) {
	var q QuarantineRecordV1
	if len(data) > MaxQuarantineRecordJSONBytes {
		return q, fmt.Errorf("quarantine record exceeds %d bytes", MaxQuarantineRecordJSONBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&q); err != nil {
		return q, fmt.Errorf("decode quarantine record v1: %w", err)
	}
	if err := requireJSONEOF(dec); err != nil {
		return q, err
	}
	if err := q.ValidateAt(now); err != nil {
		return q, err
	}
	return q, nil
}

// NewQuarantineOriginalV1 copies at most MaxQuarantineOriginalBytes from raw
// and records whether bytes were omitted. The returned value does not imply
// that the data is masked, non-secret, or otherwise safe to expose.
func NewQuarantineOriginalV1(raw []byte) QuarantineOriginalV1 {
	truncated := len(raw) > MaxQuarantineOriginalBytes
	if truncated {
		raw = raw[:MaxQuarantineOriginalBytes]
	}
	return QuarantineOriginalV1{
		Encoding:  QuarantineOriginalEncodingBase64,
		Data:      base64.StdEncoding.EncodeToString(raw),
		Truncated: truncated,
	}
}

// QuarantineDeviationIDFromOffset returns a deterministic UUID for a producer
// and the source Kafka coordinate it is quarantining. It uses a domain-separated
// SHA-256 digest and marks the UUID as RFC 9562 version 8 (custom).
func QuarantineDeviationIDFromOffset(producer, sourceTopic string, partition int32, offset int64) (string, error) {
	if issue := validateIdentityText("producer", producer); issue != "" {
		return "", errors.New(issue)
	}
	if issue := validateQuarantineTopic(sourceTopic); issue != "" {
		return "", fmt.Errorf("source_topic: %s", issue)
	}
	if partition < 0 || offset < 0 {
		return "", errors.New("partition and offset must be non-negative")
	}
	var coordinates [12]byte
	binary.BigEndian.PutUint32(coordinates[:4], uint32(partition))
	binary.BigEndian.PutUint64(coordinates[4:], uint64(offset))
	return quarantineIdentityUUID([]byte(producer), []byte(sourceTopic), coordinates[:]), nil
}

// QuarantineDeviationIDFromKey returns a deterministic UUID when no Kafka
// source coordinate exists, for example a gateway rejection before publish.
// stableKey must be a producer-owned, retry-stable, non-secret identifier.
func QuarantineDeviationIDFromKey(producer, stableKey string) (string, error) {
	if issue := validateIdentityText("producer", producer); issue != "" {
		return "", errors.New(issue)
	}
	if issue := validateIdentityText("stable key", stableKey); issue != "" {
		return "", errors.New(issue)
	}
	return quarantineIdentityUUID([]byte(producer), []byte(stableKey)), nil
}

func quarantineIdentityUUID(parts ...[]byte) string {
	h := sha256.New()
	h.Write([]byte("astryon/quarantine-record/v1\x00"))
	var size [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		h.Write(size[:])
		h.Write(part)
	}
	b := h.Sum(nil)[:16]
	b[6] = b[6]&0x0f | 0x80
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func validateQuarantineTopic(topic string) string {
	if topic == "" || len(topic) > MaxQuarantineSourceTopicLen || !kafkaTopicRe.MatchString(topic) {
		return "must be a Kafka topic name of 1..249 bytes"
	}
	return ""
}

func validateIdentityText(label, value string) string {
	if value == "" || len(value) > MaxQuarantineIdentityBytes || !validText(value) {
		return label + " must be printable and 1..1024 bytes"
	}
	return ""
}

func requireJSONEOF(dec *json.Decoder) error {
	var extra json.RawMessage
	err := dec.Decode(&extra)
	switch {
	case errors.Is(err, io.EOF):
		return nil
	case err != nil:
		return fmt.Errorf("decode trailing quarantine data: %w", err)
	default:
		return errors.New("quarantine record contains trailing JSON value")
	}
}
