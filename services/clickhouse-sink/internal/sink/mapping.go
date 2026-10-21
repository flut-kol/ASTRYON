package sink

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	ec "github.com/astryon/event-contracts"
)

const nilUUID = "00000000-0000-0000-0000-000000000000"

// processed is the processed.events.v1 value: the envelope plus optional
// normalization fields. A plain raw envelope is accepted too (extras default).
type processed struct {
	ec.Envelope
	BatchID         string            `json:"batch_id"`
	PipelineID      string            `json:"pipeline_id"`
	PipelineVersion int               `json:"pipeline_version"`
	EventType       string            `json:"event_type"`
	Attributes      map[string]string `json:"attributes"`
	QualityFlags    []string          `json:"quality_flags"`
	RawRef          string            `json:"raw_ref"`
}

// row mirrors events_normalized_local (migration 0001). event_date is MATERIALIZED
// and ingested_at has a default, so only ingested_at is sent when known.
type row struct {
	TenantID          string            `json:"tenant_id"`
	SourceID          string            `json:"source_id"`
	EventID           string            `json:"event_id"`
	BatchID           string            `json:"batch_id"`
	ProcessingVersion uint32            `json:"processing_version"`
	PipelineID        string            `json:"pipeline_id"`
	PipelineVersion   uint32            `json:"pipeline_version"`
	EventType         string            `json:"event_type"`
	EventTime         string            `json:"event_time"`
	IngestedAt        string            `json:"ingested_at"`
	EntityID          string            `json:"entity_id"`
	Attributes        map[string]string `json:"attributes"`
	Payload           string            `json:"payload"`
	RawRef            string            `json:"raw_ref"`
	QualityFlags      []string          `json:"quality_flags"`
}

const chTime = "2006-01-02 15:04:05.000"

// MapMessage converts a message to a JSONEachRow line, or returns a quarantine
// code and reason. now is the reference clock for envelope validation.
func MapMessage(m Message, now time.Time) (line []byte, code, reason string) {
	var p processed
	if err := json.Unmarshal(m.Value, &p); err != nil {
		return nil, "ERR_JSON_MALFORMED", err.Error()
	}
	if err := p.Envelope.ValidateAt(now); err != nil {
		// Do not echo payload content: only codes/fields.
		if es, ok := err.(ec.Errors); ok {
			codes := make([]string, len(es))
			for i, e := range es {
				codes[i] = string(e.Code) + "(" + e.Field + ")"
			}
			return nil, string(es[0].Code), strings.Join(codes, ",")
		}
		return nil, "ERR_ENVELOPE_INVALID", err.Error()
	}
	// ClickHouse columns tenant_id/source_id/event_id are UUID; ULID event ids cannot be stored.
	if !ec.IsUUID(p.EventID) {
		return nil, "ERR_EVENT_ID_NOT_UUID", "event_id must be a UUID for the UUID column"
	}
	if p.EventTime == nil {
		return nil, "ERR_EVENT_TIME_MISSING", "events_normalized.event_time is not nullable"
	}
	if len(p.Payload) == 0 {
		return nil, "ERR_PAYLOAD_INLINE_REQUIRED", "object_ref-only events must be normalized before the sink"
	}
	if p.BatchID == "" {
		p.BatchID = nilUUID
	}
	if p.PipelineID == "" {
		p.PipelineID = nilUUID
	}
	if !ec.IsUUID(p.BatchID) || !ec.IsUUID(p.PipelineID) {
		return nil, "ERR_ID_INVALID", "batch_id/pipeline_id must be UUID"
	}
	if p.PipelineVersion < 0 {
		return nil, "ERR_PIPELINE_VERSION_INVALID", "pipeline_version < 0"
	}
	if p.EventType == "" {
		p.EventType = p.SchemaID
	}
	if p.RawRef == "" {
		p.RawRef = p.ObjectRef
	}
	if p.RawRef == "" {
		p.RawRef = fmt.Sprintf("kafka:%s/%d/%d", m.Topic, m.Partition, m.Offset)
	}
	if p.Attributes == nil {
		p.Attributes = map[string]string{}
	}
	if p.QualityFlags == nil {
		p.QualityFlags = []string{}
	}
	r := row{
		TenantID: strings.ToLower(p.TenantID), SourceID: strings.ToLower(p.SourceID), EventID: strings.ToLower(p.EventID),
		BatchID: strings.ToLower(p.BatchID), ProcessingVersion: uint32(p.ProcessingVersion), // #nosec G115 -- small bounded counter or length, reviewed 2026-10-21
		PipelineID: strings.ToLower(p.PipelineID), PipelineVersion: uint32(p.PipelineVersion), // #nosec G115 -- small bounded counter or length, reviewed 2026-10-21
		EventType: p.EventType, EventTime: p.EventTime.UTC().Format(chTime), IngestedAt: p.IngestedAt.UTC().Format(chTime),
		EntityID: p.EntityKey, Attributes: p.Attributes, Payload: string(p.Payload), RawRef: p.RawRef, QualityFlags: p.QualityFlags,
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, "ERR_MARSHAL", err.Error()
	}
	return b, "", ""
}
