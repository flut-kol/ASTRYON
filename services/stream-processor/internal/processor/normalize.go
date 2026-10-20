// Package processor implements the DPP-026/027 stream processing semantics
// (validate -> normalize -> bounded dedup -> route) as pure functions plus a thin
// Kafka-backed runner. See the service README for the Flink deviation note.
package processor

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	ec "github.com/astryon/event-contracts"
)

// Kind is the routing decision for one input record.
type Kind int

const (
	KindOut        Kind = iota // -> processed.events.v1
	KindDuplicate              // dropped (inside dedup window)
	KindQuarantine             // -> events.quarantine.v1 (contract violation, replayable after fix)
	KindDLQ                    // -> events.dlq.v1 (unrecoverable)
)

// Input is one consumed raw record.
type Input struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Result is the outcome of processing one Input.
type Result struct {
	Kind     Kind
	In       Input
	Key      []byte // partition key for KindOut: tenant_id + entity_key (event_id if no entity)
	Value    []byte // normalized JSON for KindOut
	DedupKey string
	Code     string // reason code for quarantine/DLQ
	Reason   string // never contains payload content
}

// Processed is the processed.events.v1 value. It is the validated envelope plus
// normalization fields; field names match what services/clickhouse-sink reads.
type Processed struct {
	ec.Envelope
	PipelineVersion int      `json:"pipeline_version"`
	EventType       string   `json:"event_type"`
	QualityFlags    []string `json:"quality_flags"`
	RawRef          string   `json:"raw_ref"` // reference to the original raw record
}

// Quality flags attached by normalization.
const (
	FlagEventTimeDefaulted = "event_time_defaulted_to_ingested_at"
	FlagEntityKeyMissing   = "entity_key_missing"
	FlagEventIDNotUUID     = "event_id_not_uuid"
	FlagPayloadByRef       = "payload_by_object_ref"
)

// Codes for conditions detected by this stage (validation codes come from event-contracts).
const (
	CodeJSONMalformed  = "ERR_JSON_MALFORMED"
	CodeRecordTooLarge = "ERR_RECORD_TOO_LARGE"
	CodeInternal       = "ERR_INTERNAL"
	CodeDedupKey       = "ERR_DEDUP_KEY"
)

// Normalizer holds the stateless processing parameters.
type Normalizer struct {
	PipelineVersion int
	MaxRecordBytes  int // output record cap; larger -> DLQ (0 = no cap)
}

// Normalize validates and normalizes one raw record. It never panics and never
// touches dedup state. tenant_id is taken from the envelope only: the gateway has
// already bound it to the validated token claims (this stage trusts no headers).
func (n Normalizer) Normalize(in Input, now time.Time) (res Result) {
	res = Result{In: in}
	defer func() {
		if r := recover(); r != nil {
			res = Result{Kind: KindDLQ, In: in, Code: CodeInternal, Reason: fmt.Sprintf("panic: %v", r)}
		}
	}()

	var env ec.Envelope
	if err := json.Unmarshal(in.Value, &env); err != nil {
		return quarantine(in, CodeJSONMalformed, err.Error())
	}
	if err := env.ValidateAt(now); err != nil {
		if es, ok := err.(ec.Errors); ok && len(es) > 0 {
			parts := make([]string, len(es))
			for i, e := range es {
				parts[i] = string(e.Code) + "(" + e.Field + ")"
			}
			return quarantine(in, string(es[0].Code), strings.Join(parts, ","))
		}
		return quarantine(in, "ERR_ENVELOPE_INVALID", "validation failed")
	}

	flags := []string{}
	// Canonical identifiers so case variants collide in dedup and downstream.
	env.EventID, _ = ec.CanonicalID(env.EventID)
	env.TenantID = strings.ToLower(env.TenantID)
	env.SourceID = strings.ToLower(env.SourceID)
	if !ec.IsUUID(env.EventID) {
		flags = append(flags, FlagEventIDNotUUID)
	}
	if env.EventTime == nil {
		t := env.IngestedAt.UTC()
		env.EventTime, env.EventTimeMissing = &t, false
		flags = append(flags, FlagEventTimeDefaulted)
	}
	if env.EntityKey == "" {
		flags = append(flags, FlagEntityKeyMissing)
	}
	if len(env.Payload) == 0 {
		flags = append(flags, FlagPayloadByRef)
	}

	dk := env.DedupKey()
	if dk == "" { // unreachable after Validate; defensive -> unrecoverable
		return Result{Kind: KindDLQ, In: in, Code: CodeDedupKey, Reason: "dedup key could not be built"}
	}
	out := Processed{
		Envelope: env, PipelineVersion: n.PipelineVersion, EventType: env.SchemaID, QualityFlags: flags,
		RawRef: fmt.Sprintf("kafka:%s/%d/%d", in.Topic, in.Partition, in.Offset),
	}
	b, err := json.Marshal(out)
	if err != nil {
		return Result{Kind: KindDLQ, In: in, Code: CodeInternal, Reason: "marshal: " + err.Error()}
	}
	if n.MaxRecordBytes > 0 && len(b) > n.MaxRecordBytes {
		return Result{Kind: KindDLQ, In: in, Code: CodeRecordTooLarge,
			Reason: fmt.Sprintf("normalized record %d bytes exceeds cap %d", len(b), n.MaxRecordBytes)}
	}
	return Result{Kind: KindOut, In: in, Key: env.PartitionKey(), Value: b, DedupKey: dk}
}

func quarantine(in Input, code, reason string) Result {
	return Result{Kind: KindQuarantine, In: in, Code: code, Reason: reason}
}

// Batch is the routing plan for a polled batch.
type Batch struct {
	Results    []Result // same order as input
	Out        int
	Duplicates int
	Quarantine int
	DLQ        int
}

// Plan normalizes ins and applies dedup against w without mutating it (keys are
// committed with Commit only after the output produce succeeded, so a failed
// produce + retry cannot lose events to the window). Duplicates within the batch
// itself are detected too.
func Plan(n Normalizer, w *Window, ins []Input, now time.Time) Batch {
	b := Batch{Results: make([]Result, len(ins))}
	inBatch := make(map[string]struct{}, len(ins))
	for i, in := range ins {
		r := n.Normalize(in, now)
		if r.Kind == KindOut {
			_, dupLocal := inBatch[r.DedupKey]
			if dupLocal || w.Seen(r.DedupKey, now) {
				r.Kind, r.Value, r.Key = KindDuplicate, nil, nil
			} else {
				inBatch[r.DedupKey] = struct{}{}
			}
		}
		switch r.Kind {
		case KindOut:
			b.Out++
		case KindDuplicate:
			b.Duplicates++
		case KindQuarantine:
			b.Quarantine++
		case KindDLQ:
			b.DLQ++
		}
		b.Results[i] = r
	}
	return b
}

// Commit adds the dedup keys of emitted events to the window (call after a successful produce).
func (b Batch) Commit(w *Window, now time.Time) {
	for _, r := range b.Results {
		if r.Kind == KindOut {
			w.Add(r.DedupKey, now)
		}
	}
}
