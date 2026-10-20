package eventcontracts

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

var now = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

const (
	tid = "11111111-2222-4333-8444-555555555555"
	sid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
)

func valid() Envelope {
	et := now.Add(-time.Minute)
	p := json.RawMessage(`{"a":1}`)
	return Envelope{
		EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", TenantID: tid, SourceID: sid,
		SchemaID: "sensor.reading", SchemaVersion: 1, EventTime: &et, IngestedAt: now,
		EntityKey: "device-1", TraceID: "trace-1", PayloadFormat: FormatJSON,
		Payload: p, Checksum: ChecksumOf(p), ProcessingVersion: 1,
	}
}

func TestValidate(t *testing.T) {
	future := now.Add(time.Hour)
	past := now.Add(-20 * 366 * 24 * time.Hour)
	plus2 := now.In(time.FixedZone("x", 7200))
	big := make([]byte, MaxInlinePayloadBytes+1)
	for i := range big {
		big[i] = ' '
	}
	cases := []struct {
		name string
		mut  func(*Envelope)
		want Code // "" = valid
	}{
		{"valid ulid", func(e *Envelope) {}, ""},
		{"valid uuid", func(e *Envelope) { e.EventID = "123e4567-e89b-12d3-a456-426614174000" }, ""},
		{"valid no checksum", func(e *Envelope) { e.Checksum = "" }, ""},
		{"valid event_time missing marker", func(e *Envelope) { e.EventTime = nil; e.EventTimeMissing = true }, ""},
		{"valid object_ref", func(e *Envelope) {
			e.Payload = nil
			e.ObjectRef = "s3://bucket/a/b.parquet"
			e.PayloadFormat = FormatParquetReference
			e.Checksum = ChecksumOf([]byte("x"))
		}, ""},
		{"empty event_id", func(e *Envelope) { e.EventID = "" }, ErrEventIDInvalid},
		{"bad event_id", func(e *Envelope) { e.EventID = "not-an-id" }, ErrEventIDInvalid},
		{"ulid with I", func(e *Envelope) { e.EventID = "01ARZ3NDEKTSV4RRFFQ69G5FAI" }, ErrEventIDInvalid},
		{"ulid overflow", func(e *Envelope) { e.EventID = "81ARZ3NDEKTSV4RRFFQ69G5FAV" }, ErrEventIDInvalid},
		{"bad tenant", func(e *Envelope) { e.TenantID = "tenant" }, ErrTenantIDInvalid},
		{"ulid tenant rejected", func(e *Envelope) { e.TenantID = "01ARZ3NDEKTSV4RRFFQ69G5FAV" }, ErrTenantIDInvalid},
		{"bad source", func(e *Envelope) { e.SourceID = "" }, ErrSourceIDInvalid},
		{"empty schema_id", func(e *Envelope) { e.SchemaID = "" }, ErrSchemaIDInvalid},
		{"schema_id control char", func(e *Envelope) { e.SchemaID = "a\nb" }, ErrSchemaIDInvalid},
		{"schema_version 0", func(e *Envelope) { e.SchemaVersion = 0 }, ErrSchemaVersionInvalid},
		{"event_time absent", func(e *Envelope) { e.EventTime = nil }, ErrEventTimeMissing},
		{"event_time and marker", func(e *Envelope) { e.EventTimeMissing = true }, ErrEventTimeMissing},
		{"event_time non-UTC", func(e *Envelope) { e.EventTime = &plus2 }, ErrEventTimeNotUTC},
		{"event_time future", func(e *Envelope) { e.EventTime = &future }, ErrEventTimeFuture},
		{"event_time ancient", func(e *Envelope) { e.EventTime = &past }, ErrEventTimeTooOld},
		{"ingested_at zero", func(e *Envelope) { e.IngestedAt = time.Time{} }, ErrIngestedAtInvalid},
		{"ingested_at non-UTC", func(e *Envelope) { e.IngestedAt = plus2 }, ErrIngestedAtInvalid},
		{"entity_key too long", func(e *Envelope) { e.EntityKey = string(make([]byte, MaxEntityKeyLen+1)) }, ErrEntityKeyInvalid},
		{"entity_key control", func(e *Envelope) { e.EntityKey = "a\x00b" }, ErrEntityKeyInvalid},
		{"empty trace", func(e *Envelope) { e.TraceID = "" }, ErrTraceIDInvalid},
		{"bad format", func(e *Envelope) { e.PayloadFormat = "xml" }, ErrPayloadFormatInvalid},
		{"neither payload nor ref", func(e *Envelope) { e.Payload = nil; e.Checksum = "" }, ErrPayloadXorObjectRef},
		{"both payload and ref", func(e *Envelope) { e.ObjectRef = "s3://b/k" }, ErrPayloadXorObjectRef},
		{"parquet-reference w/o ref", func(e *Envelope) { e.PayloadFormat = FormatParquetReference }, ErrPayloadXorObjectRef},
		{"payload malformed", func(e *Envelope) { e.Payload = json.RawMessage(`{"a":`); e.Checksum = "" }, ErrPayloadMalformed},
		{"payload null", func(e *Envelope) { e.Payload = json.RawMessage(`null`); e.Checksum = "" }, ErrPayloadMalformed},
		{"payload too large", func(e *Envelope) { e.Payload = big; e.Checksum = "" }, ErrPayloadTooLarge},
		{"ref wrong scheme", func(e *Envelope) { e.Payload = nil; e.ObjectRef = "https://x/y"; e.Checksum = ChecksumOf(nil) }, ErrObjectRefInvalid},
		{"ref no key", func(e *Envelope) { e.Payload = nil; e.ObjectRef = "s3://bucket"; e.Checksum = ChecksumOf(nil) }, ErrObjectRefInvalid},
		{"ref traversal", func(e *Envelope) { e.Payload = nil; e.ObjectRef = "s3://b/a/../c"; e.Checksum = ChecksumOf(nil) }, ErrObjectRefInvalid},
		{"ref query", func(e *Envelope) { e.Payload = nil; e.ObjectRef = "s3://b/k?x=1"; e.Checksum = ChecksumOf(nil) }, ErrObjectRefInvalid},
		{"ref without checksum", func(e *Envelope) { e.Payload = nil; e.ObjectRef = "s3://b/k"; e.Checksum = "" }, ErrChecksumRequired},
		{"checksum uppercase", func(e *Envelope) { e.Checksum = "ABC" }, ErrChecksumInvalid},
		{"checksum mismatch", func(e *Envelope) { e.Checksum = ChecksumOf([]byte("other")) }, ErrChecksumMismatch},
		{"processing_version 0", func(e *Envelope) { e.ProcessingVersion = 0 }, ErrProcessingVersionInval},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := valid()
			c.mut(&e)
			err := e.ValidateAt(now)
			if c.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !HasCode(err, c.want) {
				t.Fatalf("want %s, got %v", c.want, err)
			}
			var es Errors
			if !errors.As(err, &es) || len(es.Codes()) == 0 {
				t.Fatalf("expected Errors, got %T", err)
			}
		})
	}
}

func TestValidateCollectsAllErrors(t *testing.T) {
	err := (&Envelope{}).ValidateAt(now)
	var es Errors
	if !errors.As(err, &es) || len(es) < 8 {
		t.Fatalf("expected many errors, got %v", err)
	}
}

func TestJSONRoundTripAndMalformedWire(t *testing.T) {
	e := valid()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var back Envelope
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if err := back.ValidateAt(now); err != nil {
		t.Fatalf("round trip invalid: %v", err)
	}
	// event_time with +00:00 offset must still count as UTC; +02:00 must not.
	var x Envelope
	json.Unmarshal([]byte(`{"event_time":"2026-10-20T11:00:00+02:00"}`), &x)
	if err := x.ValidateAt(now); !HasCode(err, ErrEventTimeNotUTC) {
		t.Fatalf("want not-UTC, got %v", err)
	}
	if json.Unmarshal([]byte(`{"schema_version":"1"}`), &x) == nil {
		t.Fatal("type mismatch should fail decoding")
	}
}

func TestDedupKey(t *testing.T) {
	k := DedupKey(tid, sid, "01arz3ndektsv4rrffq69g5fav", 2)
	if k != tid+"/"+sid+"/01ARZ3NDEKTSV4RRFFQ69G5FAV/v2" {
		t.Fatalf("got %q", k)
	}
	if DedupKey(tid, sid, "01ARZ3NDEKTSV4RRFFQ69G5FAV", 2) != k {
		t.Fatal("case variants must collide")
	}
	if DedupKey(tid, sid, "01ARZ3NDEKTSV4RRFFQ69G5FAV", 3) == k {
		t.Fatal("pipeline version must differ")
	}
	for _, bad := range [][4]any{{"x", sid, "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1}, {tid, "", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1}, {tid, sid, "bad", 1}, {tid, sid, "01ARZ3NDEKTSV4RRFFQ69G5FAV", 0}} {
		if DedupKey(bad[0].(string), bad[1].(string), bad[2].(string), bad[3].(int)) != "" {
			t.Fatalf("expected empty for %v", bad)
		}
	}
	e := valid()
	if e.DedupKey() == "" {
		t.Fatal("envelope dedup key empty")
	}
}

func TestTopics(t *testing.T) {
	if len(Topics) != 8 {
		t.Fatal("want 8 topics")
	}
	raw, ok := TopicByName(TopicRawEvents)
	if !ok || raw.Partitions != 96 || raw.Retention != 7*24*time.Hour || raw.ReplicationFactor != 3 || raw.MinInsyncReplicas() != 2 {
		t.Fatalf("raw topic wrong: %+v", raw)
	}
	dlq, _ := TopicByName(TopicDLQ)
	if dlq.Partitions != 24 || dlq.Retention != 14*24*time.Hour {
		t.Fatalf("dlq wrong: %+v", dlq)
	}
	if _, ok := TopicByName("nope"); ok {
		t.Fatal("unexpected topic")
	}
}

func TestPartitionKey(t *testing.T) {
	if string(PartitionKey("ab", "c")) == string(PartitionKey("a", "bc")) {
		t.Fatal("ambiguous key")
	}
	e := valid()
	if string(e.PartitionKey()) != tid+"\x1fdevice-1" {
		t.Fatalf("got %q", e.PartitionKey())
	}
	e.EntityKey = ""
	if string(e.PartitionKey()) != tid+"\x1f"+e.EventID {
		t.Fatal("fallback should use event_id")
	}
	// same entity -> same partition; many entities of one tenant spread out.
	if PartitionFor(PartitionKey(tid, "e"), 96) != PartitionFor(PartitionKey(tid, "e"), 96) {
		t.Fatal("unstable")
	}
	counts := make([]int, 96)
	for i := 0; i < 96000; i++ {
		counts[PartitionFor(PartitionKey(tid, "entity-"+itoa(i)), 96)]++
	}
	for p, c := range counts {
		if c < 700 || c > 1300 {
			t.Fatalf("partition %d skewed: %d", p, c)
		}
	}
	if PartitionFor([]byte("x"), 0) != 0 {
		t.Fatal("n<=0")
	}
}

func itoa(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}
