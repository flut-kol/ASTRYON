package processor

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
)

const (
	tenant = "11111111-1111-4111-8111-111111111111"
	source = "22222222-2222-4222-8222-222222222222"
)

var now = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func evJSON(t testing.TB, id string, mod func(*ec.Envelope)) []byte {
	t.Helper()
	p := json.RawMessage(`{"v":1}`)
	et := now.Add(-time.Minute)
	e := ec.Envelope{EventID: id, TenantID: tenant, SourceID: source, SchemaID: "sensor.reading", SchemaVersion: 1,
		EventTime: &et, IngestedAt: now.Add(-time.Second), EntityKey: "dev-1", TraceID: "tr-1",
		PayloadFormat: ec.FormatJSON, Payload: p, Checksum: ec.ChecksumOf(p), ProcessingVersion: 1}
	if mod != nil {
		mod(&e)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

const id1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const id2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

var nz = Normalizer{PipelineVersion: 3, MaxRecordBytes: 1 << 20}

func in(v []byte) Input {
	return Input{Topic: "raw.events.v1", Partition: 2, Offset: 7, Key: []byte("k"), Value: v}
}

func TestNormalizeOK(t *testing.T) {
	r := nz.Normalize(in(evJSON(t, strings.ToUpper(id1), nil)), now)
	if r.Kind != KindOut {
		t.Fatalf("%+v", r)
	}
	var p Processed
	if err := json.Unmarshal(r.Value, &p); err != nil {
		t.Fatal(err)
	}
	if p.PipelineVersion != 3 || p.ProcessingVersion != 1 || p.RawRef != "kafka:raw.events.v1/2/7" || p.EventType != "sensor.reading" {
		t.Fatalf("%+v", p)
	}
	if p.EventID != id1 {
		t.Fatalf("event id not canonical: %s", p.EventID)
	}
	if p.QualityFlags == nil || len(p.QualityFlags) != 0 {
		t.Fatalf("flags %v", p.QualityFlags)
	}
	if string(r.Key) != string(ec.PartitionKey(tenant, "dev-1")) {
		t.Fatalf("key %q", r.Key)
	}
	if r.DedupKey != tenant+"/"+source+"/"+id1+"/v1" {
		t.Fatal(r.DedupKey)
	}
	// Output must itself pass the contract (the sink revalidates it).
	if err := p.Envelope.ValidateAt(now); err != nil {
		t.Fatal(err)
	}
	// Deterministic: same input, same bytes (idempotent reprocessing).
	if r2 := nz.Normalize(in(evJSON(t, strings.ToUpper(id1), nil)), now); string(r2.Value) != string(r.Value) {
		t.Fatal("non-deterministic output")
	}
}

func TestNormalizeQualityFlags(t *testing.T) {
	r := nz.Normalize(in(evJSON(t, id1, func(e *ec.Envelope) { e.EventTime, e.EventTimeMissing, e.EntityKey = nil, true, "" })), now)
	if r.Kind != KindOut {
		t.Fatalf("%+v", r)
	}
	var p Processed
	_ = json.Unmarshal(r.Value, &p)
	if p.EventTime == nil || p.EventTimeMissing || !p.EventTime.Equal(p.IngestedAt) {
		t.Fatalf("event_time not defaulted: %+v", p.Envelope)
	}
	got := strings.Join(p.QualityFlags, ",")
	if !strings.Contains(got, FlagEventTimeDefaulted) || !strings.Contains(got, FlagEntityKeyMissing) {
		t.Fatal(got)
	}
	if err := p.Envelope.ValidateAt(now); err != nil {
		t.Fatal(err)
	}
	// no entity key -> event_id spreads the partition key
	if string(r.Key) != string(ec.PartitionKey(tenant, id1)) {
		t.Fatal("key fallback")
	}
}

func TestNormalizeQuarantine(t *testing.T) {
	cases := map[string]struct {
		v    []byte
		code string
	}{
		"not json":    {[]byte("{nope"), CodeJSONMalformed},
		"bad tenant":  {evJSON(t, id1, func(e *ec.Envelope) { e.TenantID = "x" }), string(ec.ErrTenantIDInvalid)},
		"bad payload": {evJSON(t, id1, func(e *ec.Envelope) { e.Checksum = strings.Repeat("0", 64) }), string(ec.ErrChecksumMismatch)},
		"future":      {evJSON(t, id1, func(e *ec.Envelope) { f := now.Add(time.Hour); e.EventTime = &f }), string(ec.ErrEventTimeFuture)},
	}
	for name, c := range cases {
		r := nz.Normalize(in(c.v), now)
		if r.Kind != KindQuarantine || r.Code != c.code {
			t.Errorf("%s: %+v", name, r)
		}
		if strings.Contains(r.Reason, `"v":1`) {
			t.Errorf("%s: reason leaks payload", name)
		}
	}
}

func TestNormalizeDLQTooLarge(t *testing.T) {
	small := Normalizer{PipelineVersion: 1, MaxRecordBytes: 50}
	r := small.Normalize(in(evJSON(t, id1, nil)), now)
	if r.Kind != KindDLQ || r.Code != CodeRecordTooLarge {
		t.Fatalf("%+v", r)
	}
}

func TestPlanDedup(t *testing.T) {
	w := NewWindow(time.Hour, 100)
	a, a2, b := evJSON(t, id1, nil), evJSON(t, strings.ToUpper(id1), nil), evJSON(t, id2, nil)
	p := Plan(nz, w, []Input{in(a), in(b), in(a2), in([]byte("x"))}, now)
	if p.Out != 2 || p.Duplicates != 1 || p.Quarantine != 1 {
		t.Fatalf("%+v", p)
	}
	if w.Stats().Entries != 0 {
		t.Fatal("Plan must not mutate state")
	}
	p.Commit(w, now)
	p2 := Plan(nz, w, []Input{in(a), in(b)}, now.Add(time.Minute))
	if p2.Duplicates != 2 || p2.Out != 0 {
		t.Fatalf("%+v", p2)
	}
	// outside the window: accepted again (bounded-window semantics)
	p3 := Plan(nz, w, []Input{in(a)}, now.Add(2*time.Hour))
	if p3.Out != 1 {
		t.Fatalf("%+v", p3)
	}
	// different processing_version is a different dedup key (replay does not collide)
	v2 := evJSON(t, id1, func(e *ec.Envelope) { e.ProcessingVersion = 2 })
	if p4 := Plan(nz, w, []Input{in(v2)}, now.Add(time.Minute)); p4.Out != 1 {
		t.Fatalf("%+v", p4)
	}
}
