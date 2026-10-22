package sink

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
)

const qTenant = "11111111-1111-4111-8111-111111111111"

func TestQuarantineRecords_TenantBearingIsStrictV1(t *testing.T) {
	now := time.Now()
	val := []byte(`{"tenant_id":"` + strings.ToUpper(qTenant) + `","event_id":"x"}`)
	q := []Quarantined{{Code: "ERR_EVENT_ID_NOT_UUID", Reason: "event_id must be a UUID", Msg: Message{Topic: "processed.events.v1", Partition: 3, Offset: 42, Value: val}}}
	recs, err := quarantineRecords(q, now, "events.quarantine.v1", "events.dlq.v1")
	if err != nil || len(recs) != 1 || recs[0].Topic != "events.quarantine.v1" {
		t.Fatalf("%v %+v", err, recs)
	}
	r, err := ec.DecodeQuarantineRecordV1At(recs[0].Value, now)
	if err != nil {
		t.Fatalf("not strict: %v", err)
	}
	want, _ := ec.QuarantineDeviationIDFromOffset("clickhouse-sink", "processed.events.v1", 3, 42)
	dec, _ := base64.StdEncoding.DecodeString(r.Original.Data)
	if r.TenantID != qTenant || r.DeviationID != want || r.Stage != "clickhouse-sink" || r.ReasonCode != "ERR_EVENT_ID_NOT_UUID" ||
		*r.Provenance.SourcePartition != 3 || *r.Provenance.SourceOffset != 42 || !bytes.Equal(dec, val) {
		t.Fatalf("record: %+v", r)
	}
	again, _ := quarantineRecords(q, now.Add(time.Minute), "events.quarantine.v1", "events.dlq.v1")
	if string(again[0].Key) != string(recs[0].Key) {
		t.Fatal("key must be deterministic across retries")
	}
}

func TestQuarantineRecords_NonJSONAndOddCodes(t *testing.T) {
	now := time.Now()
	// not JSON: no tenant -> DLQ, no original copied
	recs, err := quarantineRecords([]Quarantined{{Code: "it", Reason: "bad\nline", Msg: Message{Topic: "src", Value: []byte("secret not json")}}}, now, "q", "d")
	if err != nil || recs[0].Topic != "d" {
		t.Fatalf("%v %+v", err, recs)
	}
	var m map[string]any
	if json.Unmarshal(recs[0].Value, &m) != nil || m["reason_code"] != "IT" || m["dead_letter_id"] == "" || strings.Contains(string(recs[0].Value), "secret") {
		t.Fatalf("dlq: %s", recs[0].Value)
	}
	// non-UUID tenant is never trusted
	recs, _ = quarantineRecords([]Quarantined{{Code: "ERR_X", Msg: Message{Topic: "src", Value: []byte(`{"tenant_id":"nope"}`)}}}, now, "q", "d")
	if recs[0].Topic != "d" {
		t.Fatalf("topic %s", recs[0].Topic)
	}
	// invalid source topic is an error, not a silent drop
	if _, err := quarantineRecords([]Quarantined{{Code: "ERR_X", Msg: Message{Topic: "bad topic"}}}, now, "q", "d"); err == nil {
		t.Fatal("want error")
	}
}
