package replay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const journalRecord = `{"contract_version":1,"deviation_id":"00000000-0000-4000-8000-000000000001","tenant_id":"00000000-0000-4000-8000-0000000000a1","source_id":"00000000-0000-4000-8000-000000000002","event_id":"00000000-0000-4000-8000-000000000003","quarantined_at":"2026-10-22T07:00:00Z","stage":"flink-processing","reason_code":"ERR_SCHEMA_VERSION_INVALID","diagnostic":"envelope validation failed","provenance":{"source_topic":"raw.events.v1","source_partition":1,"source_offset":42},"original":{"encoding":"base64","data":"c2FmZQ==","truncated":false}}`

func TestDecodeJournalRecordUsesZeroesForMissingOptionalIdentifiers(t *testing.T) {
	row, err := DecodeJournalRecord([]byte(journalRecord), time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if row.TenantID != "00000000-0000-4000-8000-0000000000a1" || row.SourceID != "00000000-0000-4000-8000-000000000002" ||
		row.EventID != "00000000-0000-4000-8000-000000000003" || row.BatchID != zeroUUID || row.JobID != zeroUUID {
		t.Fatalf("row=%+v", row)
	}
	if row.SamplePayload != "" || row.RawRef != "kafka:raw.events.v1/1/42" || row.ErrorMessage != "envelope validation failed" {
		t.Fatalf("opaque data escaped journal row: %+v", row)
	}
	if got := journalToken("events.quarantine.v1", 1, 42); len(got) != 64 {
		t.Fatalf("token=%q", got)
	}
}

func TestDecodeJournalRecordRejectsInvalidCanonicalRecord(t *testing.T) {
	bad := strings.Replace(journalRecord, `"tenant_id":"00000000-0000-4000-8000-0000000000a1"`, `"tenant_id":"not-a-tenant"`, 1)
	if _, err := DecodeJournalRecord([]byte(bad), time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("invalid tenant was accepted")
	}
}

func TestClickHouseJournalInsertIsDeduplicatedAndDoesNotCopyOriginal(t *testing.T) {
	var query, body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("query")
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	store := &ClickHouseStore{URL: server.URL, Database: "astryon", Table: "processing_errors_local", HTTP: server.Client()}
	row, err := DecodeJournalRecord([]byte(journalRecord), time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if err := (ClickHouseJournal{Store: store}).InsertQuarantine(context.Background(), row, "token-1"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "INSERT INTO processing_errors_local") || !strings.Contains(query, "FORMAT JSONEachRow") ||
		!strings.Contains(body, `"sample_payload":""`) || strings.Contains(body, "c2FmZQ==") {
		t.Fatalf("query=%q body=%q", query, body)
	}
}

func TestNewJournalConsumerFailsClosedWithoutBrokerOrSink(t *testing.T) {
	if _, err := NewJournalConsumer(KafkaConfig{}, "group", ClickHouseJournal{}); err == nil || !strings.Contains(err.Error(), "brokers") {
		t.Fatalf("missing broker error = %v", err)
	}
	if _, err := NewJournalConsumer(KafkaConfig{Brokers: []string{"127.0.0.1:1"}}, "group", nil); err == nil || !strings.Contains(err.Error(), "sink") {
		t.Fatalf("missing sink error = %v", err)
	}
}

func TestNewJournalConsumerBlocksRebalanceDuringAcknowledgement(t *testing.T) {
	consumer, err := NewJournalConsumer(KafkaConfig{Brokers: []string{"127.0.0.1:1"}}, "group", ClickHouseJournal{Store: &ClickHouseStore{URL: "http://127.0.0.1:8123"}})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if got := consumer.client.OptValue(kgo.BlockRebalanceOnPoll); got != true {
		t.Fatalf("BlockRebalanceOnPoll=%v", got)
	}
}

func TestJournalRejectIDIsDeterministicUUID(t *testing.T) {
	record := &kgo.Record{Topic: TopicQuarantineJournal, Partition: 3, Offset: 91}
	first := journalRejectID(record)
	if first != journalRejectID(record) || len(first) != 36 || first[14] != '8' || (first[19] != '8' && first[19] != '9' && first[19] != 'a' && first[19] != 'b') {
		t.Fatalf("reject id=%q", first)
	}
}

func TestJournalRejectBodyDoesNotCopyOpaquePayloadOrError(t *testing.T) {
	record := &kgo.Record{Topic: TopicQuarantineJournal, Partition: 3, Offset: 91, Value: []byte("secret-password=do-not-copy")}
	body, err := journalRejectBody(record)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if strings.Contains(text, "secret-password") || strings.Contains(text, "original") || strings.Contains(text, "base64") {
		t.Fatalf("opaque payload leaked: %s", text)
	}
	if !strings.Contains(text, `"diagnostic":"invalid quarantine journal record"`) || !strings.Contains(text, `"source_offset":91`) {
		t.Fatalf("bounded rejection missing safe metadata: %s", text)
	}
}
