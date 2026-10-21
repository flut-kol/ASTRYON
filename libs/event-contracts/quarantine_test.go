package eventcontracts

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

var quarantineNow = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

func validQuarantineRecord() QuarantineRecordV1 {
	original := NewQuarantineOriginalV1([]byte(`{"customer":"potentially-sensitive"}`))
	return QuarantineRecordV1{
		ContractVersion:   QuarantineContractVersionV1,
		DeviationID:       "10000000-0000-8000-8000-000000000001",
		TenantID:          "10000000-0000-4000-8000-000000000002",
		SourceID:          "10000000-0000-4000-8000-000000000003",
		EventID:           "10000000-0000-4000-8000-000000000004",
		BatchID:           "10000000-0000-4000-8000-000000000005",
		JobID:             "10000000-0000-4000-8000-000000000006",
		ProcessingVersion: ptr(uint32(2)),
		PipelineVersion:   ptr(uint32(3)),
		QuarantinedAt:     quarantineNow,
		Stage:             "flink-processing",
		ReasonCode:        "ERR_SCHEMA_INVALID",
		Diagnostic:        "schema validation failed",
		Provenance: QuarantineProvenanceV1{
			SourceTopic: "raw.events.v1", SourcePartition: ptr(int32(7)), SourceOffset: ptr(int64(91)),
		},
		Original: &original,
	}
}

func TestQuarantineRecordV1JSONContract(t *testing.T) {
	record := validQuarantineRecord()
	b, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"contract_version":1`, `"deviation_id":"10000000-0000-8000-8000-000000000001"`,
		`"processing_version":2`, `"quarantined_at":"2026-10-21T12:00:00Z"`,
		`"provenance":{"source_topic":"raw.events.v1","source_partition":7,"source_offset":91}`,
		`"original":{"encoding":"base64"`, `"truncated":false`,
	} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing %s in %s", want, b)
		}
	}
	decoded, err := DecodeQuarantineRecordV1At(b, quarantineNow)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.TenantID != record.TenantID || decoded.Original.Data != record.Original.Data {
		t.Fatalf("round trip mismatch: %+v", decoded)
	}
}

func TestQuarantineRecordV1OptionalFields(t *testing.T) {
	record := validQuarantineRecord()
	record.SourceID, record.EventID, record.BatchID, record.JobID = "", "", "", ""
	record.ProcessingVersion, record.PipelineVersion, record.Original = nil, nil, nil
	record.Provenance.SourcePartition, record.Provenance.SourceOffset = nil, nil
	if err := record.ValidateAt(quarantineNow); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(record)
	for _, absent := range []string{"source_id", "event_id", "batch_id", "job_id", "processing_version", "pipeline_version", "original", "source_partition", "source_offset"} {
		if strings.Contains(string(b), `"`+absent+`"`) {
			t.Fatalf("optional %s was emitted: %s", absent, b)
		}
	}
}

func TestQuarantineRecordV1Validation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*QuarantineRecordV1)
		want   string
	}{
		{"version", func(q *QuarantineRecordV1) { q.ContractVersion = 2 }, "contract_version"},
		{"tenant required uuid", func(q *QuarantineRecordV1) { q.TenantID = "" }, "tenant_id"},
		{"optional source uuid", func(q *QuarantineRecordV1) { q.SourceID = "bad" }, "source_id"},
		{"optional event uuid", func(q *QuarantineRecordV1) { q.EventID = "01ARZ3NDEKTSV4RRFFQ69G5FAV" }, "event_id"},
		{"processing version zero", func(q *QuarantineRecordV1) { q.ProcessingVersion = ptr(uint32(0)) }, "processing_version"},
		{"pipeline version zero", func(q *QuarantineRecordV1) { q.PipelineVersion = ptr(uint32(0)) }, "pipeline_version"},
		{"stage required", func(q *QuarantineRecordV1) { q.Stage = "" }, "stage"},
		{"stage bounded", func(q *QuarantineRecordV1) { q.Stage = strings.Repeat("a", 65) }, "stage"},
		{"code shape", func(q *QuarantineRecordV1) { q.ReasonCode = "bad-code" }, "reason_code"},
		{"diagnostic bounded", func(q *QuarantineRecordV1) { q.Diagnostic = strings.Repeat("a", MaxQuarantineDiagnosticBytes+1) }, "diagnostic"},
		{"diagnostic controls", func(q *QuarantineRecordV1) { q.Diagnostic = "unsafe\nlog" }, "diagnostic"},
		{"time utc", func(q *QuarantineRecordV1) {
			q.QuarantinedAt = time.Date(2026, 10, 21, 15, 0, 0, 0, time.FixedZone("EEST", 3*3600))
		}, "quarantined_at"},
		{"topic required", func(q *QuarantineRecordV1) { q.Provenance.SourceTopic = "" }, "provenance.source_topic"},
		{"partition pair", func(q *QuarantineRecordV1) { q.Provenance.SourceOffset = nil }, "source_partition and source_offset"},
		{"negative partition", func(q *QuarantineRecordV1) { q.Provenance.SourcePartition = ptr(int32(-1)) }, "source_partition"},
		{"negative offset", func(q *QuarantineRecordV1) { q.Provenance.SourceOffset = ptr(int64(-1)) }, "source_offset"},
		{"original encoding", func(q *QuarantineRecordV1) { q.Original.Encoding = "raw" }, "original.encoding"},
		{"original base64", func(q *QuarantineRecordV1) { q.Original.Data = "***" }, "original.data"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record := validQuarantineRecord()
			tc.mutate(&record)
			err := record.ValidateAt(quarantineNow)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestDecodeQuarantineRecordV1IsStrict(t *testing.T) {
	record := validQuarantineRecord()
	b, _ := json.Marshal(record)

	unknown := append([]byte(nil), b[:len(b)-1]...)
	unknown = append(unknown, []byte(`,"admin":true}`)...)
	if _, err := DecodeQuarantineRecordV1(unknown); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field err=%v", err)
	}
	if _, err := DecodeQuarantineRecordV1(append(b, []byte(` {}`)...)); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing value err=%v", err)
	}
	if _, err := DecodeQuarantineRecordV1([]byte(`null`)); err == nil {
		t.Fatal("null accepted")
	}
	if _, err := DecodeQuarantineRecordV1(make([]byte, MaxQuarantineRecordJSONBytes+1)); err == nil {
		t.Fatal("oversized JSON accepted")
	}

	nestedUnknown := strings.Replace(string(b), `"source_topic":"raw.events.v1"`, `"source_topic":"raw.events.v1","admin":true`, 1)
	if _, err := DecodeQuarantineRecordV1At([]byte(nestedUnknown), quarantineNow); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("nested unknown field err=%v", err)
	}
	for _, invalid := range []string{"-1", "4294967296", "1.5"} {
		wire := strings.Replace(string(b), `"processing_version":2`, `"processing_version":`+invalid, 1)
		if _, err := DecodeQuarantineRecordV1At([]byte(wire), quarantineNow); err == nil {
			t.Fatalf("invalid UInt32 %s accepted", invalid)
		}
	}
}

func TestNewQuarantineOriginalV1BoundsAndCopies(t *testing.T) {
	raw := make([]byte, MaxQuarantineOriginalBytes+1)
	for i := range raw {
		raw[i] = byte(i)
	}
	original := NewQuarantineOriginalV1(raw)
	if !original.Truncated || original.Encoding != QuarantineOriginalEncodingBase64 {
		t.Fatalf("original=%+v", original)
	}
	record := validQuarantineRecord()
	record.Original = &original
	if err := record.ValidateAt(quarantineNow); err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 0xff
	if original.Data != NewQuarantineOriginalV1(makePattern(MaxQuarantineOriginalBytes)).Data {
		t.Fatal("constructor retained caller-owned storage")
	}

	tooLarge := QuarantineOriginalV1{
		Encoding: QuarantineOriginalEncodingBase64,
		Data:     base64.StdEncoding.EncodeToString(make([]byte, MaxQuarantineOriginalBytes+1)),
	}
	record.Original = &tooLarge
	if err := record.ValidateAt(quarantineNow); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized decoded original err=%v", err)
	}
}

func makePattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func TestQuarantineDeviationIdentity(t *testing.T) {
	a, err := QuarantineDeviationIDFromOffset("flink-processing", "raw.events.v1", 7, 91)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := QuarantineDeviationIDFromOffset("flink-processing", "raw.events.v1", 7, 91)
	c, _ := QuarantineDeviationIDFromOffset("flink-processing", "raw.events.v1", 7, 92)
	if a != b || a == c || !IsUUID(a) || a[14] != '8' {
		t.Fatalf("a=%q b=%q c=%q", a, b, c)
	}
	keyA, err := QuarantineDeviationIDFromKey("gateway", "tenant/request/0")
	if err != nil || !IsUUID(keyA) {
		t.Fatalf("key id=%q err=%v", keyA, err)
	}
	keyB, _ := QuarantineDeviationIDFromKey("gateway", "tenant/request/0")
	if keyA != keyB || keyA == a {
		t.Fatalf("key identity unstable or colliding: %q %q %q", keyA, keyB, a)
	}
	for _, tc := range []struct {
		name string
		fn   func() error
	}{
		{"empty producer", func() error { _, err := QuarantineDeviationIDFromOffset("", "raw.events.v1", 0, 0); return err }},
		{"bad topic", func() error { _, err := QuarantineDeviationIDFromOffset("p", "bad topic", 0, 0); return err }},
		{"negative coordinate", func() error { _, err := QuarantineDeviationIDFromOffset("p", "raw.events.v1", -1, 0); return err }},
		{"empty key", func() error { _, err := QuarantineDeviationIDFromKey("gateway", ""); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); err == nil {
				t.Fatal("invalid identity accepted")
			}
		})
	}
}

func TestContractViolationsAreTypedAndAggregated(t *testing.T) {
	record := validQuarantineRecord()
	record.TenantID = "bad"
	record.ReasonCode = "bad"
	err := record.ValidateAt(quarantineNow)
	var violations ContractViolations
	if !errors.As(err, &violations) || len(violations) < 2 {
		t.Fatalf("err=%T %v", err, err)
	}
}

func FuzzDecodeQuarantineRecordV1(f *testing.F) {
	b, _ := json.Marshal(validQuarantineRecord())
	f.Add(b)
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeQuarantineRecordV1(data)
	})
}
