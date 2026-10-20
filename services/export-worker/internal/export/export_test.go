package export

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestRunSuccessTenantPrefixAndAudit(t *testing.T) {
	ch := newFakeCH(ndjsonRows(5), 200)
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	r := newRunner(ch, st, au)
	res, err := r.Run(context.Background(), Principal{TenantID: tenantA, Subject: "u1"}, exportID, goodSpec())
	if err != nil {
		t.Fatal(err)
	}
	wantKey := "exports/" + tenantA + "/" + exportID + ".ndjson"
	if res.Key != wantKey || len(st.puts) != 1 || st.puts[0] != wantKey {
		t.Fatalf("key %q puts %v", res.Key, st.puts)
	}
	if res.Rows != 5 || res.Bytes != int64(len(ndjsonRows(5))) || !strings.Contains(res.DownloadURL, "X-Amz-Signature=") {
		t.Fatalf("%+v", res)
	}
	if got := ch.lastParams.Get("param_p_tenant"); got != tenantA {
		t.Fatalf("tenant param %q", got)
	}
	if ch.lastParams.Get("readonly") != "1" || !strings.Contains(ch.lastQuery, "LIMIT 1 BY tenant_id, source_id, event_id, processing_version") ||
		!strings.Contains(ch.lastQuery, "LIMIT 101 FORMAT JSONEachRow") || ch.lastUser != "u" {
		t.Fatalf("query/params: %s %v", ch.lastQuery, ch.lastParams)
	}
	recs := au.Records()
	if len(recs) != 1 || recs[0].Outcome != OutcomeSuccess || recs[0].TenantID != tenantA || recs[0].Rows != 5 ||
		recs[0].Bytes != res.Bytes || recs[0].SpecHash == "" || recs[0].ExportID != exportID {
		t.Fatalf("%+v", recs)
	}
}

func TestQueryNeverInlinesValues(t *testing.T) {
	spec := goodSpec()
	spec.EventType = "x' OR 1=1 --"
	spec.EntityID = "e;DROP TABLE t"
	spec, _ = spec.Normalize()
	q, params := buildQuery("t", tenantA, spec)
	if strings.Contains(q, "DROP") || strings.Contains(q, "OR 1=1") || strings.Contains(q, tenantA) {
		t.Fatal(q)
	}
	if params.Get("param_p_type") != spec.EventType || params.Get("param_p_entity") != spec.EntityID {
		t.Fatal(params)
	}
}

func TestSpecValidation(t *testing.T) {
	good := goodSpec()
	cases := map[string]func(*ExportSpec){
		"format":  func(s *ExportSpec) { s.Format = "xml" },
		"range":   func(s *ExportSpec) { s.To = s.From },
		"31 days": func(s *ExportSpec) { s.To = s.From.Add(32 * 24 * time.Hour) },
		"version": func(s *ExportSpec) { s.ProcessingVersion = nil },
		"source":  func(s *ExportSpec) { s.SourceID = "nope" },
		"rows":    func(s *ExportSpec) { s.MaxRows = HardMaxRows + 1 },
	}
	for name, mut := range cases {
		s := good
		mut(&s)
		if _, err := s.Normalize(); !errors.Is(err, ErrInvalidSpec) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := good.Normalize(); err != nil {
		t.Fatal(err)
	}
	// tenant is not an accepted field of the spec
	var s ExportSpec
	var m map[string]any
	b, _ := json.Marshal(s)
	_ = json.Unmarshal(b, &m)
	for k := range m {
		if strings.Contains(k, "tenant") {
			t.Fatalf("spec exposes %s", k)
		}
	}
}

func TestInvalidPrincipalAndExportID(t *testing.T) {
	ch := newFakeCH("", 200)
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	r := newRunner(ch, st, au)
	if _, err := r.Run(context.Background(), Principal{TenantID: "../other"}, exportID, goodSpec()); !errors.Is(err, ErrInvalidSpec) {
		t.Fatal(err)
	}
	if _, err := r.Run(context.Background(), Principal{TenantID: tenantA}, "../../x", goodSpec()); !errors.Is(err, ErrInvalidSpec) {
		t.Fatal(err)
	}
	if len(st.puts) != 0 || len(au.Records()) != 2 {
		t.Fatal("must not store; both attempts audited")
	}
}

func TestKeyBelongsToTenant(t *testing.T) {
	ok := "exports/" + tenantA + "/a.csv"
	for key, want := range map[string]bool{
		ok:                              true,
		"exports/" + tenantB + "/a.csv": false,
		"exports/" + tenantA + "/../" + tenantB + "/a": false,
		"exports/" + tenantA + "/x/y.csv":              false,
		"exports/" + tenantA + "/":                     false,
		"other/" + tenantA + "/a.csv":                  false,
	} {
		if KeyBelongsToTenant(key, tenantA) != want {
			t.Errorf("%s want %v", key, want)
		}
	}
	r := &Runner{Presign: &S3Store{Endpoint: "http://h", Bucket: "b", Creds: awsCreds}}
	if _, _, err := r.DownloadURL(Principal{TenantID: tenantB}, ok, 0); err == nil {
		t.Fatal("cross-tenant download must fail")
	}
	if _, _, err := r.DownloadURL(Principal{TenantID: tenantA}, ok, 0); err != nil {
		t.Fatal(err)
	}
}

func TestRowCapAbortsAndDeletes(t *testing.T) {
	ch := newFakeCH(ndjsonRows(101), 200) // max_rows=100 -> the 101st row overflows
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	_, err := newRunner(ch, st, au).Run(context.Background(), Principal{TenantID: tenantA}, exportID, goodSpec())
	if !errors.Is(err, ErrRowCap) {
		t.Fatal(err)
	}
	assertCleaned(t, st, au, OutcomeRowCap)
}

func TestExactlyMaxRowsSucceeds(t *testing.T) {
	ch := newFakeCH(ndjsonRows(100), 200)
	defer ch.Close()
	if _, err := newRunner(ch, newMemStore(), &MemoryAudit{}).Run(context.Background(), Principal{TenantID: tenantA}, exportID, goodSpec()); err != nil {
		t.Fatal(err)
	}
}

func TestByteCapAbortsAndDeletes(t *testing.T) {
	ch := newFakeCH(ndjsonRows(50), 200)
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	r := newRunner(ch, st, au)
	r.MaxBytes = 200
	_, err := r.Run(context.Background(), Principal{TenantID: tenantA}, exportID, goodSpec())
	if !errors.Is(err, ErrByteCap) {
		t.Fatal(err)
	}
	assertCleaned(t, st, au, OutcomeByteCap)
}

func TestClickHouseErrorMidStream(t *testing.T) {
	body := ndjsonRows(3) + "Code: 241. DB::Exception: Memory limit (for query) exceeded\n"
	ch := newFakeCH(body, 200)
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	_, err := newRunner(ch, st, au).Run(context.Background(), Principal{TenantID: tenantA}, exportID, goodSpec())
	if !errors.Is(err, ErrTruncated) {
		t.Fatal(err)
	}
	assertCleaned(t, st, au, OutcomeFailed)
}

func TestClickHouseCSVErrorMidStream(t *testing.T) {
	body := "event_id,payload\n\"a\",\"multi\nline\"\nCode: 241. DB::Exception: boom\n"
	ch := newFakeCH(body, 200)
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	s := goodSpec()
	s.Format = "csv"
	_, err := newRunner(ch, st, au).Run(context.Background(), Principal{TenantID: tenantA}, exportID, s)
	if !errors.Is(err, ErrTruncated) {
		t.Fatal(err)
	}
	assertCleaned(t, st, au, OutcomeFailed)
}

func TestCSVCountsQuotedNewlinesAsOneRow(t *testing.T) {
	body := "event_id,payload\n\"a\",\"multi\nline\"\n\"b\",\"x\"\n"
	ch := newFakeCH(body, 200)
	defer ch.Close()
	s := goodSpec()
	s.Format, s.MaxRows = "csv", 2
	res, err := newRunner(ch, newMemStore(), &MemoryAudit{}).Run(context.Background(), Principal{TenantID: tenantA}, exportID, s)
	if err != nil || res.Rows != 2 || !strings.HasSuffix(res.Key, ".csv") {
		t.Fatalf("%v %+v", err, res)
	}
}

func TestTruncatedBodyWithoutTrailingNewline(t *testing.T) {
	ch := newFakeCH(ndjsonRows(2)+`{"event_id":"e`, 200)
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	_, err := newRunner(ch, st, au).Run(context.Background(), Principal{TenantID: tenantA}, exportID, goodSpec())
	if !errors.Is(err, ErrTruncated) {
		t.Fatal(err)
	}
	assertCleaned(t, st, au, OutcomeFailed)
}

func TestClickHouseHTTPErrorBeforeStreaming(t *testing.T) {
	ch := newFakeCH("Code: 60. DB::Exception: Table doesn't exist", 404)
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	_, err := newRunner(ch, st, au).Run(context.Background(), Principal{TenantID: tenantA}, exportID, goodSpec())
	if err == nil || len(st.puts) != 0 {
		t.Fatal(err, st.puts)
	}
	if r := au.Records(); len(r) != 1 || r[0].Outcome != OutcomeFailed {
		t.Fatalf("%+v", r)
	}
}

func TestStoreErrorDeletesAndAudits(t *testing.T) {
	ch := newFakeCH(ndjsonRows(2), 200)
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	r := newRunner(ch, &failingStore{memStore: st}, au)
	if _, err := r.Run(context.Background(), Principal{TenantID: tenantA}, exportID, goodSpec()); !errors.Is(err, errBoom) {
		t.Fatal(err)
	}
	if len(st.deleted) != 1 {
		t.Fatal("expected delete")
	}
}

type failingStore struct{ *memStore }

func (f *failingStore) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	return 0, errBoom
}

func assertCleaned(t *testing.T, st *memStore, au *MemoryAudit, outcome string) {
	t.Helper()
	key := "exports/" + tenantA + "/" + exportID + ".ndjson"
	if _, ok := st.objects[key]; ok && !strings.HasSuffix(key, ".csv") {
		t.Fatal("partial object not deleted")
	}
	if len(st.deleted) != 1 {
		t.Fatalf("deleted %v", st.deleted)
	}
	for k := range st.objects {
		t.Fatalf("leftover object %s", k)
	}
	recs := au.Records()
	if len(recs) != 1 || recs[0].Outcome != outcome || recs[0].Error == "" {
		t.Fatalf("audit %+v", recs)
	}
}

func TestQuotaExceededAndConcurrency(t *testing.T) {
	ch := newFakeCH(ndjsonRows(1), 200)
	defer ch.Close()
	st, au := newMemStore(), &MemoryAudit{}
	r := newRunner(ch, st, au)
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	r.Quota = &MemoryQuota{MaxPerDay: 2, MaxConcurrent: 1, Now: func() time.Time { return now }}
	p := Principal{TenantID: tenantA}
	for i := 0; i < 2; i++ {
		if _, err := r.Run(context.Background(), p, exportID, goodSpec()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Run(context.Background(), p, exportID, goodSpec()); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatal(err)
	}
	if recs := au.Records(); recs[2].Outcome != OutcomeQuotaExceeded {
		t.Fatalf("%+v", recs)
	}
	// other tenant unaffected; next UTC day resets
	if _, err := r.Run(context.Background(), Principal{TenantID: tenantB}, exportID, goodSpec()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24 * time.Hour)
	if _, err := r.Run(context.Background(), p, exportID, goodSpec()); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryQuotaConcurrent(t *testing.T) {
	q := &MemoryQuota{MaxConcurrent: 1}
	rel, err := q.Acquire(tenantA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Acquire(tenantA); !errors.Is(err, ErrTooManyConcurrent) {
		t.Fatal(err)
	}
	rel()
	rel() // idempotent
	if _, err := q.Acquire(tenantA); err != nil {
		t.Fatal(err)
	}
}

func TestPresignTTLBounds(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &S3Store{Endpoint: "http://127.0.0.1:9000", Bucket: "b", Creds: awsCreds, Now: func() time.Time { return now }}
	key := ObjectKey(tenantA, exportID, "csv")
	u, exp, err := s.PresignGet(key, 0)
	if err != nil || !exp.Equal(now.Add(15*time.Minute)) {
		t.Fatal(err, exp)
	}
	pu, _ := url.Parse(u)
	if pu.Query().Get("X-Amz-Expires") != "900" || pu.Path != "/b/"+key {
		t.Fatal(u)
	}
	if _, _, err := s.PresignGet(key, time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, ttl := range []time.Duration{time.Hour + time.Second, -time.Minute, time.Millisecond, 24 * time.Hour} {
		if _, _, err := s.PresignGet(key, ttl); !errors.Is(err, ErrBadTTL) {
			t.Errorf("ttl %v: %v", ttl, err)
		}
	}
}

func TestJSONLinesAudit(t *testing.T) {
	var buf bytes.Buffer
	a := &JSONLinesAudit{W: &buf}
	_ = a.Emit(AuditRecord{TenantID: tenantA, ExportID: "x", Outcome: OutcomeSuccess, Rows: 3, Bytes: 9, SpecHash: "h"})
	_ = a.Emit(AuditRecord{TenantID: tenantA, ExportID: "y", Outcome: OutcomeFailed, Error: "e"})
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatal(buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil || rec["tenant"] != tenantA || rec["rows"] != float64(3) || rec["spec_hash"] != "h" {
		t.Fatal(rec, err)
	}
}
