package agg

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	tT = "11111111-1111-1111-1111-111111111111"
	sS = "22222222-2222-2222-2222-222222222222"
	b1 = "aaaaaaaa-0000-0000-0000-000000000001" // clean
	b2 = "aaaaaaaa-0000-0000-0000-000000000002" // missing
	b3 = "aaaaaaaa-0000-0000-0000-000000000003" // extra
	b4 = "aaaaaaaa-0000-0000-0000-000000000004" // open, young, short
	b5 = "aaaaaaaa-0000-0000-0000-000000000005" // unregistered
)

type fakeReg struct {
	created, byID []RegBatch
	askedIDs      []string
}

func (f *fakeReg) BatchesCreated(context.Context, time.Time, time.Time, int) ([]RegBatch, error) {
	return f.created, nil
}
func (f *fakeReg) BatchesByID(_ context.Context, ids []string) ([]RegBatch, error) {
	f.askedIDs = ids
	return f.byID, nil
}

func chCount(b string, pv int, n int, firstMs int64) string {
	return `{"tenant_id":"` + tT + `","source_id":"` + sS + `","batch_id":"` + b + `","processing_version":` + itoa(pv) + `,"events":"` + itoa(n) + `","first_ingested_ms":"` + itoa(int(firstMs)) + `"}` + "\n"
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestReconcileRegistry(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	old := now.Add(-2 * time.Hour)
	var sql string
	var url string
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bb, _ := io.ReadAll(r.Body)
		sql, url = string(bb), r.URL.RawQuery
		old := now.Add(-2 * time.Hour).UnixMilli()
		_, _ = w.Write([]byte(chCount(b1, 1, 10, old) + chCount(b2, 1, 7, old) + chCount(b3, 1, 12, old) + chCount(b4, 1, 3, old) + chCount(b5, 1, 4, old)))
	}))
	defer srv.Close()
	c, _ := New(srv.URL, "", "", "")
	reg := &fakeReg{
		created: []RegBatch{
			{TenantID: tT, SourceID: sS, BatchID: b1, Status: "COMPLETED", Accepted: 10, UpdatedAt: old},
			{TenantID: tT, SourceID: sS, BatchID: b2, Status: "COMPLETED", Accepted: 10, UpdatedAt: old},
			{TenantID: tT, SourceID: sS, BatchID: b3, Status: "COMPLETED", Accepted: 10, UpdatedAt: old},
			{TenantID: tT, SourceID: sS, BatchID: b4, Status: "OPEN", Accepted: 9, UpdatedAt: old},
			{TenantID: tT, SourceID: sS, BatchID: "aaaaaaaa-0000-0000-0000-000000000006", Status: "COMPLETED", Accepted: 5, UpdatedAt: old}, // no CH rows
			{TenantID: tT, SourceID: sS, BatchID: "aaaaaaaa-0000-0000-0000-000000000007", Status: "COMPLETED", Accepted: 0, UpdatedAt: old}, // empty batch, fine
		},
	}
	w, _ := HourWindow(now.Add(-3*time.Hour), now)
	res, err := c.ReconcileRegistry(context.Background(), reg, w, now, 15*time.Minute, 100)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]string{}
	for _, m := range res.Mismatches {
		kinds[m.BatchID[len(m.BatchID)-1:]] = m.Kind + "/" + m.Class
	}
	want := map[string]string{"2": "missing/persistent", "3": "extra/persistent", "4": "missing/transient", "5": "unregistered/persistent", "6": "missing/persistent"}
	if len(kinds) != len(want) {
		t.Fatalf("%v", res.Mismatches)
	}
	for k, v := range want {
		if kinds[k] != v {
			t.Fatalf("batch %s: %s want %s (%v)", k, kinds[k], v, res.Mismatches)
		}
	}
	if res.Persistent() != 4 || len(reg.askedIDs) != 1 || reg.askedIDs[0] != b5 {
		t.Fatalf("persistent=%d asked=%v", res.Persistent(), reg.askedIDs)
	}
	for _, s := range []string{"LIMIT 1 BY tenant_id, source_id, event_id, processing_version", "ingested_at >=", "'" + b1 + "'"} {
		if !strings.Contains(sql, s) {
			t.Fatalf("missing %q in %s", s, sql)
		}
	}
	if !strings.Contains(url, "max_execution_time=") || !strings.Contains(url, "readonly=2") {
		t.Fatalf("bounds missing: %s", url)
	}
}

func TestRegistryTransientYoungAndLaggingRegistry(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(chCount(b1, 1, 3, now.Add(-time.Minute).UnixMilli()) + chCount(b5, 1, 4, now.Add(-time.Minute).UnixMilli())))
	}))
	defer srv.Close()
	c, _ := New(srv.URL, "", "", "")
	reg := &fakeReg{created: []RegBatch{{TenantID: tT, SourceID: sS, BatchID: b1, Status: "COMPLETED", Accepted: 9, UpdatedAt: now.Add(-time.Minute)}}}
	w, _ := HourWindow(now.Add(-time.Hour), now)
	res, err := c.ReconcileRegistry(context.Background(), reg, w, now, 15*time.Minute, 100)
	if err != nil || len(res.Mismatches) != 2 || res.Persistent() != 0 {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestRegistryCountsSQLRejectsBadID(t *testing.T) {
	c, _ := New("http://x", "", "", "")
	w, _ := HourWindow(time.Now().Add(-time.Hour), time.Now())
	if _, err := c.RegistryCountsSQL(w, []string{"x' OR 1=1 --"}, 10); err == nil {
		t.Fatal("injection accepted")
	}
}

func part(date string, n, sum uint64, ing int64) Partition {
	return Partition{TenantID: tT, SourceID: sS, EventDate: date, ProcessingVersion: 1, Events: n, Checksum: sum, MaxIngestedMs: ing}
}

func TestCompareSnapshots(t *testing.T) {
	d := func(s string) time.Time { x, _ := time.Parse("2006-01-02", s); return x }
	prev := Snapshot{Version: 1, From: d("2026-10-19"), To: d("2026-10-21"), Partitions: []Partition{
		part("2026-10-19", 5, 100, 1000), // unchanged
		part("2026-10-20", 5, 200, 2000), // tampered: same count/revision
		part("2026-10-19", 5, 300, 3000), // placeholder replaced below
	}}
	prev.Partitions[2].SourceID = "33333333-3333-3333-3333-333333333333" // legit change: count moves
	cur := Snapshot{Version: 1, From: d("2026-10-20"), To: d("2026-10-22"), Partitions: []Partition{
		part("2026-10-20", 5, 999, 2000),
		part("2026-10-21", 1, 1, 1),
	}}
	r := CompareSnapshots(prev, cur) // common range: only 2026-10-20
	if len(r.Changes) != 1 || !r.Changes[0].Silent || r.SilentCount() != 1 || r.Added != 0 || len(r.Missing) != 0 {
		t.Fatalf("%+v", r)
	}
	cur2 := Snapshot{Version: 1, From: d("2026-10-19"), To: d("2026-10-21"), Partitions: []Partition{
		part("2026-10-19", 5, 100, 1000), part("2026-10-20", 6, 999, 2500),
	}}
	r = CompareSnapshots(prev, cur2)
	if len(r.Changes) != 1 || r.Changes[0].Silent || r.SilentCount() != 0 || len(r.Missing) != 1 {
		t.Fatalf("explained change must not be silent: %+v", r)
	}
}

func TestChecksumSQLAndSnapshotRoundTrip(t *testing.T) {
	var q, rq string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		q, rq = string(b), r.URL.RawQuery
		_, _ = w.Write([]byte(`{"tenant_id":"` + tT + `","source_id":"` + sS + `","event_date":"2026-10-20","processing_version":1,"events":"5","checksum":"18446744073709551615","max_ingested_ms":"2000"}` + "\n"))
	}))
	defer srv.Close()
	c, _ := New(srv.URL, "", "", "")
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	w, _ := HourWindow(now.Add(-3*time.Hour), now)
	s, err := c.Checksums(context.Background(), w, 10, now)
	if err != nil || len(s.Partitions) != 1 || s.Partitions[0].Checksum != 18446744073709551615 {
		t.Fatalf("%+v %v", s, err)
	}
	if !s.From.Equal(time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)) || !s.To.Equal(time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("window not day aligned: %v %v", s.From, s.To)
	}
	for _, x := range []string{"cityHash64(", "LIMIT 1 BY", "LIMIT 10"} {
		if !strings.Contains(q, x) {
			t.Fatalf("%s missing: %s", x, q)
		}
	}
	if !strings.Contains(rq, "max_execution_time=") {
		t.Fatal(rq)
	}
	p := filepath.Join(t.TempDir(), "s.json")
	if err := WriteSnapshot(p, s); err != nil {
		t.Fatal(err)
	}
	back, err := ReadSnapshot(p)
	if err != nil || back.Partitions[0] != s.Partitions[0] {
		t.Fatalf("%+v %v", back, err)
	}
}

// fakeCH answers every reconcile query kind with canned rows.
func fakeCH(t *testing.T, checksum string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		q := string(b)
		switch {
		case strings.Contains(q, "cityHash64("):
			_, _ = w.Write([]byte(`{"tenant_id":"` + tT + `","source_id":"` + sS + `","event_date":"` + time.Now().UTC().Format("2006-01-02") + `","processing_version":1,"events":"5","checksum":"` + checksum + `","max_ingested_ms":"2000"}` + "\n"))
		case strings.Contains(q, "first_ingested_ms"):
			_, _ = w.Write([]byte(chCount(b1, 1, 10, 1000)))
		}
	}))
}

func TestRunExitCodes(t *testing.T) {
	srv := fakeCH(t, "100")
	defer srv.Close()
	c, _ := New(srv.URL, "", "", "")
	now := time.Now().UTC()
	w, _ := HourWindow(now.Add(-time.Hour), now)
	old := now.Add(-3 * time.Hour)
	var out bytes.Buffer
	o := RunOptions{Window: w, ChecksumWindow: w, Now: now, Grace: time.Minute, Out: &out, SnapOut: &out,
		Registry: &fakeReg{created: []RegBatch{{TenantID: tT, SourceID: sS, BatchID: b1, Status: "COMPLETED", Accepted: 10, UpdatedAt: old}}}}
	snap := filepath.Join(t.TempDir(), "snap.json")
	o.SnapshotPath = snap
	if code := c.Run(context.Background(), o); code != 0 {
		t.Fatalf("clean run exit=%d\n%s", code, out.String())
	}
	// registry mismatch -> 2
	o.SnapshotPath = ""
	o.Registry = &fakeReg{created: []RegBatch{{TenantID: tT, SourceID: sS, BatchID: b1, Status: "COMPLETED", Accepted: 12, UpdatedAt: old}}}
	if code := c.Run(context.Background(), o); code != 2 || !strings.Contains(out.String(), "kind=missing class=persistent") {
		t.Fatalf("exit=%d\n%s", code, out.String())
	}
	// no registry -> skipped notice, still ok
	o.Registry = nil
	out.Reset()
	if code := c.Run(context.Background(), o); code != 0 || !strings.Contains(out.String(), "registry-check skipped") {
		t.Fatalf("exit=%d\n%s", code, out.String())
	}
	// same partition, other checksum, same count/revision -> silent -> 2
	srv2 := fakeCH(t, "101")
	defer srv2.Close()
	c2, _ := New(srv2.URL, "", "", "")
	o.ComparePath = snap
	out.Reset()
	if code := c2.Run(context.Background(), o); code != 2 || !strings.Contains(out.String(), "SILENT") {
		t.Fatalf("exit=%d\n%s", code, out.String())
	}
	// unchanged compare -> 0
	out.Reset()
	if code := c.Run(context.Background(), o); code != 0 {
		t.Fatalf("exit=%d\n%s", code, out.String())
	}
}
