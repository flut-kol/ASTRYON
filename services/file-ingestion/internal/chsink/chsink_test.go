package chsink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/parse"
	"github.com/astryon/file-ingestion/internal/worker"
)

const (
	tenant = "0a1b2c3d-1111-4222-8333-444455556666"
	source = "0a1b2c3d-1111-4222-8333-444455557777"
	fileID = "0a1b2c3d-1111-4222-8333-444455558888"
)

type files struct{ committed bool }

func (f files) Register(context.Context, string, string, objects.File) (string, error) {
	return "", nil
}
func (f files) Get(_ context.Context, t, id string) (objects.File, error) {
	return objects.File{ID: fileID, SourceID: source, Bucket: "b", Key: "k.ndjson", Committed: f.committed}, nil
}
func (files) MarkCommitted(context.Context, string, string, int64) error { return nil }
func (files) MarkRejected(context.Context, string, string) error         { return nil }

func item(off int64, raw string, err error) worker.Item {
	r := parse.Record{Number: 1, Offset: off, NextOffset: off + int64(len(raw)) + 1, Raw: []byte(raw), Err: err}
	k, _ := r.DedupKey("fp")
	return worker.Item{Record: r, DedupKey: k}
}

func newSink(t *testing.T, committed bool) (*Sink, *[]struct {
	Q map[string][]string
	B string
}) {
	var calls []struct {
		Q map[string][]string
		B string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		calls = append(calls, struct {
			Q map[string][]string
			B string
		}{r.URL.Query(), string(b)})
	}))
	t.Cleanup(srv.Close)
	s, err := New(Config{URL: srv.URL, Database: "astryon", Files: files{committed}, Mapping: Mapping{EventType: "file.row", EventTimeField: "ts", EntityIDField: "id"},
		Now: func() time.Time { return time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	return s, &calls
}

func TestAcceptInsertsEventsAndErrorsWithDeterministicTokens(t *testing.T) {
	s, calls := newSink(t, true)
	j := jobs.Job{ID: "j1", TenantID: tenant, FileObjectID: fileID}
	valid := []worker.Item{item(0, `{"id":"a","ts":"2026-10-01T00:00:00Z"}`, nil), item(40, `{"id":7,"ts":1760000000}`, nil), item(70, `{"x":1}`, nil)}
	bad := []worker.Item{item(80, `{oops`, io.ErrUnexpectedEOF)}
	if err := s.Accept(context.Background(), j, valid, bad); err != nil {
		t.Fatal(err)
	}
	if len(*calls) != 2 {
		t.Fatalf("calls = %d", len(*calls))
	}
	ev, er := (*calls)[0], (*calls)[1]
	if !strings.Contains(ev.Q["query"][0], "INSERT INTO events_normalized_distributed") || ev.Q["insert_deduplicate"][0] != "1" || ev.Q["insert_deduplication_token"][0] == "" {
		t.Fatalf("events query: %v", ev.Q)
	}
	lines := strings.Split(strings.TrimSpace(ev.B), "\n")
	if len(lines) != 3 {
		t.Fatalf("rows = %d", len(lines))
	}
	var r0, r1, r2 map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &r0)
	_ = json.Unmarshal([]byte(lines[1]), &r1)
	_ = json.Unmarshal([]byte(lines[2]), &r2)
	if r0["tenant_id"] != tenant || r0["source_id"] != source || r0["batch_id"] != fileID || r0["entity_id"] != "a" || r0["event_time"] != "2026-10-01 00:00:00.000" || r0["raw_ref"] != "s3://b/k.ndjson#0" {
		t.Fatalf("row0 = %v", r0)
	}
	if r1["entity_id"] != "7" || r1["event_time"] != "2025-10-09 08:53:20.000" {
		t.Fatalf("row1 = %v", r1)
	}
	if fl := r2["quality_flags"].([]any); len(fl) != 1 || fl[0] != "event_time_defaulted" || r2["event_time"] != "2026-10-21 12:00:00.000" {
		t.Fatalf("row2 = %v", r2)
	}
	if r0["event_id"] == r1["event_id"] || !strings.HasPrefix(r0["event_id"].(string)[14:], "5") {
		t.Fatalf("event ids: %v %v", r0["event_id"], r1["event_id"])
	}
	if !strings.Contains(er.Q["query"][0], "INSERT INTO processing_errors_local") || strings.Contains(er.B, "oops") || !strings.Contains(er.B, "ERR_JSON_MALFORMED") {
		t.Fatalf("errors: %v %s", er.Q, er.B)
	}
	// a replay of the same batch must produce identical tokens and event ids
	if err := s.Accept(context.Background(), j, valid, bad); err != nil {
		t.Fatal(err)
	}
	again := (*calls)[2]
	if again.Q["insert_deduplication_token"][0] != ev.Q["insert_deduplication_token"][0] || strings.Split(again.B, "\n")[0] != lines[0] {
		t.Fatal("replay is not deterministic")
	}
	if ev.Q["insert_deduplication_token"][0] == er.Q["insert_deduplication_token"][0] {
		t.Fatal("events and errors must not share a token")
	}
}

func TestAcceptRefusesUnverifiedFileAndFailsOnClickHouseError(t *testing.T) {
	s, calls := newSink(t, false)
	if err := s.Accept(context.Background(), jobs.Job{TenantID: tenant, FileObjectID: fileID}, []worker.Item{item(0, `{}`, nil)}, nil); err == nil || len(*calls) != 0 {
		t.Fatalf("err=%v calls=%d", err, len(*calls))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "boom", 500) }))
	defer srv.Close()
	s2, _ := New(Config{URL: srv.URL, Files: files{true}, Mapping: Mapping{EventType: "x"}})
	if err := s2.Accept(context.Background(), jobs.Job{TenantID: tenant, FileObjectID: fileID}, []worker.Item{item(0, `{}`, nil)}, nil); err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("err=%v", err)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	for name, c := range map[string]Config{
		"no url":             {Files: files{}, Mapping: Mapping{EventType: "x"}},
		"no event type":      {URL: "http://x", Files: files{}},
		"bad table name":     {URL: "http://x", Files: files{}, Mapping: Mapping{EventType: "x"}, EventsTable: "a;DROP"},
		"bad source mapping": {URL: "http://x", Files: files{}, Mapping: Mapping{EventType: "x"}, SourceMappings: map[string]Mapping{"not-a-source": {EventType: "x"}}},
	} {
		if _, err := New(c); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestSourceMappingOverridesDefault(t *testing.T) {
	s, calls := newSink(t, true)
	s.cfg.SourceMappings = map[string]Mapping{source: {EventType: "geo.point", EventTimeField: "occurred", EntityIDField: "device", ProcessingVersion: 7}}
	j := jobs.Job{ID: "j1", TenantID: tenant, FileObjectID: fileID}
	if err := s.Accept(context.Background(), j, []worker.Item{item(0, `{"device":"d1","occurred":"2026-10-01T00:00:00Z"}`, nil)}, nil); err != nil {
		t.Fatal(err)
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace((*calls)[0].B)), &row); err != nil {
		t.Fatal(err)
	}
	if row["event_type"] != "geo.point" || row["processing_version"] != float64(7) || row["entity_id"] != "d1" {
		t.Fatalf("row = %v", row)
	}
}
