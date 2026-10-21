package chsink

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/worker"
)

// Live: CHSINK_IT_URL (HTTP, migrations 0001+0002 applied on a single node, database CHSINK_IT_DB default "default").
func TestLiveReplayedBatchIsDeduplicated(t *testing.T) {
	base := os.Getenv("CHSINK_IT_URL")
	if base == "" {
		t.Skip("CHSINK_IT_URL not set")
	}
	db := os.Getenv("CHSINK_IT_DB")
	if db == "" {
		db = "default"
	}
	query := func(q string) string {
		resp, err := http.Post(base+"/?database="+db, "text/plain", strings.NewReader(q))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %s", q, b)
		}
		return strings.TrimSpace(string(b))
	}
	s, err := New(Config{URL: base, Database: db, EventsTable: "events_normalized_local", Files: files{true},
		Mapping: Mapping{EventType: "file.row", EventTimeField: "ts", EntityIDField: "id"}})
	if err != nil {
		t.Fatal(err)
	}
	j := jobs.Job{ID: "0a1b2c3d-1111-4222-8333-444455559999", TenantID: tenant, FileObjectID: fileID}
	valid := []worker.Item{item(0, `{"id":"a","ts":"2026-10-01T00:00:00Z"}`, nil), item(40, `{"id":"b","ts":1760000000000}`, nil), item(80, `{"id":"c"}`, nil)}
	bad := []worker.Item{item(100, `{oops`, io.ErrUnexpectedEOF)}
	for i := 0; i < 3; i++ { // the worker replays the same batch after a crash between sink and checkpoint
		if err := s.Accept(context.Background(), j, valid, bad); err != nil {
			t.Fatal(err)
		}
	}
	where := " WHERE tenant_id = '" + tenant + "' AND batch_id = '" + fileID + "'"
	if n := query("SELECT count() FROM events_normalized_local" + where); n != "3" {
		t.Fatalf("events after 3 identical inserts = %s, want 3", n)
	}
	if n := query("SELECT uniqExact(event_id) FROM events_normalized_local" + where); n != "3" {
		t.Fatalf("unique events = %s", n)
	}
	if n := query("SELECT count() FROM processing_errors_local" + where); n != "1" {
		t.Fatalf("errors after 3 identical inserts = %s, want 1", n)
	}
	if got := query("SELECT entity_id, toString(event_time), quality_flags FROM events_normalized_local" + where + " AND entity_id = 'b'"); !strings.HasPrefix(got, "b\t2025-10-09 08:53:20.000") {
		t.Fatalf("row b = %q", got)
	}
}
