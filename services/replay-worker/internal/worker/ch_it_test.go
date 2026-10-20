package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// Live ClickHouse test, skipped unless REPLAYW_IT_URL is set (migration 0001 applied; REPLAYW_IT_USER,
// REPLAYW_IT_PASSWORD, REPLAYW_IT_DB). Seeds 25 events (+ duplicates) of a random source into
// events_normalized_local and runs Execute with a fake sink: keyset paging, dedup, version selection.
func TestCHLiveCopyForward(t *testing.T) {
	base := os.Getenv("REPLAYW_IT_URL")
	if base == "" {
		t.Skip("REPLAYW_IT_URL not set")
	}
	user, pass, db := os.Getenv("REPLAYW_IT_USER"), os.Getenv("REPLAYW_IT_PASSWORD"), os.Getenv("REPLAYW_IT_DB")
	src := fmt.Sprintf("dddddddd-%04x-4ddd-8ddd-%012x", time.Now().UnixNano()&0xffff, time.Now().UnixNano()&0xffffffffffff)
	exec := func(q, body string) {
		v := url.Values{"query": {q}, "database": {db}}
		req, _ := http.NewRequest("POST", base+"/?"+v.Encode(), strings.NewReader(body))
		req.Header.Set("X-ClickHouse-User", user)
		req.Header.Set("X-ClickHouse-Key", pass)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if b, _ := io.ReadAll(resp.Body); resp.StatusCode != 200 {
			t.Fatalf("%s: %s", q, b)
		}
	}
	var sb strings.Builder
	line := func(i, pv int) {
		fmt.Fprintf(&sb, `{"tenant_id":%q,"source_id":%q,"event_id":"00000000-0000-4000-8000-%012d","batch_id":"00000000-0000-0000-0000-000000000000","processing_version":%d,"pipeline_id":"00000000-0000-0000-0000-000000000000","pipeline_version":1,"event_type":"x","event_time":"2026-10-01 00:%02d:00.000","ingested_at":"2026-10-01 01:00:00.000","entity_id":"e","attributes":{},"payload":"{\"i\":%d}","raw_ref":"r","quality_flags":[]}`+"\n",
			tenantA, src, i, pv, i%60, i)
	}
	for i := 1; i <= 25; i++ {
		line(i, 1)
	}
	for i := 1; i <= 5; i++ { // duplicate physical rows of v1
		line(i, 1)
	}
	for i := 1; i <= 3; i++ { // already copied under v2 (resume)
		line(i, 2)
	}
	line(99, 3) // a LATER version must not be chosen as base for v2
	exec("INSERT INTO events_normalized_local FORMAT JSONEachRow", sb.String())

	c := &CH{URL: base, Database: db, User: user, Password: pass, Table: "events_normalized_local"}
	sink := &fakeSink{}
	w := &Worker{Reader: c, Sink: sink, PageSize: 7, Now: time.Now, Logf: func(string, ...any) {}}
	job := Job{JobID: "live1", TenantID: tenantA, SourceID: src, ProcessingVersion: 2,
		Ranges: []Range{{time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)}}}
	n, err := w.Execute(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if n != 22 || len(sink.recs) != 22 {
		t.Fatalf("published %d, want 22 (25 unique minus 3 already under v2)", n)
	}
	seen := map[string]bool{}
	for _, r := range sink.recs {
		var e struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(r.Value, &e); err != nil || seen[e.EventID] {
			t.Fatalf("duplicate or bad event %v %s", err, r.Value)
		}
		seen[e.EventID] = true
	}
}
