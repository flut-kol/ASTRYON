package api

// Real-ClickHouse integration test, gated like services/clickhouse-sink's
// realinfra_test.go. Run against the dev stack (make dev-up + db/apply.sh):
//
//	QUERYAPI_IT_CH_URL=http://127.0.0.1:18124 QUERYAPI_IT_CH_DB=astryon \
//	QUERYAPI_IT_CH_USER=astryon QUERYAPI_IT_CH_PASSWORD=astryon_dev_only \
//	go test ./internal/api/ -run TestRealClickHouse -v
//
// It uses fresh random tenant UUIDs and deletes only its own rows afterwards,
// so it is safe to re-run against a shared dev stack.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func itExec(t *testing.T, base, db, user, pass, q string) {
	t.Helper()
	req, _ := http.NewRequest("POST", base+"/?"+url.Values{"database": {db}}.Encode(), strings.NewReader(q))
	req.Header.Set("X-ClickHouse-User", user)
	req.Header.Set("X-ClickHouse-Key", pass)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	if r.StatusCode != 200 {
		t.Fatalf("exec: HTTP %d: %s", r.StatusCode, b)
	}
}

func TestRealClickHouse(t *testing.T) {
	base := os.Getenv("QUERYAPI_IT_CH_URL")
	if base == "" {
		t.Skip("QUERYAPI_IT_CH_URL not set")
	}
	db, user, pass := os.Getenv("QUERYAPI_IT_CH_DB"), os.Getenv("QUERYAPI_IT_CH_USER"), os.Getenv("QUERYAPI_IT_CH_PASSWORD")
	if db == "" {
		db = "astryon"
	}
	seed := fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff)
	tA := seed + "-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	tB := seed + "-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	src := seed + "-5555-4555-8555-555555555555"
	ev := func(tenant string, i, pv int) string {
		return fmt.Sprintf(`{"tenant_id":"%s","source_id":"%s","event_id":"%s","batch_id":"%s","processing_version":%d,`+
			`"pipeline_id":"%s","pipeline_version":1,"event_type":"t%d","event_time":"2026-09-30 12:00:%02d.000",`+
			`"entity_id":"ent%d","attributes":{"k":"v%d"},"payload":"{}","raw_ref":"r","quality_flags":[]}`,
			tenant, src, fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i), src, pv, src, i%2, i, i%3, i%2)
	}
	var rows []string
	for i := 1; i <= 25; i++ {
		rows = append(rows, ev(tA, i, 1))
	}
	rows = append(rows, ev(tA, 1, 2)) // replay of event 1 under a newer processing_version: must not double
	for i := 1; i <= 7; i++ {
		rows = append(rows, ev(tB, i, 1))
	}
	itExec(t, base, db, user, pass, "INSERT INTO events_normalized_local FORMAT JSONEachRow\n"+strings.Join(rows, "\n"))
	t.Cleanup(func() {
		itExec(t, base, db, user, pass, fmt.Sprintf(
			"ALTER TABLE events_normalized_local DELETE WHERE tenant_id IN ('%s','%s') SETTINGS mutations_sync=1", tA, tB))
	})

	a := CHAnalytics{R: NewCHClient(base, db, user, pass)}
	ctx := context.Background()
	if err := a.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	tr := TimeRange{From: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}

	// paginate through all of tenant A, 10 at a time
	req := EventsRequest{TimeRange: tr, Limit: 10}
	if err := req.Normalize(); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var after *Keyset
	for page := 0; page < 10; page++ {
		items, err := a.QueryEvents(ctx, tA, req, after)
		if err != nil {
			t.Fatal(err)
		}
		n := len(items)
		if n > req.Limit {
			n = req.Limit
		}
		for _, e := range items[:n] {
			if seen[e.EventID] {
				t.Fatalf("duplicate event across pages: %s", e.EventID)
			}
			seen[e.EventID] = true
		}
		if len(items) <= req.Limit {
			break
		}
		last := items[n-1]
		after = &Keyset{Time: last.EventTime, ID: last.EventID}
	}
	if len(seen) != 25 {
		t.Fatalf("tenant A saw %d distinct events, want 25 (replay version must be deduplicated)", len(seen))
	}

	// tenant isolation: B sees only its own 7, never A's ids
	items, err := a.QueryEvents(ctx, tB, EventsRequest{TimeRange: tr, Limit: 100}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 7 {
		t.Fatalf("tenant B saw %d, want 7", len(items))
	}
	if rows, _ := a.QueryEvents(ctx, "99999999-9999-4999-8999-999999999999", EventsRequest{TimeRange: tr, Limit: 100}, nil); len(rows) != 0 {
		t.Fatal("unknown tenant saw rows")
	}

	// filters (attribute + event_type) are parameter-bound and work
	f := EventsRequest{TimeRange: tr, Limit: 100, Filters: EventFilters{EventTypes: []string{"t1"}, Attributes: map[string]string{"k": "v1"}}}
	if items, err = a.QueryEvents(ctx, tA, f, nil); err != nil || len(items) == 0 {
		t.Fatalf("filtered query: %v %d", err, len(items))
	}
	for _, e := range items {
		if e.EventType != "t1" || e.Attributes["k"] != "v1" {
			t.Fatalf("filter leaked %+v", e)
		}
	}

	// aggregate: dedup-safe, grouped, tenant scoped
	agg := AggregateRequest{TimeRange: tr, GroupBy: []string{"event_type"}, Metrics: []string{"events", "entities", "last_event_time"}}
	if err := agg.Normalize(); err != nil {
		t.Fatal(err)
	}
	res, err := a.Aggregate(ctx, tA, agg)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, r := range res.Rows {
		total += r["events"].(int64)
	}
	if len(res.Rows) != 2 || total != 25 {
		t.Fatalf("aggregate rows=%d total=%d: %+v", len(res.Rows), total, res.Rows)
	}

	// summaries execute against the real aggregate tables (empty is fine)
	if _, err := a.MetricsSummary(ctx, tA, tr, ""); err != nil {
		t.Fatalf("metrics summary: %v", err)
	}
	if _, err := a.QualitySummary(ctx, tA, tr, src); err != nil {
		t.Fatalf("quality summary: %v", err)
	}
}
