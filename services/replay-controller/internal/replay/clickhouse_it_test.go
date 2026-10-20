package replay

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Live test. Run with REPLAY_IT_URL=http://127.0.0.1:PORT (+ REPLAY_IT_USER/PASSWORD/DB) against a
// ClickHouse with migrations 0001 applied (single node; events_normalized_distributed exists).
func TestLiveClickHouse(t *testing.T) {
	base := os.Getenv("REPLAY_IT_URL")
	if base == "" {
		t.Skip("REPLAY_IT_URL not set")
	}
	user, pass, db := os.Getenv("REPLAY_IT_USER"), os.Getenv("REPLAY_IT_PASSWORD"), os.Getenv("REPLAY_IT_DB")
	exec := func(q string) {
		v := url.Values{"query": {q}}
		if db != "" {
			v.Set("database", db)
		}
		req, _ := http.NewRequest("POST", base+"/?"+v.Encode(), nil)
		req.Header.Set("X-ClickHouse-User", user)
		req.Header.Set("X-ClickHouse-Key", pass)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("%d %s", resp.StatusCode, b)
		}
	}
	// versions 1 and 3 for tenantA/srcX; 8 for tenantB/srcX (must be invisible to A).
	// v3: 5 rows incl. one exact duplicate row (same event_id) -> 4 de-duplicated, 1 outside range.
	ins := func(tenant, src string, ver int, eid int, ts string) string {
		return fmt.Sprintf("('%s','%s',toUUID('00000000-0000-0000-0000-%012d'),generateUUIDv4(),%d,generateUUIDv4(),1,'t',toDateTime64('%s',3,'UTC'),'','r',map(),'{}',[])",
			tenant, src, eid, ver, ts)
	}
	exec("TRUNCATE TABLE events_normalized_local")
	rows := []string{
		ins(tenantA, srcX, 1, 1, "2026-01-01 10:00:00"),
		ins(tenantA, srcX, 3, 1, "2026-01-01 10:00:00"),
		ins(tenantA, srcX, 3, 2, "2026-01-01 11:00:00"),
		ins(tenantA, srcX, 3, 2, "2026-01-01 11:00:00"), // duplicate
		ins(tenantA, srcX, 3, 3, "2026-01-02 11:00:00"),
		ins(tenantA, srcX, 3, 4, "2026-01-02 12:00:00"),
		ins(tenantA, srcX, 3, 5, "2026-03-01 12:00:00"), // outside range
		ins(tenantB, srcX, 8, 1, "2026-01-01 10:00:00"),
	}
	exec("INSERT INTO events_normalized_local (tenant_id,source_id,event_id,batch_id,processing_version,pipeline_id,pipeline_version,event_type,event_time,entity_id,raw_ref,attributes,payload,quality_flags) VALUES " +
		strings.Join(rows, ","))
	ch := &CH{URL: base, Database: db, User: user, Password: pass}
	ctx := context.Background()
	if v, err := ch.Peek(ctx, tenantA, srcX); err != nil || v != 3 {
		t.Fatalf("peek %d %v", v, err)
	}
	if v, err := ch.Peek(ctx, tenantA, "cccccccc-cccc-cccc-cccc-cccccccccccc"); err != nil || v != 0 {
		t.Fatalf("empty peek %d %v", v, err)
	}
	from, to := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	n, err := ch.Count(ctx, tenantA, srcX, 3, from, to, MaxEstimateRows+1)
	if err != nil || n != 4 {
		t.Fatalf("count %d %v (want 4 de-duplicated)", n, err)
	}
	if n, _ := ch.Count(ctx, tenantA, srcX, 3, from, to, 2); n != 2 {
		t.Fatalf("limit not applied: %d", n)
	}
	// concurrent allocations: all > 3 and distinct
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[uint32]bool{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := ch.Allocate(ctx, tenantA, srcX)
			mu.Lock()
			defer mu.Unlock()
			if err != nil || v <= 3 || seen[v] {
				t.Errorf("bad allocation %d %v", v, err)
			}
			seen[v] = true
		}()
	}
	wg.Wait()
	// full planner dry-run against live CH has no side effects
	pl := &Planner{Alloc: ch, Counter: ch, Publisher: &MemPublisher{}}
	before, _ := ch.Peek(ctx, tenantA, srcX)
	p, err := pl.Plan(ctx, tenantA, Request{SourceID: srcX, From: from, To: to, PipelineVersion: 2, DryRun: true})
	if err != nil || p.ProcessingVersion != before+1 {
		t.Fatalf("%+v %v", p, err)
	}
	if after, _ := ch.Peek(ctx, tenantA, srcX); after != before {
		t.Fatal("dry run reserved")
	}
	// readonly: a write through the CH query path must fail
	if _, err := ch.query(ctx, "INSERT INTO events_normalized_local SELECT * FROM events_normalized_local LIMIT 0", nil); err == nil {
		t.Fatal("write succeeded through the readonly path")
	}
}
