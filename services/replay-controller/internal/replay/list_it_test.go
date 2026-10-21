package replay

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Live PostgreSQL (REPLAY_PG_URL, see pg_it_test.go): keyset paging, filters, tenant isolation, soft delete.
func TestPGLiveReaderListGet(t *testing.T) {
	pool, tA, tB, src, srcB := pgFixture(t)
	ctx := context.Background()
	a := &PGAllocator{DB: PoolDB{pool}}
	var ids []string
	for i := 0; i < 5; i++ {
		r, err := a.Reserve(ctx, tA, src, time.Now().Add(-time.Hour), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
		time.Sleep(5 * time.Millisecond)
	}
	if err := a.SetStatus(ctx, tA, ids[0], StatusFailed); err != nil {
		t.Fatal(err)
	}
	bReq, err := a.Reserve(ctx, tB, srcB, time.Now().Add(-time.Hour), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	rd := PGReader{Pool: pool}

	var seen []string
	var cur *Cursor
	for page := 0; page < 5; page++ {
		items, next, err := rd.List(ctx, tA, ListFilter{Limit: 2, After: cur})
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range items {
			seen = append(seen, i.ID)
		}
		if next == nil {
			break
		}
		cur = next
	}
	if len(seen) != 5 {
		t.Fatalf("paged %d rows, want 5: %v", len(seen), seen)
	}
	for i, id := range seen { // newest first
		if id != ids[4-i] {
			t.Fatalf("order %v, want reverse of %v", seen, ids)
		}
	}
	if items, _, _ := rd.List(ctx, tA, ListFilter{Status: "FAILED", Limit: 10}); len(items) != 1 || items[0].ID != ids[0] || items[0].CompletedAt == nil {
		t.Fatalf("status filter: %+v", items)
	}
	if items, _, _ := rd.List(ctx, tA, ListFilter{SourceID: srcB, Limit: 10}); len(items) != 0 {
		t.Fatalf("other tenant's source visible: %+v", items)
	}
	for _, i := range seen {
		if i == bReq.ID {
			t.Fatal("tenant B row listed for A")
		}
	}
	if _, err := rd.Get(ctx, tA, bReq.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get = %v, want ErrNotFound", err)
	}
	got, err := rd.Get(ctx, tA, ids[2])
	if err != nil || got.Status != StatusRequested || got.SourceID != src || got.ProcessingVersion != 3 || got.TimeFrom == nil {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := rd.Get(ctx, tA, "not-a-uuid"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("bad id = %v", err)
	}
	if _, _, err := rd.List(ctx, "bad", ListFilter{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad tenant = %v", err)
	}
}
