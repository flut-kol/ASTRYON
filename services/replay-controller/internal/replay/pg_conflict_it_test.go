package replay

import (
	"context"
	"testing"
	"time"
)

// Live (REPLAY_PG_URL, migrations up to 0014): a writer that bypasses the allocator and duplicates a reserved
// version is rejected by replay_requests_version_uq, and the error is recognised as a conflict.
func TestPGLiveUniqueVersionIndex(t *testing.T) {
	pool, tA, _, src, _ := pgFixture(t)
	ctx := context.Background()
	a := &PGAllocator{DB: PoolDB{pool}}
	r, err := a.Reserve(ctx, tA, src, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, sqlSetTenant, tA); err != nil {
		t.Fatal(err)
	}
	var id string
	err = tx.QueryRow(ctx, sqlIns, tA, src, int32(r.ProcessingVersion), nil, nil).Scan(&id)
	if err == nil || !isVersionConflict(err) {
		t.Fatalf("duplicate version accepted or misclassified: %v", err)
	}
}
