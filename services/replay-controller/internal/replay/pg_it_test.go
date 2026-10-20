package replay

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live PostgreSQL test, skipped unless REPLAY_PG_URL is set. The role in the URL should be a
// non-superuser without BYPASSRLS (otherwise the RLS assertions are skipped). Migrations
// db/postgres/migrations/*.up.sql must already be applied.

const sqlAsAdmin = `SELECT set_config('astryon.platform_admin', 'on', true)`

func pgFixture(t *testing.T) (*pgxpool.Pool, string, string, string, string) {
	t.Helper()
	url := os.Getenv("REPLAY_PG_URL")
	if url == "" {
		t.Skip("REPLAY_PG_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var tA, tB, src, srcB string
	admin := func(fn func(tx pgx.Tx) error) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, sqlAsAdmin); err != nil {
			t.Fatal(err)
		}
		if err := fn(tx); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	admin(func(tx pgx.Tx) error {
		sfx := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
		for i, dst := range []*string{&tA, &tB} {
			if err := tx.QueryRow(ctx, `INSERT INTO tenants (slug, name) VALUES ($1, 'replay it') RETURNING id::text`,
				fmt.Sprintf("replay-it-%c-%s", 'a'+i, sfx)).Scan(dst); err != nil {
				return err
			}
		}
		if err := tx.QueryRow(ctx, `INSERT INTO sources (tenant_id, name, kind) VALUES ($1::uuid, 's', 'http_push') RETURNING id::text`, tA).Scan(&src); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO sources (tenant_id, name, kind) VALUES ($1::uuid, 's', 'http_push') RETURNING id::text`, tB).Scan(&srcB)
	})
	t.Cleanup(func() {
		admin(func(tx pgx.Tx) error {
			for _, q := range []string{`DELETE FROM replay_requests WHERE tenant_id IN ($1::uuid,$2::uuid)`,
				`DELETE FROM sources WHERE tenant_id IN ($1::uuid,$2::uuid)`, `DELETE FROM tenants WHERE id IN ($1::uuid,$2::uuid)`} {
				if _, err := tx.Exec(ctx, q, tA, tB); err != nil {
					return err
				}
			}
			return nil
		})
	})
	return pool, tA, tB, src, srcB
}

func TestPGLiveTwoReplicasDistinctIncreasing(t *testing.T) {
	pool, tA, tB, src, srcB := pgFixture(t)
	ctx := context.Background()
	// two allocator instances over two separate pools = two replicas
	pool2, err := pgxpool.New(ctx, os.Getenv("REPLAY_PG_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	reps := []*PGAllocator{{DB: PoolDB{pool}, CH: fixedPeek(5)}, {DB: PoolDB{pool2}, CH: fixedPeek(5)}}

	const n = 50
	var wg sync.WaitGroup
	got := make([]uint32, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := reps[i%2].Allocate(ctx, tA, src)
			if err != nil {
				t.Error(err)
			}
			got[i] = v
		}()
	}
	wg.Wait()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	for i, v := range got {
		if v != uint32(6+i) { // distinct, contiguous, strictly above the ClickHouse max (5)
			t.Fatalf("versions %v", got)
		}
	}
	if v, _ := reps[1].Peek(ctx, tA, src); v != 55 {
		t.Fatalf("peek %d", v)
	}

	// Tenant B: independent numbering, and A's rows are invisible.
	if v, err := reps[0].Allocate(ctx, tB, srcB); err != nil || v != 6 {
		t.Fatalf("tenant B got %d,%v want 6", v, err)
	}
	var super, bypass bool
	if err := pool.QueryRow(ctx, `SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user`).Scan(&super, &bypass); err != nil {
		t.Fatal(err)
	}
	if super || bypass {
		t.Log("role bypasses RLS: skipping visibility assertions")
		return
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, tB); err != nil {
		t.Fatal(err)
	}
	var cnt, foreign int
	if err := tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE tenant_id <> $1::uuid) FROM replay_requests`, tB).Scan(&cnt, &foreign); err != nil {
		t.Fatal(err)
	}
	if cnt != 1 || foreign != 0 {
		t.Fatalf("tenant B sees %d rows (%d foreign)", cnt, foreign)
	}
	// Without a tenant setting nothing is visible at all.
	tx2, _ := pool.Begin(ctx)
	defer tx2.Rollback(ctx)
	if err := tx2.QueryRow(ctx, `SELECT count(*) FROM replay_requests`).Scan(&cnt); err != nil || cnt != 0 {
		t.Fatalf("no-tenant session sees %d rows, %v", cnt, err)
	}
	// A cross-tenant insert is refused by the RLS WITH CHECK.
	if _, err := tx.Exec(ctx, `INSERT INTO replay_requests (tenant_id, source_id, target_processing_version) VALUES ($1::uuid,$2::uuid,99)`, tA, src); err == nil {
		t.Fatal("cross-tenant insert allowed")
	}
}

func TestPGLiveFailedPublishBurnsVersion(t *testing.T) {
	pool, tA, _, src, _ := pgFixture(t)
	ctx := context.Background()
	alloc := &PGAllocator{DB: PoolDB{pool}}
	pub := &MemPublisher{Err: errors.New("kafka down")}
	pl := &Planner{Alloc: alloc, Counter: FixedCounter{}, Publisher: pub}
	req := Request{SourceID: src, From: time.Now().Add(-time.Hour), To: time.Now(), PipelineVersion: 1}
	if _, err := pl.Plan(ctx, tA, req); err == nil {
		t.Fatal("expected publish error")
	}
	var st string
	var v int32
	var tf *time.Time
	q := func() {
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		tx.Exec(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, tA)
		if err := tx.QueryRow(ctx, `SELECT status::text, target_processing_version, time_from FROM replay_requests WHERE source_id=$1::uuid ORDER BY target_processing_version DESC LIMIT 1`, src).Scan(&st, &v, &tf); err != nil {
			t.Fatal(err)
		}
	}
	q()
	if st != StatusFailed || v != 1 || tf == nil {
		t.Fatalf("status=%s v=%d time_from=%v", st, v, tf)
	}
	pub.Err = nil
	plan, err := pl.Plan(ctx, tA, req)
	if err != nil || plan.ProcessingVersion != 2 {
		t.Fatalf("%+v %v", plan, err)
	}
	q()
	if st != StatusRequested || v != 2 {
		t.Fatalf("status=%s v=%d", st, v)
	}
}
