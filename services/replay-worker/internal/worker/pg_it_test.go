package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live PostgreSQL test, skipped unless REPLAYW_PG_URL is set. Use a non-superuser role without BYPASSRLS
// (otherwise the RLS assertions are skipped). db/postgres/migrations 0001-0007 must already be applied.

const itAdmin = `SELECT set_config('astryon.platform_admin', 'on', true)`

type itEnv struct {
	pool          *pgxpool.Pool
	tA, tB        string
	srcA, srcB    string
	ver           uint32
	st            *PGStatus
	rlsEnforcedOK bool
}

func itSetup(t *testing.T) *itEnv {
	t.Helper()
	url := os.Getenv("REPLAYW_PG_URL")
	if url == "" {
		t.Skip("REPLAYW_PG_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var super, bypass bool
	if err := pool.QueryRow(ctx, `SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user`).Scan(&super, &bypass); err != nil {
		t.Fatal(err)
	}
	e := &itEnv{pool: pool, ver: 5, rlsEnforcedOK: !super && !bypass}
	if !e.rlsEnforcedOK {
		t.Log("role bypasses RLS: cross-tenant assertions rely on the WHERE clause only")
	}
	e.admin(t, func(tx pgx.Tx) error {
		b := make([]byte, 4)
		_, _ = rand.Read(b)
		sfx := fmt.Sprintf("%x", b)
		for i, dst := range []*string{&e.tA, &e.tB} {
			if err := tx.QueryRow(ctx, `INSERT INTO tenants (slug, name) VALUES ($1, 'replayw it') RETURNING id::text`,
				fmt.Sprintf("replayw-it-%c-%s", 'a'+i, sfx)).Scan(dst); err != nil {
				return err
			}
		}
		if err := tx.QueryRow(ctx, `INSERT INTO sources (tenant_id, name, kind) VALUES ($1::uuid, 's', 'http_push') RETURNING id::text`, e.tA).Scan(&e.srcA); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO sources (tenant_id, name, kind) VALUES ($1::uuid, 's', 'http_push') RETURNING id::text`, e.tB).Scan(&e.srcB)
	})
	t.Cleanup(func() {
		e.admin(t, func(tx pgx.Tx) error {
			for _, q := range []string{`DELETE FROM replay_requests WHERE tenant_id IN ($1::uuid,$2::uuid)`,
				`DELETE FROM sources WHERE tenant_id IN ($1::uuid,$2::uuid)`, `DELETE FROM tenants WHERE id IN ($1::uuid,$2::uuid)`} {
				if _, err := tx.Exec(ctx, q, e.tA, e.tB); err != nil {
					return err
				}
			}
			return nil
		})
	})
	e.st = &PGStatus{DB: PoolDB{pool}}
	return e
}

func (e *itEnv) admin(t *testing.T, fn func(tx pgx.Tx) error) {
	t.Helper()
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, itAdmin); err != nil {
		t.Fatal(err)
	}
	if err := fn(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// insert creates a replay request with a fresh version.
func (e *itEnv) insert(t *testing.T, tenant, source, status string, deleted bool) uint32 {
	t.Helper()
	e.ver++
	v := e.ver
	e.admin(t, func(tx pgx.Tx) error {
		_, err := tx.Exec(context.Background(), `INSERT INTO replay_requests (tenant_id, source_id, target_processing_version, status, deleted_at)
VALUES ($1::uuid,$2::uuid,$3,$4::replay_status, CASE WHEN $5::bool THEN now() ELSE NULL END)`, tenant, source, int32(v), status, deleted)
		return err
	})
	return v
}

type itRow struct {
	status    string
	completed *time.Time
	updated   time.Time
}

func (a itRow) equal(b itRow) bool {
	return a.status == b.status && a.updated.Equal(b.updated) &&
		(a.completed == nil) == (b.completed == nil) && (a.completed == nil || a.completed.Equal(*b.completed))
}

func (e *itEnv) row(t *testing.T, tenant, source string, v uint32) itRow {
	t.Helper()
	var r itRow
	e.admin(t, func(tx pgx.Tx) error {
		return tx.QueryRow(context.Background(), `SELECT status::text, completed_at, updated_at FROM replay_requests
WHERE tenant_id=$1::uuid AND source_id=$2::uuid AND target_processing_version=$3`, tenant, source, int32(v)).Scan(&r.status, &r.completed, &r.updated)
	})
	return r
}

func TestPGLiveLifecycle(t *testing.T) {
	e := itSetup(t)
	ctx := context.Background()
	v := e.insert(t, e.tA, e.srcA, "REQUESTED", false)
	r0 := e.row(t, e.tA, e.srcA, v)
	time.Sleep(20 * time.Millisecond)
	if err := e.st.SetStatus(ctx, e.tA, e.srcA, v, StatusRunning); err != nil {
		t.Fatal(err)
	}
	r1 := e.row(t, e.tA, e.srcA, v)
	if r1.status != "RUNNING" || r1.completed != nil || !r1.updated.After(r0.updated) {
		t.Fatalf("after RUNNING: %+v (was %+v)", r1, r0)
	}
	time.Sleep(20 * time.Millisecond)
	if err := e.st.SetStatus(ctx, e.tA, e.srcA, v, StatusCompleted); err != nil {
		t.Fatal(err)
	}
	r2 := e.row(t, e.tA, e.srcA, v)
	if r2.status != "COMPLETED" || r2.completed == nil || !r2.updated.After(r1.updated) {
		t.Fatalf("after COMPLETED: %+v", r2)
	}
	// idempotent re-completion and illegal transitions are refused and leave the row untouched
	for _, s := range []string{StatusCompleted, StatusRunning, StatusFailed} {
		if err := e.st.SetStatus(ctx, e.tA, e.srcA, v, s); !errors.Is(err, ErrNotRunnable) {
			t.Fatalf("COMPLETED -> %s: %v", s, err)
		}
		if r := e.row(t, e.tA, e.srcA, v); !r.equal(r2) {
			t.Fatalf("row changed by refused %s: %+v vs %+v", s, r, r2)
		}
	}
	// APPROVED -> RUNNING -> FAILED -> RUNNING (retry) -> COMPLETED
	v2 := e.insert(t, e.tA, e.srcA, "APPROVED", false)
	for _, s := range []string{StatusRunning, StatusFailed, StatusRunning, StatusCompleted} {
		if err := e.st.SetStatus(ctx, e.tA, e.srcA, v2, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if r := e.row(t, e.tA, e.srcA, v2); r.status != s {
			t.Fatalf("want %s got %s", s, r.status)
		}
	}
	// FAILED path: only from RUNNING, sets completed_at
	v3 := e.insert(t, e.tA, e.srcA, "REQUESTED", false)
	if err := e.st.SetStatus(ctx, e.tA, e.srcA, v3, StatusFailed); !errors.Is(err, ErrNotRunnable) {
		t.Fatalf("REQUESTED -> FAILED must be refused: %v", err)
	}
	if err := e.st.SetStatus(ctx, e.tA, e.srcA, v3, StatusRunning); err != nil {
		t.Fatal(err)
	}
	if err := e.st.SetStatus(ctx, e.tA, e.srcA, v3, StatusFailed); err != nil {
		t.Fatal(err)
	}
	if r := e.row(t, e.tA, e.srcA, v3); r.status != "FAILED" || r.completed == nil {
		t.Fatalf("%+v", r)
	}
}

func TestPGLiveRefusals(t *testing.T) {
	e := itSetup(t)
	ctx := context.Background()
	vc := e.insert(t, e.tA, e.srcA, "CANCELLED", false)
	vd := e.insert(t, e.tA, e.srcA, "REQUESTED", true)
	vb := e.insert(t, e.tB, e.srcB, "REQUESTED", false)
	cBefore, dBefore, bBefore := e.row(t, e.tA, e.srcA, vc), e.row(t, e.tA, e.srcA, vd), e.row(t, e.tB, e.srcB, vb)

	for _, s := range []string{StatusRunning, StatusCompleted, StatusFailed} {
		if err := e.st.SetStatus(ctx, e.tA, e.srcA, vc, s); !errors.Is(err, ErrNotRunnable) {
			t.Fatalf("CANCELLED -> %s: %v", s, err)
		}
		if err := e.st.SetStatus(ctx, e.tA, e.srcA, vd, s); !errors.Is(err, ErrNotRunnable) {
			t.Fatalf("soft-deleted -> %s: %v", s, err)
		}
		if err := e.st.SetStatus(ctx, e.tA, e.srcA, 99999, s); !errors.Is(err, ErrNotRunnable) {
			t.Fatalf("missing -> %s: %v", s, err)
		}
		// tenant A job naming tenant B's source/version, and tenant B's identity with A's source
		if err := e.st.SetStatus(ctx, e.tA, e.srcB, vb, s); !errors.Is(err, ErrNotRunnable) {
			t.Fatalf("A job on B request -> %s: %v", s, err)
		}
		if err := e.st.SetStatus(ctx, e.tB, e.srcA, vb, s); !errors.Is(err, ErrNotRunnable) {
			t.Fatalf("B job on A source -> %s: %v", s, err)
		}
	}
	if !cBefore.equal(e.row(t, e.tA, e.srcA, vc)) || !dBefore.equal(e.row(t, e.tA, e.srcA, vd)) || !bBefore.equal(e.row(t, e.tB, e.srcB, vb)) {
		t.Fatal("a refused update changed a row")
	}
	// RLS itself (independent of the WHERE clause): with tenant A set, tenant B's rows are untouchable.
	if e.rlsEnforcedOK {
		tx, err := e.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, e.tA); err != nil {
			t.Fatal(err)
		}
		ct, err := tx.Exec(ctx, `UPDATE replay_requests SET status='RUNNING' WHERE tenant_id = $1::uuid`, e.tB)
		if err != nil || ct.RowsAffected() != 0 {
			t.Fatalf("RLS let tenant A touch B: %v %d", err, ct.RowsAffected())
		}
	}
	// the owner can still move its own request
	if err := e.st.SetStatus(ctx, e.tB, e.srcB, vb, StatusRunning); err != nil {
		t.Fatal(err)
	}
}

func TestPGLiveRaceSingleWinner(t *testing.T) {
	e := itSetup(t)
	ctx := context.Background()
	v := e.insert(t, e.tA, e.srcA, "REQUESTED", false)
	pool2, err := pgxpool.New(ctx, os.Getenv("REPLAYW_PG_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	workers := []*PGStatus{e.st, {DB: PoolDB{pool2}}}
	const n = 16
	var wins, refused atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			switch err := workers[i%2].SetStatus(ctx, e.tA, e.srcA, v, StatusRunning); {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, ErrNotRunnable):
				refused.Add(1)
			default:
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 || refused.Load() != n-1 {
		t.Fatalf("wins=%d refused=%d, want exactly one winner", wins.Load(), refused.Load())
	}

	// A crashed worker's RUNNING row is re-claimable only after the lease.
	lease := &PGStatus{DB: PoolDB{e.pool}, RunningLease: time.Hour}
	if err := lease.SetStatus(ctx, e.tA, e.srcA, v, StatusRunning); !errors.Is(err, ErrNotRunnable) {
		t.Fatalf("fresh RUNNING stolen: %v", err)
	}
	short := &PGStatus{DB: PoolDB{e.pool}, RunningLease: 300 * time.Millisecond}
	time.Sleep(450 * time.Millisecond) // lease expires (updated_at is set by the trigger on the RUNNING claim)
	lease = short
	var w2 atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if lease.SetStatus(ctx, e.tA, e.srcA, v, StatusRunning) == nil {
				w2.Add(1)
			}
		}()
	}
	wg.Wait()
	if w2.Load() != 1 {
		t.Fatalf("stale takeover winners=%d, want 1", w2.Load())
	}
}
