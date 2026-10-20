package export

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live PostgreSQL tests, skipped unless EXPORT_PG_URL is set. The role in the URL must be a non-superuser
// without BYPASSRLS (otherwise the test fails: RLS would not be enforced). Migrations 0001-0009 must
// already be applied and the role needs DML on export_jobs and tenants.

type pgFix struct {
	url    string
	tA, tB string
	pools  []*pgxpool.Pool
}

func (f *pgFix) store(t *testing.T, worker string, lease time.Duration) *PGJobStore {
	t.Helper()
	p, err := pgxpool.New(context.Background(), f.url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	f.pools = append(f.pools, p)
	return &PGJobStore{DB: PoolDB{Pool: p}, WorkerID: worker, Lease: lease}
}

func (f *pgFix) admin(t *testing.T, fn func(tx pgx.Tx) error) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.pools[0].Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, sqlAdminOn); err != nil {
		t.Fatal(err)
	}
	if err := fn(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func newPGFix(t *testing.T) (*pgFix, *PGJobStore) {
	t.Helper()
	url := os.Getenv("EXPORT_PG_URL")
	if url == "" {
		t.Skip("EXPORT_PG_URL not set")
	}
	f := &pgFix{url: url}
	s := f.store(t, "w-main", time.Minute)
	ctx := context.Background()
	var super, bypass bool
	if err := f.pools[0].QueryRow(ctx, `SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user`).Scan(&super, &bypass); err != nil {
		t.Fatal(err)
	}
	if super || bypass {
		t.Fatal("EXPORT_PG_URL role must be non-superuser without BYPASSRLS, else RLS is not tested")
	}
	sfx := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
	f.admin(t, func(tx pgx.Tx) error {
		for i, dst := range []*string{&f.tA, &f.tB} {
			if err := tx.QueryRow(ctx, `INSERT INTO tenants (slug, name) VALUES ($1, 'export it') RETURNING id::text`,
				fmt.Sprintf("export-it-%c-%s", 'a'+i, sfx)).Scan(dst); err != nil {
				return err
			}
		}
		return nil
	})
	t.Cleanup(func() {
		f.admin(t, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `DELETE FROM export_jobs WHERE tenant_id IN ($1::uuid,$2::uuid)`, f.tA, f.tB); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `DELETE FROM tenants WHERE id IN ($1::uuid,$2::uuid)`, f.tA, f.tB)
			return err
		})
	})
	return f, s
}

func liveJob(tenant string) Job {
	id := NewExportID()
	return Job{ID: id, TenantID: tenant, Subject: "u1", Spec: goodSpec(), ObjectKey: ObjectKey(tenant, id, "csv")}
}

func TestPGLiveRoundTripAndTenantScope(t *testing.T) {
	f, s := newPGFix(t)
	j := liveJob(f.tA)
	if err := s.Create(j); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(j); err == nil {
		t.Fatal("duplicate id must fail")
	}
	got, err := s.Get(f.tA, j.ID)
	if err != nil || got.Status != StatusQueued || got.Subject != "u1" || got.Spec.Format != j.Spec.Format || got.ObjectKey != j.ObjectKey {
		t.Fatalf("%v %+v", err, got)
	}
	// another tenant: not found for Get and Update, and no change leaks
	if _, err := s.Get(f.tB, j.ID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("cross-tenant get: %v", err)
	}
	if _, err := s.Update(f.tB, j.ID, func(c *Job) { c.Status = StatusFailed }); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("cross-tenant update: %v", err)
	}
	now := time.Now().UTC()
	if _, err := s.Update(f.tA, j.ID, func(c *Job) { c.Status, c.StartedAt = StatusRunning, now }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Update(f.tA, j.ID, func(c *Job) {
		c.Status, c.Rows, c.Bytes, c.FinishedAt = StatusSucceeded, 7, 99, now
	}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(f.tA, j.ID)
	if got.Status != StatusSucceeded || got.Rows != 7 || got.Bytes != 99 || got.StartedAt.IsZero() || got.FinishedAt.IsZero() {
		t.Fatalf("%+v", got)
	}
	var owner *string
	f.admin(t, func(tx pgx.Tx) error {
		return tx.QueryRow(context.Background(), `SELECT locked_by FROM export_jobs WHERE id=$1::uuid`, j.ID).Scan(&owner)
	})
	if owner != nil {
		t.Fatalf("terminal job must release ownership, got %q", *owner)
	}
	if _, err := s.Get("not-a-uuid", j.ID); err == nil {
		t.Fatal("invalid tenant must error")
	}
}

func TestPGLiveRLSBlocksSessionWithoutTenant(t *testing.T) {
	f, s := newPGFix(t)
	if err := s.Create(liveJob(f.tA)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var n int
	if err := f.pools[0].QueryRow(ctx, `SELECT count(*) FROM export_jobs`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("no tenant must see 0 rows, got %d err=%v", n, err)
	}
	if _, err := f.pools[0].Exec(ctx, `UPDATE export_jobs SET status='FAILED'`); err != nil {
		t.Fatal(err)
	}
	other := liveJob(f.tB)
	_, err := f.pools[0].Exec(ctx, `INSERT INTO export_jobs (id, tenant_id, status, spec, spec_hash) VALUES ($1::uuid,$2::uuid,'QUEUED','{}','h')`, other.ID, other.TenantID)
	if err == nil {
		t.Fatal("insert without tenant must violate RLS")
	}
	// tenant B session cannot see or write tenant A's row
	tx, _ := f.pools[0].Begin(ctx)
	defer tx.Rollback(ctx)
	_, _ = tx.Exec(ctx, sqlSetTenant, f.tB)
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM export_jobs`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("tenant B sees %d rows of tenant A", n)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO export_jobs (id, tenant_id, status, spec, spec_hash) VALUES ($1::uuid,$2::uuid,'QUEUED','{}','h')`, other.ID, f.tA); err == nil {
		t.Fatal("writing a row for another tenant must violate RLS")
	}
}

func TestPGLiveClaimNeverDuplicates(t *testing.T) {
	f, s1 := newPGFix(t)
	s2 := f.store(t, "w-2", time.Minute)
	s1.WorkerID = "w-1"
	const total = 50
	want := map[string]bool{}
	for i := 0; i < total; i++ {
		tenant := f.tA
		if i%2 == 1 {
			tenant = f.tB
		}
		j := liveJob(tenant)
		want[j.ID] = true
		if err := s1.Enqueue(j); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	got := map[string][]string{}
	var wg sync.WaitGroup
	for _, s := range []*PGJobStore{s1, s2} {
		for k := 0; k < 3; k++ { // 3 claim loops per instance
			wg.Add(1)
			go func(s *PGJobStore) {
				defer wg.Done()
				for {
					j, ok, err := s.Claim(context.Background())
					if err != nil {
						t.Error(err)
						return
					}
					if !ok {
						return
					}
					if j.Status != StatusQueued { // claimed, not started
						t.Errorf("status %s", j.Status)
					}
					mu.Lock()
					got[j.ID] = append(got[j.ID], s.WorkerID)
					mu.Unlock()
				}
			}(s)
		}
	}
	wg.Wait()
	if len(got) != total {
		t.Fatalf("claimed %d of %d", len(got), total)
	}
	by := map[string]int{}
	for id, w := range got {
		if !want[id] || len(w) != 1 {
			t.Fatalf("job %s claimed by %v", id, w)
		}
		by[w[0]]++
	}
	if len(by) < 1 {
		t.Fatal("nobody claimed")
	}
	t.Logf("claims per worker: %v", by)
	// a claimed job is tenant-visible as QUEUED until Start
	for id := range got {
		j, err := s1.Get(f.tA, id)
		if err == nil && j.Status != StatusQueued {
			t.Fatalf("status %s", j.Status)
		}
	}
}

func TestPGLiveQuotaCannotBeExceeded(t *testing.T) {
	for _, mode := range []string{"day", "concurrent"} {
		t.Run(mode, func(t *testing.T) {
			f, s1 := newPGFix(t)
			s2 := f.store(t, "w-2", time.Minute)
			q1 := &PGQuota{Store: s1}
			q2 := &PGQuota{Store: s2}
			const limit = 5
			for _, q := range []*PGQuota{q1, q2} {
				if mode == "day" {
					q.MaxPerDay = limit
				} else {
					q.MaxConcurrent = limit
				}
			}
			var ok, day, conc, other int
			var mu sync.Mutex
			var wg sync.WaitGroup
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					q := q1
					if i%2 == 1 {
						q = q2
					}
					err := q.CreateReserved(liveJob(f.tA))
					mu.Lock()
					defer mu.Unlock()
					switch {
					case err == nil:
						ok++
					case errors.Is(err, ErrQuotaExceeded):
						day++
					case errors.Is(err, ErrTooManyConcurrent):
						conc++
					default:
						other++
						t.Error(err)
					}
				}(i)
			}
			wg.Wait()
			if ok != limit || ok+day+conc != 20 || other != 0 {
				t.Fatalf("ok=%d day=%d conc=%d", ok, day, conc)
			}
			// the other tenant is unaffected
			if err := q1.CreateReserved(liveJob(f.tB)); err != nil {
				t.Fatal(err)
			}
			if mode == "concurrent" { // finishing one frees a slot
				var id string
				f.admin(t, func(tx pgx.Tx) error {
					return tx.QueryRow(context.Background(), `SELECT id::text FROM export_jobs WHERE tenant_id=$1::uuid LIMIT 1`, f.tA).Scan(&id)
				})
				if _, err := s1.Update(f.tA, id, func(c *Job) { c.Status, c.FinishedAt = StatusFailed, time.Now() }); err != nil {
					t.Fatal(err)
				}
				if err := q2.CreateReserved(liveJob(f.tA)); err != nil {
					t.Fatalf("slot should be free: %v", err)
				}
				if err := q2.CreateReserved(liveJob(f.tA)); !errors.Is(err, ErrTooManyConcurrent) {
					t.Fatalf("got %v", err)
				}
			}
		})
	}
}

func TestPGLiveRecoveryAndHeartbeat(t *testing.T) {
	f, _ := newPGFix(t)
	dead := f.store(t, "w-dead", time.Second)
	alive := f.store(t, "w-alive", time.Second)
	jd, ja := liveJob(f.tA), liveJob(f.tA)
	for _, c := range []struct {
		s *PGJobStore
		j Job
	}{{dead, jd}, {alive, ja}} {
		if err := c.s.Create(c.j); err != nil {
			t.Fatal(err)
		}
		if _, err := c.s.Update(c.j.TenantID, c.j.ID, func(x *Job) { x.Status, x.StartedAt = StatusRunning, time.Now() }); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	time.Sleep(1300 * time.Millisecond)
	if n, err := alive.Heartbeat(ctx); err != nil || n != 1 {
		t.Fatalf("heartbeat %d %v", n, err)
	}
	// anyone can run recovery; only the expired lease is failed
	if n, err := alive.Recover(ctx); err != nil || n != 1 {
		t.Fatalf("recover %d %v", n, err)
	}
	g, _ := alive.Get(f.tA, jd.ID)
	if g.Status != StatusFailed || g.ErrorCode != "worker_lost" {
		t.Fatalf("%+v", g)
	}
	g, _ = alive.Get(f.tA, ja.ID)
	if g.Status != StatusRunning {
		t.Fatalf("heartbeated job must survive: %+v", g)
	}
	// FailUnfinished touches only the caller's own jobs
	if n := dead.FailUnfinished("shutdown", time.Now()); n != 0 {
		t.Fatalf("dead owns nothing now: %d", n)
	}
	if n := alive.FailUnfinished("shutdown", time.Now()); n != 1 {
		t.Fatalf("alive owns 1: %d", n)
	}
}
