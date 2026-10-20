package export

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// fakeDB scripts transaction behaviour by statement; it records every statement per transaction.
type fakeDB struct {
	txs     []*fakeTx
	handler func(t *fakeTx, sql string, args []any) (int64, Row, error)
}
type fakeTx struct {
	log       []string
	args      [][]any
	committed bool
	rolled    bool
	h         func(t *fakeTx, sql string, args []any) (int64, Row, error)
}
type scanFn func(dest ...any) error

func (f scanFn) Scan(d ...any) error { return f(d...) }

func (d *fakeDB) Begin(context.Context) (Tx, error) {
	t := &fakeTx{h: d.handler}
	d.txs = append(d.txs, t)
	return t, nil
}
func (t *fakeTx) rec(sql string, a []any) { t.log = append(t.log, sql); t.args = append(t.args, a) }
func (t *fakeTx) Exec(_ context.Context, sql string, a ...any) (int64, error) {
	t.rec(sql, a)
	if t.h != nil {
		n, _, err := t.h(t, sql, a)
		return n, err
	}
	return 0, nil
}
func (t *fakeTx) QueryRow(_ context.Context, sql string, a ...any) Row {
	t.rec(sql, a)
	if t.h != nil {
		if _, r, _ := t.h(t, sql, a); r != nil {
			return r
		}
	}
	return scanFn(func(...any) error { return pgx.ErrNoRows })
}
func (t *fakeTx) Commit(context.Context) error   { t.committed = true; return nil }
func (t *fakeTx) Rollback(context.Context) error { t.rolled = true; return nil }

func jobRow(id, tenant, status string) Row {
	return scanFn(func(d ...any) error {
		*(d[0].(*string)), *(d[1].(*string)), *(d[3].(*string)) = id, tenant, status
		*(d[4].(*string)) = `{"format":"csv"}`
		*(d[9].(*time.Time)) = time.Unix(1, 0)
		return nil
	})
}

const jobA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

func TestPGTenantSetFirstAndValidated(t *testing.T) {
	db := &fakeDB{handler: func(_ *fakeTx, sql string, _ []any) (int64, Row, error) {
		if sql == sqlGet {
			return 0, jobRow(jobA, tenantA, StatusQueued), nil
		}
		return 1, nil, nil
	}}
	s := &PGJobStore{DB: db, WorkerID: "w1"}
	j, err := s.Get(tenantA, jobA)
	if err != nil || j.ID != jobA || j.Spec.Format != "csv" {
		t.Fatalf("%v %+v", err, j)
	}
	tx := db.txs[0]
	if tx.log[0] != sqlSetTenant || tx.args[0][0] != tenantA || !tx.committed {
		t.Fatalf("tenant must be set first from the argument: %v", tx.log)
	}
	n := len(db.txs)
	if _, err := s.Get("not-a-uuid", jobA); err == nil || len(db.txs) != n {
		t.Fatalf("invalid tenant must be rejected before touching the DB: %v", err)
	}
	if _, err := s.Get(tenantA, "x"); !errors.Is(err, ErrJobNotFound) || len(db.txs) != n {
		t.Fatalf("bad id: %v", err)
	}
}

func TestPGGetNotFoundAndRollback(t *testing.T) {
	db := &fakeDB{}
	s := &PGJobStore{DB: db}
	if _, err := s.Get(tenantB, jobA); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("got %v", err)
	}
	if !db.txs[0].rolled || db.txs[0].committed {
		t.Fatal("must roll back on error")
	}
	if _, err := s.Update(tenantB, jobA, func(*Job) {}); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("update: %v", err)
	}
}

func TestPGUpdateWritesAndOwnsRunning(t *testing.T) {
	db := &fakeDB{handler: func(_ *fakeTx, sql string, _ []any) (int64, Row, error) {
		if sql == sqlGetForUpd {
			return 0, jobRow(jobA, tenantA, StatusQueued), nil
		}
		return 1, nil, nil
	}}
	s := &PGJobStore{DB: db, WorkerID: "w1", Lease: 30 * time.Second}
	j, err := s.Update(tenantA, jobA, func(j *Job) { j.Status = StatusRunning; j.TenantID = tenantB })
	if err != nil || j.TenantID != tenantA || j.Status != StatusRunning {
		t.Fatalf("%v %+v", err, j)
	}
	tx := db.txs[0]
	if len(tx.log) != 3 || tx.log[1] != sqlGetForUpd || tx.log[2] != sqlUpdate {
		t.Fatalf("%v", tx.log)
	}
	a := tx.args[2]
	if a[1] != tenantA || a[9] != "w1" || a[10] != 30.0 {
		t.Fatalf("tenant of the write must be the caller's, owner w1, lease 30: %v", a)
	}
}

func TestPGClaimSetsTenantFromRowAndDropsAdmin(t *testing.T) {
	db := &fakeDB{handler: func(_ *fakeTx, sql string, _ []any) (int64, Row, error) {
		switch sql {
		case sqlClaimPick:
			return 0, scanFn(func(d ...any) error { *(d[0].(*string)), *(d[1].(*string)) = jobA, tenantB; return nil }), nil
		case sqlClaimSet:
			return 0, jobRow(jobA, tenantB, StatusRunning), nil
		}
		return 1, nil, nil
	}}
	s := &PGJobStore{DB: db, WorkerID: "w1"}
	j, ok, err := s.Claim(context.Background())
	if err != nil || !ok || j.TenantID != tenantB {
		t.Fatalf("%v %v %+v", err, ok, j)
	}
	want := []string{sqlAdminOn, sqlClaimPick, sqlAdminOff, sqlSetTenant, sqlClaimSet}
	if strings.Join(db.txs[0].log, "|") != strings.Join(want, "|") {
		t.Fatalf("%v", db.txs[0].log)
	}
	if db.txs[0].args[3][0] != tenantB {
		t.Fatal("tenant must come from the claimed row")
	}
	empty := &PGJobStore{DB: &fakeDB{}}
	if _, ok, err := empty.Claim(context.Background()); ok || err != nil {
		t.Fatalf("empty: %v %v", ok, err)
	}
}

func quotaDB(day, active int64, ins *int) *fakeDB {
	return &fakeDB{handler: func(_ *fakeTx, sql string, _ []any) (int64, Row, error) {
		switch sql {
		case sqlQuotaCount:
			return 0, scanFn(func(d ...any) error { *(d[0].(*int64)), *(d[1].(*int64)) = day, active; return nil }), nil
		case sqlInsert:
			*ins++
		}
		return 1, nil, nil
	}}
}

func TestPGQuota(t *testing.T) {
	job := Job{ID: jobA, TenantID: tenantA, Spec: goodSpec()}
	cases := []struct {
		day, active int64
		want        error
	}{{0, 0, nil}, {5, 0, ErrQuotaExceeded}, {1, 2, ErrTooManyConcurrent}, {4, 1, nil}}
	for _, c := range cases {
		ins := 0
		db := quotaDB(c.day, c.active, &ins)
		q := &PGQuota{Store: &PGJobStore{DB: db, WorkerID: "w"}, MaxPerDay: 5, MaxConcurrent: 2}
		err := q.CreateReserved(job)
		if !errors.Is(err, c.want) || (c.want == nil) != (ins == 1) {
			t.Fatalf("%+v: err=%v inserts=%d", c, err, ins)
		}
		tx := db.txs[0]
		if tx.log[0] != sqlSetTenant || tx.log[1] != sqlQuotaLock || tx.log[2] != sqlQuotaCount {
			t.Fatalf("lock must precede the count: %v", tx.log)
		}
		if tx.committed != (c.want == nil) || tx.rolled == (c.want == nil) {
			t.Fatalf("commit/rollback wrong for %+v", c)
		}
	}
	// 0 = unlimited
	ins := 0
	q := &PGQuota{Store: &PGJobStore{DB: quotaDB(1000, 1000, &ins)}}
	if err := q.CreateReserved(job); err != nil || ins != 1 {
		t.Fatalf("%v", err)
	}
	rel, err := (&PGQuota{Store: &PGJobStore{DB: quotaDB(9, 0, &ins)}, MaxPerDay: 9}).Acquire(tenantA)
	if !errors.Is(err, ErrQuotaExceeded) || rel != nil {
		t.Fatalf("acquire: %v", err)
	}
}

func TestServerUsesReservingCreator(t *testing.T) {
	ins := 0
	db := quotaDB(5, 0, &ins)
	q := &PGQuota{Store: &PGJobStore{DB: db}, MaxPerDay: 5}
	e := newEnv(t, newMemStore(), q, 1)
	e.srv.Jobs = q.Store
	w := e.do("POST", "/v1/exports", "a", specBody)
	if w.Code != 429 || ins != 0 || errCodeOf(t, w) != "quota_exceeded" {
		t.Fatalf("%d %s inserts=%d", w.Code, w.Body, ins)
	}
}

func TestPGRecoverRequeuesBeforeFailingAndPassesMaxAttempts(t *testing.T) {
	db := &fakeDB{handler: func(_ *fakeTx, sql string, _ []any) (int64, Row, error) {
		if sql == sqlRequeue {
			return 2, nil, nil
		}
		return 1, nil, nil
	}}
	s := &PGJobStore{DB: db, WorkerID: "w1"}
	st, err := s.RecoverJobs(context.Background())
	if err != nil || st.Requeued != 2 || st.Failed != 1 {
		t.Fatalf("%v %+v", err, st)
	}
	tx := db.txs[0]
	if strings.Join(tx.log, "|") != strings.Join([]string{sqlAdminOn, sqlRequeue, sqlRecover}, "|") {
		t.Fatalf("requeue must run before fail: %v", tx.log)
	}
	if tx.args[1][0] != DefaultMaxAttempts || DefaultMaxAttempts != 3 {
		t.Fatalf("default max attempts is 3: %v", tx.args[1])
	}
	(&PGJobStore{DB: db, MaxAttempts: 5}).RecoverJobs(context.Background())
	if db.txs[1].args[1][0] != 5 {
		t.Fatal("MaxAttempts override")
	}
	// the rule is in the statements: only never-started claims are re-queued; started jobs are worker_lost
	for _, want := range []string{"started_at IS NULL", "attempts + 1 < $1", "attempts = attempts + 1", "status = 'QUEUED'"} {
		if !strings.Contains(sqlRequeue, want) {
			t.Fatalf("requeue statement lacks %q", want)
		}
	}
	if !strings.Contains(sqlRecover, "'max_attempts'") || !strings.Contains(sqlRecover, "'worker_lost'") {
		t.Fatal("recover statement must distinguish max_attempts and worker_lost")
	}
}

func TestPGStartFinishRequireOwnership(t *testing.T) {
	zero := &fakeDB{handler: func(*fakeTx, string, []any) (int64, Row, error) { return 0, nil, nil }}
	s := &PGJobStore{DB: zero, WorkerID: "w1"}
	j := Job{ID: jobA, TenantID: tenantA}
	if ok, err := s.Start(context.Background(), j); ok || err != nil {
		t.Fatalf("lost claim: %v %v", ok, err)
	}
	if ok, err := s.Finish(context.Background(), j, JobResult{Status: StatusSucceeded}); ok || err != nil {
		t.Fatalf("lost lease: %v %v", ok, err)
	}
	for _, q := range []string{sqlStart, sqlFinish, sqlRelease} {
		if !strings.Contains(q, "locked_by = $3") {
			t.Fatalf("must be guarded by owner: %s", q)
		}
	}
	if zero.txs[0].log[0] != sqlSetTenant || zero.txs[0].args[1][2] != "w1" {
		t.Fatalf("tenant first, owner arg: %v", zero.txs[0].log)
	}
}
