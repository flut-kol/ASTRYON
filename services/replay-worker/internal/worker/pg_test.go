package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type fakeTx struct {
	execs     []string
	args      [][]any
	affected  int64
	committed bool
}

func (f *fakeTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	f.execs, f.args = append(f.execs, sql), append(f.args, args)
	if strings.Contains(sql, "set_config") {
		return 1, nil
	}
	return f.affected, nil
}
func (f *fakeTx) Commit(ctx context.Context) error   { f.committed = true; return nil }
func (f *fakeTx) Rollback(ctx context.Context) error { return nil }

type fakeDB struct{ tx *fakeTx }

func (f fakeDB) Begin(ctx context.Context) (Tx, error) { return f.tx, nil }

func TestPGStatusSetsTenantFirstAndFiltersByIt(t *testing.T) {
	tx := &fakeTx{affected: 1}
	p := &PGStatus{DB: fakeDB{tx}}
	if err := p.SetStatus(context.Background(), tenantA, srcID, 7, StatusRunning); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tx.execs[0], "set_config('astryon.tenant_id'") || tx.args[0][0] != tenantA {
		t.Fatalf("first statement must set the RLS tenant: %v %v", tx.execs[0], tx.args[0])
	}
	if !strings.Contains(tx.execs[1], "tenant_id = $1::uuid") || tx.args[1][0] != tenantA || !tx.committed {
		t.Fatalf("update: %s %v committed=%v", tx.execs[1], tx.args[1], tx.committed)
	}
	if err := p.SetStatus(context.Background(), tenantA, srcID, 7, StatusCompleted); err != nil || !strings.Contains(tx.execs[3], "status = 'RUNNING'") {
		t.Fatalf("terminal only from RUNNING: %v %s", err, tx.execs[3])
	}
}

func TestPGStatusNotRunnable(t *testing.T) {
	tx := &fakeTx{affected: 0}
	p := &PGStatus{DB: fakeDB{tx}}
	if err := p.SetStatus(context.Background(), tenantA, srcID, 7, StatusRunning); !errors.Is(err, ErrNotRunnable) || tx.committed {
		t.Fatalf("%v committed=%v", err, tx.committed)
	}
	if err := p.SetStatus(context.Background(), "bad", srcID, 7, StatusRunning); err == nil {
		t.Fatal("invalid tenant accepted")
	}
	if err := p.SetStatus(context.Background(), tenantA, srcID, 7, "CANCELLED"); err == nil {
		t.Fatal("unsupported status accepted")
	}
}

func TestPGStatusRunningLeaseArg(t *testing.T) {
	tx := &fakeTx{affected: 1}
	p := &PGStatus{DB: fakeDB{tx}, RunningLease: 90 * time.Second}
	if err := p.SetStatus(context.Background(), tenantA, srcID, 7, StatusRunning); err != nil {
		t.Fatal(err)
	}
	if tx.args[1][3] != 90.0 || !strings.Contains(tx.execs[1], "status IN ('REQUESTED','APPROVED','FAILED')") || strings.Contains(tx.execs[1], "'APPROVED','RUNNING'") {
		t.Fatalf("lease/claim rule: %v %s", tx.args[1], tx.execs[1])
	}
	tx2 := &fakeTx{affected: 1}
	_ = (&PGStatus{DB: fakeDB{tx2}}).SetStatus(context.Background(), tenantA, srcID, 7, StatusRunning)
	if tx2.args[1][3] != DefaultRunningLease.Seconds() {
		t.Fatalf("default lease: %v", tx2.args[1])
	}
}
