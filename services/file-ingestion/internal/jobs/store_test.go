package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/jackc/pgx/v5"
)

type fakeTx struct {
	log       []string
	args      [][]any
	rows      map[string]int64 // sql -> rows affected
	pick      bool
	committed bool
}
type fakeDB struct{ tx *fakeTx }
type scan func(...any) error

func (f scan) Scan(d ...any) error { return f(d...) }

func (d *fakeDB) Begin(context.Context) (Tx, error) { return d.tx, nil }
func (t *fakeTx) Exec(_ context.Context, sql string, a ...any) (int64, error) {
	t.log, t.args = append(t.log, sql), append(t.args, a)
	if n, ok := t.rows[sql]; ok {
		return n, nil
	}
	return 1, nil
}
func (t *fakeTx) QueryRow(_ context.Context, sql string, a ...any) Row {
	t.log, t.args = append(t.log, sql), append(t.args, a)
	switch sql {
	case sqlPick:
		if !t.pick {
			return scan(func(...any) error { return pgx.ErrNoRows })
		}
		return scan(func(d ...any) error { *d[0].(*string), *d[1].(*string) = "job-1", "tenant-1"; return nil })
	case sqlLabelsGet:
		return scan(func(d ...any) error { *d[0].(*string) = `{"contains_geo":{"count":2,"fields":["lat"]}}`; return nil })
	case sqlClaim:
		return scan(func(d ...any) error {
			*d[0].(*int), *d[1].(*int), *d[2].(*string), *d[3].(*string) = 2, 3, "file-1", `{"NextOffset":42}`
			*d[4].(*string), *d[5].(*string) = "src-1", "geo"
			return nil
		})
	}
	return scan(func(...any) error { return pgx.ErrNoRows })
}
func (t *fakeTx) Commit(context.Context) error   { t.committed = true; return nil }
func (t *fakeTx) Rollback(context.Context) error { return nil }

func newStore(tx *fakeTx) *Store { return &Store{DB: &fakeDB{tx}, WorkerID: "w1"} }

func TestClaimFencesAndResumesFromCheckpoint(t *testing.T) {
	tx := &fakeTx{pick: true}
	j, err := newStore(tx).Claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if j.ID != "job-1" || j.Fence != 2 || j.MaxAttempts != 3 || string(j.Checkpoint) != `{"NextOffset":42}` {
		t.Fatalf("job = %+v", j)
	}
	if tx.log[0] != sqlAdminOn || tx.log[3] != sqlAttempt || !tx.committed {
		t.Fatalf("statements: %v", tx.log)
	}
	if tx.args[3][2] != 2 { // job_attempts.attempt_no = fence
		t.Fatalf("attempt args: %v", tx.args[3])
	}
}

func TestClaimNothingDue(t *testing.T) {
	tx := &fakeTx{}
	if _, err := newStore(tx).Claim(context.Background()); !errors.Is(err, ErrNoJob) || tx.committed {
		t.Fatalf("err=%v committed=%v", err, tx.committed)
	}
}

func TestStaleWorkerIsFenced(t *testing.T) {
	j := Job{ID: "job-1", TenantID: "tenant-1", Fence: 1, MaxAttempts: 3}
	for name, call := range map[string]func(*Store) error{
		"heartbeat":  func(s *Store) error { return s.Heartbeat(context.Background(), j) },
		"checkpoint": func(s *Store) error { return s.SaveCheckpoint(context.Background(), j, []byte(`{}`), 1, 0, nil) },
		"succeed":    func(s *Store) error { return s.Succeed(context.Background(), j) },
		"fail":       func(s *Store) error { _, err := s.Fail(context.Background(), j, "x", true); return err },
	} {
		tx := &fakeTx{rows: map[string]int64{sqlHeartbeat: 0, sqlCheckpoint: 0, sqlSucceed: 0, sqlRetry: 0}}
		if err := call(newStore(tx)); !errors.Is(err, ErrFenced) || tx.committed {
			t.Errorf("%s: err=%v committed=%v", name, err, tx.committed)
		}
	}
}

func TestFailRetriesUntilBudgetSpent(t *testing.T) {
	tx := &fakeTx{}
	st, err := newStore(tx).Fail(context.Background(), Job{ID: "j", TenantID: "t", Fence: 1, MaxAttempts: 3}, "io", true)
	if err != nil || st != Retrying || tx.log[1] != sqlRetry {
		t.Fatalf("retryable: %v %v %v", st, err, tx.log)
	}
	for name, j := range map[string]struct {
		fence     int
		retryable bool
	}{"exhausted": {3, true}, "permanent": {1, false}} {
		tx := &fakeTx{}
		st, err := newStore(tx).Fail(context.Background(), Job{ID: "j", TenantID: "t", Fence: j.fence, MaxAttempts: 3}, "bad", j.retryable)
		if err != nil || st != Failed || tx.log[1] != sqlFail {
			t.Errorf("%s: %v %v %v", name, st, err, tx.log)
		}
	}
}

func TestSaveCheckpointRejectsInvalidJSON(t *testing.T) {
	tx := &fakeTx{}
	if err := newStore(tx).SaveCheckpoint(context.Background(), Job{}, []byte(`{`), 0, 0, nil); err == nil || len(tx.log) != 0 {
		t.Fatalf("err=%v log=%v", err, tx.log)
	}
}

func TestSweepOrder(t *testing.T) {
	tx := &fakeTx{}
	if _, _, _, err := newStore(tx).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	// recover first so a requeued job is promoted in the same pass
	want := []string{sqlAdminOn, sqlRecoverRetry, sqlRecoverFail, sqlPromote}
	for i, w := range want {
		if tx.log[i] != w {
			t.Fatalf("statement %d = %q", i, tx.log[i])
		}
	}
}

func TestSaveCheckpointMergesLabelsInOneTx(t *testing.T) {
	tx := &fakeTx{}
	d := classify.Labels{classify.Geo: {Count: 3, Fields: []string{"lon"}}}
	if err := newStore(tx).SaveCheckpoint(context.Background(), Job{ID: "j", TenantID: "t"}, []byte(`{}`), 1, 0, d); err != nil {
		t.Fatal(err)
	}
	if !tx.committed || tx.log[1] != sqlLabelsGet || tx.log[2] != sqlCheckpoint {
		t.Fatalf("statements: %v", tx.log)
	}
	merged := tx.args[2][8].(string)
	var l classify.Labels
	if err := json.Unmarshal([]byte(merged), &l); err != nil || l[classify.Geo].Count != 5 || len(l[classify.Geo].Fields) != 2 {
		t.Fatalf("merged = %s (%v)", merged, err)
	}
}

func TestQuarantineRaisesSensitivity(t *testing.T) {
	tx := &fakeTx{}
	err := newStore(tx).Quarantine(context.Background(), Job{ID: "j", TenantID: "t", FileObjectID: "f"}, "CREDENTIALS_DETECTED", classify.Labels{classify.Credentials: {Count: 1, Fields: []string{"pw"}}})
	if err != nil || tx.log[1] != sqlQuarantine || tx.log[2] != sqlRaise || tx.log[3] != sqlEnd || !tx.committed {
		t.Fatalf("err=%v statements: %v", err, tx.log)
	}
}
