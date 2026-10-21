package jobs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live PostgreSQL test, skipped unless FILEJOBS_PG_URL is set. Role: non-superuser without BYPASSRLS with DML on
// jobs, job_attempts and tenants; migrations 0001-0010 applied.
func TestLiveLeaseFencingAndResume(t *testing.T) {
	url := os.Getenv("FILEJOBS_PG_URL")
	if url == "" {
		t.Skip("FILEJOBS_PG_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	admin := func(fn func(tx pgx.Tx) error) {
		t.Helper()
		tx, err := pool.Begin(ctx)
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
	var tenant, job string
	admin(func(tx pgx.Tx) error {
		slug := fmt.Sprintf("filejobs-it-%d", time.Now().UnixNano()%1_000_000_000)
		if err := tx.QueryRow(ctx, `INSERT INTO tenants (slug, name) VALUES ($1,'it') RETURNING id::text`, slug).Scan(&tenant); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO jobs (tenant_id, kind, status, max_attempts) VALUES ($1::uuid,'process_file','QUEUED',3) RETURNING id::text`, tenant).Scan(&job)
	})
	t.Cleanup(func() {
		admin(func(tx pgx.Tx) error {
			tx.Exec(ctx, `DELETE FROM job_attempts WHERE tenant_id=$1::uuid`, tenant)
			tx.Exec(ctx, `DELETE FROM jobs WHERE tenant_id=$1::uuid`, tenant)
			_, err := tx.Exec(ctx, `DELETE FROM tenants WHERE id=$1::uuid`, tenant)
			return err
		})
	})
	w1 := &Store{DB: PoolDB{pool}, WorkerID: "w1", Lease: time.Minute}
	w2 := &Store{DB: PoolDB{pool}, WorkerID: "w2", Lease: time.Minute}

	j1, err := w1.Claim(ctx)
	if err != nil || j1.ID != job || j1.Fence != 1 {
		t.Fatalf("w1 claim: %v %+v", err, j1)
	}
	if _, err := w2.Claim(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("running job must not be claimable: %v", err)
	}
	if err := w1.Heartbeat(ctx, j1); err != nil {
		t.Fatal(err)
	}
	if err := w1.SaveCheckpoint(ctx, j1, []byte(`{"NextOffset":42}`), 10, 1, nil); err != nil {
		t.Fatal(err)
	}
	// w1 vanishes: its lease expires, the sweep retries and promotes the job
	admin(func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id=$1::uuid`, job)
		return err
	})
	if err := w1.Heartbeat(ctx, j1); !errors.Is(err, ErrFenced) {
		t.Fatalf("expired lease must fence the owner: %v", err)
	}
	if _, retried, _, err := w2.Sweep(ctx); err != nil || retried < 1 {
		t.Fatalf("sweep: %d %v", retried, err)
	}
	j2, err := w2.Claim(ctx)
	if err != nil || j2.Fence != 2 || string(j2.Checkpoint) != `{"NextOffset": 42}` {
		t.Fatalf("w2 claim: %v %+v (checkpoint %s)", err, j2, j2.Checkpoint)
	}
	if err := w1.SaveCheckpoint(ctx, j1, []byte(`{"NextOffset":99}`), 1, 0, nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale worker wrote a checkpoint: %v", err)
	}
	if err := w1.Succeed(ctx, j1); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale worker finished the job: %v", err)
	}
	if st, err := w2.Fail(ctx, j2, "io", true); err != nil || st != Retrying {
		t.Fatalf("retryable fail: %v %v", st, err)
	}
	if _, _, _, err := w2.Sweep(ctx); err != nil { // backoff not elapsed: stays RETRYING
		t.Fatal(err)
	}
	if _, err := w1.Claim(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("job in backoff must not be claimable: %v", err)
	}
	admin(func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE jobs SET run_after = now() WHERE id=$1::uuid`, job)
		return err
	})
	_, _, _, _ = w1.Sweep(ctx)
	j3, err := w1.Claim(ctx)
	if err != nil || j3.Fence != 3 {
		t.Fatalf("third claim: %v %+v", err, j3)
	}
	if err := w1.Succeed(ctx, j3); err != nil {
		t.Fatal(err)
	}
	var status string
	var processed, failed int64
	admin(func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT status::text, events_processed, events_failed FROM jobs WHERE id=$1::uuid`, job).Scan(&status, &processed, &failed)
	})
	if status != "SUCCEEDED" || processed != 10 || failed != 1 {
		t.Fatalf("final row: %s %d %d", status, processed, failed)
	}
}
