package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Kind is the jobs.kind handled by file workers.
const Kind = "process_file"

var (
	// ErrNoJob means nothing is claimable right now.
	ErrNoJob = errors.New("jobs: no claimable job")
	// ErrFenced means the caller no longer owns the job: lease expired, job re-claimed (new fence) or finished.
	ErrFenced = errors.New("jobs: lease lost (fenced)")
)

// Job is a claimed file job. Fence is jobs.attempt_count at claim time: every claim increments it, so a worker
// that lost its lease and later wakes up cannot write a checkpoint or a result (the compare-and-set fails).
type Job struct {
	ID, TenantID, FileObjectID string
	Fence                      int
	MaxAttempts                int
	Checkpoint                 json.RawMessage // last persisted parse checkpoint, nil on first run
}

// Row/Tx/DB mirror the small subset of pgx the store needs (fakeable in unit tests).
type Row interface{ Scan(dest ...any) error }
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}
type DB interface {
	Begin(ctx context.Context) (Tx, error)
}

// PoolDB adapts a pgxpool.Pool to DB.
type PoolDB struct{ Pool *pgxpool.Pool }

func (p PoolDB) Begin(ctx context.Context) (Tx, error) {
	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	return pgxTx{tx}, nil
}

type pgxTx struct{ tx pgx.Tx }

func (t pgxTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := t.tx.Exec(ctx, sql, args...)
	return tag.RowsAffected(), err
}
func (t pgxTx) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return t.tx.QueryRow(ctx, sql, args...)
}
func (t pgxTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t pgxTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

// RLS: cross-tenant scans (claim, promote, recover) switch platform_admin on with a constant; per-job writes
// run with the job's tenant. Neither value comes from a request.
const (
	sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	sqlAdminOn   = `SELECT set_config('astryon.platform_admin', 'on', true)`

	// Only QUEUED is claimable (Transition: RETRYING -> QUEUED first, see Promote).
	sqlPick = `SELECT id::text, tenant_id::text FROM jobs WHERE kind = '` + Kind + `' AND status = 'QUEUED' AND deleted_at IS NULL
		AND run_after <= now() AND (locked_until IS NULL OR locked_until < now())
		ORDER BY priority DESC, run_after, id LIMIT 1 FOR UPDATE SKIP LOCKED`
	sqlClaim = `UPDATE jobs SET status = 'RUNNING', attempt_count = attempt_count + 1, locked_by = $3,
		locked_until = now() + make_interval(secs => $4), started_at = COALESCE(started_at, now())
		WHERE id = $1::uuid AND tenant_id = $2::uuid
		RETURNING attempt_count, max_attempts, COALESCE(file_object_id::text,''), COALESCE(payload->'checkpoint','null')::text`
	sqlAttempt = `INSERT INTO job_attempts (tenant_id, job_id, attempt_no, status, worker_id) VALUES ($1::uuid, $2::uuid, $3, 'RUNNING', $4)`

	fenced = ` WHERE id = $1::uuid AND tenant_id = $2::uuid AND locked_by = $3 AND attempt_count = $4 AND status = 'RUNNING' AND locked_until > now()`

	sqlHeartbeat  = `UPDATE jobs SET locked_until = now() + make_interval(secs => $5)` + fenced
	sqlCheckpoint = `UPDATE jobs SET payload = jsonb_set(payload, '{checkpoint}', $5::jsonb), events_processed = events_processed + $6,
		events_failed = events_failed + $7, locked_until = now() + make_interval(secs => $8)` + fenced
	sqlSucceed = `UPDATE jobs SET status = 'SUCCEEDED', finished_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL` + fenced
	sqlRetry   = `UPDATE jobs SET status = 'RETRYING', run_after = now() + make_interval(secs => $5), last_error = $6, locked_by = NULL, locked_until = NULL` + fenced
	sqlFail    = `UPDATE jobs SET status = 'FAILED', finished_at = now(), last_error = $5, locked_by = NULL, locked_until = NULL` + fenced
	sqlEnd     = `UPDATE job_attempts SET status = $4, finished_at = now(), error_code = NULLIF($5,'') WHERE tenant_id = $1::uuid AND job_id = $2::uuid AND attempt_no = $3`

	// A worker that vanished leaves RUNNING with an expired lease. File jobs resume from the checkpoint, so they are
	// retried (not failed as worker_lost) until max_attempts is spent.
	sqlRecoverRetry = `UPDATE jobs SET status = 'RETRYING', run_after = now(), last_error = 'lease_expired', locked_by = NULL, locked_until = NULL
		WHERE kind = '` + Kind + `' AND status = 'RUNNING' AND locked_until < now() AND attempt_count < max_attempts`
	sqlRecoverFail = `UPDATE jobs SET status = 'FAILED', finished_at = now(), last_error = 'max_attempts_lease_expired', locked_by = NULL, locked_until = NULL
		WHERE kind = '` + Kind + `' AND status = 'RUNNING' AND locked_until < now() AND attempt_count >= max_attempts`
	sqlPromote = `UPDATE jobs SET status = 'QUEUED' WHERE kind = '` + Kind + `' AND status = 'RETRYING' AND run_after <= now()`
)

// Store is the durable, replica-safe job repository over the jobs table (migration 0004).
type Store struct {
	DB       DB
	WorkerID string        // unique per replica
	Lease    time.Duration // default 60s
	Backoff  func(attempt int) time.Duration
}

func (s *Store) lease() float64 {
	if s.Lease <= 0 {
		return 60
	}
	return s.Lease.Seconds()
}

func (s *Store) backoff(attempt int) time.Duration {
	if s.Backoff != nil {
		return s.Backoff(attempt)
	}
	d := time.Duration(1<<min(attempt, 10)) * 5 * time.Second
	return min(d, 15*time.Minute)
}

func (s *Store) inTx(ctx context.Context, admin bool, tenant string, f func(Tx) error) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if admin {
		_, err = tx.Exec(ctx, sqlAdminOn)
	} else {
		_, err = tx.Exec(ctx, sqlSetTenant, tenant)
	}
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Claim takes the next due QUEUED job (SKIP LOCKED, so replicas never block each other), marks it RUNNING with
// a new fence and opens a job_attempts row. ErrNoJob when nothing is due.
func (s *Store) Claim(ctx context.Context) (Job, error) {
	var j Job
	var cp string
	err := s.inTx(ctx, true, "", func(tx Tx) error {
		if err := tx.QueryRow(ctx, sqlPick).Scan(&j.ID, &j.TenantID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNoJob
			}
			return err
		}
		if err := tx.QueryRow(ctx, sqlClaim, j.ID, j.TenantID, s.WorkerID, s.lease()).Scan(&j.Fence, &j.MaxAttempts, &j.FileObjectID, &cp); err != nil {
			return err
		}
		if cp != "" && cp != "null" {
			j.Checkpoint = json.RawMessage(cp)
		}
		_, err := tx.Exec(ctx, sqlAttempt, j.TenantID, j.ID, j.Fence, s.WorkerID)
		return err
	})
	if err != nil {
		return Job{}, err
	}
	return j, nil
}

func (s *Store) fencedWrite(ctx context.Context, j Job, sql string, extra ...any) error {
	return s.inTx(ctx, false, j.TenantID, func(tx Tx) error {
		n, err := tx.Exec(ctx, sql, append([]any{j.ID, j.TenantID, s.WorkerID, j.Fence}, extra...)...)
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrFenced
		}
		return nil
	})
}

// Heartbeat extends the lease while this worker still owns the current fence.
func (s *Store) Heartbeat(ctx context.Context, j Job) error {
	return s.fencedWrite(ctx, j, sqlHeartbeat, s.lease())
}

// SaveCheckpoint persists the parse checkpoint and progress deltas atomically with the fence check. Call it only
// after the sink accepted the records (sinks dedupe on Record.DedupKey, so a crash in between replays safely).
func (s *Store) SaveCheckpoint(ctx context.Context, j Job, checkpoint json.RawMessage, processed, failed int64) error {
	if !json.Valid(checkpoint) {
		return errors.New("jobs: checkpoint is not valid JSON")
	}
	return s.fencedWrite(ctx, j, sqlCheckpoint, string(checkpoint), processed, failed, s.lease())
}

// Succeed finishes the job.
func (s *Store) Succeed(ctx context.Context, j Job) error {
	return s.finish(ctx, j, sqlSucceed, "SUCCEEDED", "")
}

// Fail ends the attempt with an error. Retryable errors schedule RETRYING with backoff while attempts remain;
// anything else, or an exhausted budget, is FAILED. Returns the resulting state.
func (s *Store) Fail(ctx context.Context, j Job, code string, retryable bool) (State, error) {
	if retryable && j.Fence < j.MaxAttempts {
		return Retrying, s.finish(ctx, j, sqlRetry, "RETRYING", code, s.backoff(j.Fence).Seconds(), code)
	}
	return Failed, s.finish(ctx, j, sqlFail, "FAILED", code, code)
}

func (s *Store) finish(ctx context.Context, j Job, sql, attemptStatus, code string, extra ...any) error {
	return s.inTx(ctx, false, j.TenantID, func(tx Tx) error {
		n, err := tx.Exec(ctx, sql, append([]any{j.ID, j.TenantID, s.WorkerID, j.Fence}, extra...)...)
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrFenced
		}
		_, err = tx.Exec(ctx, sqlEnd, j.TenantID, j.ID, j.Fence, attemptStatus, code)
		return err
	})
}

// Sweep promotes due RETRYING jobs to QUEUED and recovers jobs whose worker vanished. Safe to run on every replica.
func (s *Store) Sweep(ctx context.Context) (promoted, retried, failed int64, err error) {
	err = s.inTx(ctx, true, "", func(tx Tx) error {
		if retried, err = tx.Exec(ctx, sqlRecoverRetry); err != nil {
			return err
		}
		if failed, err = tx.Exec(ctx, sqlRecoverFail); err != nil {
			return err
		}
		promoted, err = tx.Exec(ctx, sqlPromote)
		return err
	})
	return
}
