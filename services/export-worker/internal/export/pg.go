package export

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Row is the subset of pgx.Row the stores need.
type Row interface{ Scan(dest ...any) error }

// Tx is the subset of a PostgreSQL transaction the stores need (fakeable in unit tests).
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// DB starts transactions.
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

// Statements. RLS: tenant comes from set_config('astryon.tenant_id') per transaction; the platform-admin
// switch is only used by the cross-tenant maintenance paths (claim scan, heartbeat, recovery, shutdown) and is
// a constant in this file, never derived from a request.
const (
	sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	sqlAdminOn   = `SELECT set_config('astryon.platform_admin', 'on', true)`
	sqlAdminOff  = `SELECT set_config('astryon.platform_admin', 'off', true)`
	sqlQuotaLock = `SELECT pg_advisory_xact_lock(hashtextextended('astryon.export_quota:' || $1::text, 0))`
	jobCols      = `id::text, tenant_id::text, COALESCE(requested_by,''), status, spec::text, COALESCE(object_key,''), rows, bytes, COALESCE(error_code,''), created_at, started_at, finished_at, attempts`
	sqlInsert    = `INSERT INTO export_jobs (id, tenant_id, requested_by, status, spec, spec_hash, object_key, locked_by, lease_expires_at) VALUES ($1::uuid, $2::uuid, NULLIF($3,''), $4, $5::jsonb, $6, NULLIF($7,''), NULLIF($8,''), CASE WHEN $8 = '' THEN NULL ELSE now() + make_interval(secs => $9) END)`
	sqlGet       = `SELECT ` + jobCols + ` FROM export_jobs WHERE id = $1::uuid AND tenant_id = $2::uuid AND deleted_at IS NULL`
	sqlGetForUpd = sqlGet + ` FOR UPDATE`
	sqlUpdate    = `UPDATE export_jobs SET status = $3, rows = $4, bytes = $5, object_key = NULLIF($6,''), error_code = NULLIF($7,''), started_at = $8, finished_at = $9, locked_by = CASE WHEN $3 IN ('SUCCEEDED','FAILED','CANCELLED') THEN NULL WHEN $3 = 'RUNNING' THEN $10 ELSE locked_by END, lease_expires_at = CASE WHEN $3 IN ('SUCCEEDED','FAILED','CANCELLED') THEN NULL WHEN $3 = 'RUNNING' THEN now() + make_interval(secs => $11) ELSE lease_expires_at END WHERE id = $1::uuid AND tenant_id = $2::uuid`
	sqlFailOwn   = `UPDATE export_jobs SET status = 'FAILED', error_code = $1, finished_at = $2, locked_by = NULL, lease_expires_at = NULL WHERE locked_by = $3 AND status IN ('QUEUED','RUNNING')`
	// Recovery rule (see README): a claimed-but-not-started job (QUEUED, started_at NULL, owner lease expired) returns to
	// QUEUED with attempts+1 while attempts+1 < $1 (max attempts); an exhausted one is FAILED 'max_attempts'; a job that
	// had started (RUNNING) may have written output and is FAILED 'worker_lost', never re-run. Requeue runs first.
	sqlRequeue        = `UPDATE export_jobs SET locked_by = NULL, lease_expires_at = NULL, attempts = attempts + 1 WHERE status = 'QUEUED' AND started_at IS NULL AND locked_by IS NOT NULL AND lease_expires_at < now() AND attempts + 1 < $1`
	sqlRecover        = `UPDATE export_jobs SET status = 'FAILED', error_code = CASE WHEN status = 'QUEUED' AND started_at IS NULL THEN 'max_attempts' ELSE 'worker_lost' END, finished_at = now(), locked_by = NULL, lease_expires_at = NULL WHERE status IN ('QUEUED','RUNNING') AND locked_by IS NOT NULL AND lease_expires_at < now()`
	sqlStart          = `UPDATE export_jobs SET status = 'RUNNING', started_at = now(), lease_expires_at = now() + make_interval(secs => $4) WHERE id = $1::uuid AND tenant_id = $2::uuid AND locked_by = $3 AND status = 'QUEUED'`
	sqlFinish         = `UPDATE export_jobs SET status = $4, rows = $5, bytes = $6, object_key = NULLIF($7,''), error_code = NULLIF($8,''), finished_at = now(), locked_by = NULL, lease_expires_at = NULL WHERE id = $1::uuid AND tenant_id = $2::uuid AND locked_by = $3 AND status = 'RUNNING'`
	sqlRelease        = `UPDATE export_jobs SET locked_by = NULL, lease_expires_at = NULL WHERE id = $1::uuid AND tenant_id = $2::uuid AND locked_by = $3 AND status = 'QUEUED' AND started_at IS NULL`
	sqlHeartbeat      = `UPDATE export_jobs SET lease_expires_at = now() + make_interval(secs => $2) WHERE locked_by = $1 AND status IN ('QUEUED','RUNNING') AND NOT (id::text = ANY($3::text[]))`
	sqlHeartbeatOwned = `WITH u AS (UPDATE export_jobs SET lease_expires_at = now() + make_interval(secs => $2) WHERE locked_by = $1 AND status IN ('QUEUED','RUNNING') AND NOT (id::text = ANY($3::text[])) RETURNING id::text) SELECT COALESCE(array_agg(id), ARRAY[]::text[]) FROM u`
	sqlClaimPick      = `SELECT id::text, tenant_id::text FROM export_jobs WHERE status = 'QUEUED' AND deleted_at IS NULL AND locked_by IS NULL ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED`
	sqlClaimSet       = `UPDATE export_jobs SET locked_by = $3, lease_expires_at = now() + make_interval(secs => $4) WHERE id = $1::uuid AND tenant_id = $2::uuid RETURNING ` + jobCols
	sqlQuotaCount     = `SELECT count(*) FILTER (WHERE created_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'), count(*) FILTER (WHERE status IN ('QUEUED','RUNNING')) FROM export_jobs WHERE tenant_id = $1::uuid`
)

// PGJobStore is the durable, replica-safe JobStore backed by export_jobs (migration 0009).
// WorkerID must be unique per replica; Lease is how long a non-terminal job stays owned without a heartbeat.
type PGJobStore struct {
	DB       DB
	WorkerID string
	Lease    time.Duration // default 60s
	Timeout  time.Duration // per-operation timeout, default 15s
	// MaxAttempts bounds how often a claimed-but-not-started job is re-queued by Recover (default 3 attempts).
	MaxAttempts int

	abMu      sync.Mutex
	abandoned map[string]struct{}
}

// Abandon stops heartbeating a job whose terminal write could not be delivered (see Dispatcher.finish), so its
// lease expires and recovery fails it as worker_lost instead of it staying RUNNING forever.
func (s *PGJobStore) Abandon(j Job) {
	s.abMu.Lock()
	defer s.abMu.Unlock()
	if s.abandoned == nil {
		s.abandoned = map[string]struct{}{}
	}
	s.abandoned[j.ID] = struct{}{}
}

func (s *PGJobStore) abandonedIDs() []string {
	s.abMu.Lock()
	defer s.abMu.Unlock()
	ids := make([]string, 0, len(s.abandoned))
	for id := range s.abandoned {
		ids = append(ids, id)
	}
	return ids
}

// DefaultMaxAttempts is the number of times a job may be claimed before a lost claim fails it (max_attempts).
const DefaultMaxAttempts = 3

func (s *PGJobStore) maxAttempts() int {
	if s.MaxAttempts <= 0 {
		return DefaultMaxAttempts
	}
	return s.MaxAttempts
}

func (s *PGJobStore) lease() float64 {
	if s.Lease <= 0 {
		return 60
	}
	return s.Lease.Seconds()
}

func (s *PGJobStore) ctx() (context.Context, context.CancelFunc) {
	t := s.Timeout
	if t <= 0 {
		t = 15 * time.Second
	}
	return context.WithTimeout(context.Background(), t)
}

// inTx runs fn in a transaction scoped to tenant (nil tenant = platform-admin maintenance).
func (s *PGJobStore) inTx(ctx context.Context, tenantID string, admin bool, fn func(Tx) error) (err error) {
	if s.DB == nil {
		return errors.New("export: no database")
	}
	if !admin && !uuidRe.MatchString(tenantID) {
		return fmt.Errorf("export: invalid tenant id")
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if admin {
		_, err = tx.Exec(ctx, sqlAdminOn)
	} else {
		_, err = tx.Exec(ctx, sqlSetTenant, tenantID)
	}
	if err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func scanJob(r Row) (Job, error) {
	var j Job
	var spec string
	var st, fin *time.Time
	if err := r.Scan(&j.ID, &j.TenantID, &j.Subject, &j.Status, &spec, &j.ObjectKey, &j.Rows, &j.Bytes, &j.ErrorCode, &j.CreatedAt, &st, &fin, &j.Attempts); err != nil {
		return Job{}, err
	}
	if err := json.Unmarshal([]byte(spec), &j.Spec); err != nil {
		return Job{}, fmt.Errorf("export: corrupt spec: %w", err)
	}
	if st != nil {
		j.StartedAt = *st
	}
	if fin != nil {
		j.FinishedAt = *fin
	}
	return j, nil
}

func (s *PGJobStore) insert(tx Tx, ctx context.Context, j Job, owner string) error {
	spec, err := json.Marshal(j.Spec)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, sqlInsert, j.ID, j.TenantID, j.Subject, StatusQueued, string(spec), j.Spec.Hash(), j.ObjectKey, owner, s.lease())
	return err
}

// Create inserts a QUEUED job owned by this worker (lease = now+Lease).
func (s *PGJobStore) Create(j Job) error {
	ctx, cancel := s.ctx()
	defer cancel()
	if !uuidRe.MatchString(j.ID) {
		return errors.New("export: invalid job id")
	}
	return s.inTx(ctx, j.TenantID, false, func(tx Tx) error { return s.insert(tx, ctx, j, s.WorkerID) })
}

// Enqueue inserts a QUEUED job with no owner; any replica may Claim it.
func (s *PGJobStore) Enqueue(j Job) error {
	ctx, cancel := s.ctx()
	defer cancel()
	if !uuidRe.MatchString(j.ID) {
		return errors.New("export: invalid job id")
	}
	return s.inTx(ctx, j.TenantID, false, func(tx Tx) error { return s.insert(tx, ctx, j, "") })
}

// Get is tenant-scoped: another tenant's id is ErrJobNotFound (RLS plus an explicit tenant predicate).
func (s *PGJobStore) Get(tenantID, id string) (Job, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	if !uuidRe.MatchString(id) {
		return Job{}, ErrJobNotFound
	}
	var j Job
	err := s.inTx(ctx, tenantID, false, func(tx Tx) (e error) {
		j, e = scanJob(tx.QueryRow(ctx, sqlGet, id, tenantID))
		return e
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}
	return j, err
}

// Update locks the row (FOR UPDATE), applies fn and writes the result. A job entering RUNNING is owned by this
// worker with a fresh lease; a terminal job releases ownership.
func (s *PGJobStore) Update(tenantID, id string, fn func(*Job)) (Job, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	if !uuidRe.MatchString(id) {
		return Job{}, ErrJobNotFound
	}
	var out Job
	err := s.inTx(ctx, tenantID, false, func(tx Tx) error {
		j, e := scanJob(tx.QueryRow(ctx, sqlGetForUpd, id, tenantID))
		if e != nil {
			return e
		}
		fn(&j)
		j.ID, j.TenantID = id, tenantID
		_, e = tx.Exec(ctx, sqlUpdate, id, tenantID, j.Status, j.Rows, j.Bytes, j.ObjectKey, j.ErrorCode,
			nullTime(j.StartedAt), nullTime(j.FinishedAt), s.WorkerID, s.lease())
		out = j
		return e
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}
	return out, err
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// FailUnfinished fails only THIS worker's non-terminal jobs (other replicas' jobs are theirs).
func (s *PGJobStore) FailUnfinished(code string, at time.Time) int {
	ctx, cancel := s.ctx()
	defer cancel()
	var n int64
	if err := s.inTx(ctx, "", true, func(tx Tx) (e error) {
		n, e = tx.Exec(ctx, sqlFailOwn, code, at, s.WorkerID)
		return e
	}); err != nil {
		return 0
	}
	return int(n)
}

// RecoverStats is the outcome of one recovery pass.
type RecoverStats struct{ Requeued, Failed int }

// RecoverJobs handles jobs whose owner's lease expired, in one transaction (safe on every replica, periodically):
// claimed-but-not-started jobs return to QUEUED with attempts+1 (until MaxAttempts, then FAILED max_attempts);
// started jobs are FAILED worker_lost and never re-run.
func (s *PGJobStore) RecoverJobs(ctx context.Context) (st RecoverStats, err error) {
	err = s.inTx(ctx, "", true, func(tx Tx) error {
		n, e := tx.Exec(ctx, sqlRequeue, s.maxAttempts())
		if e != nil {
			return e
		}
		st.Requeued = int(n)
		n, e = tx.Exec(ctx, sqlRecover)
		st.Failed = int(n)
		return e
	})
	if err != nil {
		return RecoverStats{}, err
	}
	return st, nil
}

// Recover is RecoverJobs returning only the number of jobs FAILED.
func (s *PGJobStore) Recover(ctx context.Context) (int, error) {
	st, err := s.RecoverJobs(ctx)
	return st.Failed, err
}

// Start moves a job claimed by this worker from QUEUED to RUNNING (started_at = now). ok=false means the claim
// was lost (recovered or released); the caller must not run it.
func (s *PGJobStore) Start(ctx context.Context, j Job) (ok bool, err error) {
	err = s.inTx(ctx, j.TenantID, false, func(tx Tx) error {
		n, e := tx.Exec(ctx, sqlStart, j.ID, j.TenantID, s.WorkerID, s.lease())
		ok = n == 1
		return e
	})
	return ok && err == nil, err
}

// Finish writes the terminal state of a RUNNING job still owned by this worker. ok=false: the lease was lost and
// another replica already failed it; the late result is ignored.
func (s *PGJobStore) Finish(ctx context.Context, j Job, r JobResult) (ok bool, err error) {
	err = s.inTx(ctx, j.TenantID, false, func(tx Tx) error {
		n, e := tx.Exec(ctx, sqlFinish, j.ID, j.TenantID, s.WorkerID, r.Status, r.Rows, r.Bytes, r.ObjectKey, r.ErrorCode)
		ok = n == 1
		return e
	})
	return ok && err == nil, err
}

// Release gives a claimed-but-not-started job back to the queue (no attempt is consumed).
func (s *PGJobStore) Release(ctx context.Context, j Job) error {
	return s.inTx(ctx, j.TenantID, false, func(tx Tx) error {
		_, e := tx.Exec(ctx, sqlRelease, j.ID, j.TenantID, s.WorkerID)
		return e
	})
}

// Heartbeat extends the lease of this worker's non-terminal jobs.
func (s *PGJobStore) Heartbeat(ctx context.Context) (int, error) {
	var n int64
	err := s.inTx(ctx, "", true, func(tx Tx) (e error) {
		n, e = tx.Exec(ctx, sqlHeartbeat, s.WorkerID, s.lease(), s.abandonedIDs())
		return e
	})
	return int(n), err
}

// HeartbeatOwned is Heartbeat that also returns the ids of the jobs whose lease it extended; a running job that is
// missing (recovered as worker_lost by another replica) has lost its lease and the Dispatcher cancels it.
func (s *PGJobStore) HeartbeatOwned(ctx context.Context) ([]string, error) {
	var ids []string
	err := s.inTx(ctx, "", true, func(tx Tx) error {
		return tx.QueryRow(ctx, sqlHeartbeatOwned, s.WorkerID, s.lease(), s.abandonedIDs()).Scan(&ids)
	})
	return ids, err
}

// Claim takes one unowned QUEUED job with FOR UPDATE SKIP LOCKED and leases it to this worker; it stays QUEUED
// until Start (a claim whose owner dies before Start is re-queued by RecoverJobs). The tenant for the write is set from the claimed row and the
// platform-admin switch is turned off again before the write. ok=false means nothing to claim.
func (s *PGJobStore) Claim(ctx context.Context) (j Job, ok bool, err error) {
	err = s.inTx(ctx, "", true, func(tx Tx) error {
		var id, tenant string
		if e := tx.QueryRow(ctx, sqlClaimPick).Scan(&id, &tenant); e != nil {
			if errors.Is(e, pgx.ErrNoRows) {
				return nil
			}
			return e
		}
		if _, e := tx.Exec(ctx, sqlAdminOff); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, sqlSetTenant, tenant); e != nil {
			return e
		}
		var e error
		if j, e = scanJob(tx.QueryRow(ctx, sqlClaimSet, id, tenant, s.WorkerID, s.lease())); e != nil {
			return e
		}
		ok = true
		return nil
	})
	if err != nil {
		return Job{}, false, err
	}
	return j, ok, nil
}

// PGQuota is the durable QuotaChecker: counts are derived from export_jobs inside one transaction holding a
// per-tenant advisory lock, so replicas cannot over-grant. Day = UTC day by the database clock.
type PGQuota struct {
	Store         *PGJobStore
	Unowned       bool // insert jobs without an owner so any replica's dispatcher claims them (dispatcher mode)
	MaxPerDay     int  // 0 = unlimited
	MaxConcurrent int  // 0 = unlimited
}

func (q *PGQuota) check(ctx context.Context, tx Tx, tenantID string) error {
	if _, err := tx.Exec(ctx, sqlQuotaLock, tenantID); err != nil {
		return err
	}
	var day, active int64
	if err := tx.QueryRow(ctx, sqlQuotaCount, tenantID).Scan(&day, &active); err != nil {
		return err
	}
	if q.MaxPerDay > 0 && day >= int64(q.MaxPerDay) {
		return ErrQuotaExceeded
	}
	if q.MaxConcurrent > 0 && active >= int64(q.MaxConcurrent) {
		return ErrTooManyConcurrent
	}
	return nil
}

// CreateReserved checks both limits and inserts the QUEUED job atomically (the reservation IS the row; the
// concurrent slot is freed when the job turns terminal, so there is nothing to release).
func (q *PGQuota) CreateReserved(j Job) error {
	ctx, cancel := q.Store.ctx()
	defer cancel()
	if !uuidRe.MatchString(j.ID) {
		return errors.New("export: invalid job id")
	}
	return q.Store.inTx(ctx, j.TenantID, false, func(tx Tx) error {
		if err := q.check(ctx, tx, j.TenantID); err != nil {
			return err
		}
		owner := q.Store.WorkerID
		if q.Unowned {
			owner = ""
		}
		return q.Store.insert(tx, ctx, j, owner)
	})
}

// Acquire is an advisory admission check (QuotaChecker contract); it cannot reserve without a row, so the
// Server uses CreateReserved instead. The release func is a no-op.
func (q *PGQuota) Acquire(tenantID string) (func(), error) {
	ctx, cancel := q.Store.ctx()
	defer cancel()
	if err := q.Store.inTx(ctx, tenantID, false, func(tx Tx) error { return q.check(ctx, tx, tenantID) }); err != nil {
		return nil, err
	}
	return func() {}, nil
}
