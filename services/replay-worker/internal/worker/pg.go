package worker

import (
	"context"
	"fmt"
	"time"

	ec "github.com/astryon/event-contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Tx / DB are the subset of PostgreSQL the updater needs (fakeable in unit tests).
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) (rowsAffected int64, err error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type DB interface {
	Begin(ctx context.Context) (Tx, error)
}

// PoolDB adapts a pgxpool.Pool.
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
	ct, err := t.tx.Exec(ctx, sql, args...)
	return ct.RowsAffected(), err
}
func (t pgxTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t pgxTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

const (
	sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	// Allowed transitions. RUNNING is claimed atomically from REQUESTED/APPROVED/FAILED, or from a RUNNING row
	// whose last update is older than the lease (a crashed worker; a fresh RUNNING row belongs to a live worker, so
	// a racing second worker updates 0 rows: PostgreSQL re-evaluates the WHERE after the row lock).
	// CANCELLED and COMPLETED rows are never touched. Terminal states only leave RUNNING.
	sqlRunning = `UPDATE replay_requests SET status = 'RUNNING'
WHERE tenant_id = $1::uuid AND source_id = $2::uuid AND target_processing_version = $3 AND deleted_at IS NULL
  AND (status IN ('REQUESTED','APPROVED','FAILED')
       OR (status = 'RUNNING' AND updated_at < now() - make_interval(secs => $4::double precision)))`
	sqlIsRunning = `SELECT 1 FROM replay_requests
WHERE tenant_id = $1::uuid AND source_id = $2::uuid AND target_processing_version = $3 AND deleted_at IS NULL AND status = 'RUNNING'`
	sqlTerminal = `UPDATE replay_requests SET status = $4::replay_status, completed_at = now()
WHERE tenant_id = $1::uuid AND source_id = $2::uuid AND target_processing_version = $3 AND deleted_at IS NULL
  AND status = 'RUNNING'`
)

// PGStatus implements StatusUpdater. Same RLS pattern as replay-controller/internal/replay/pg.go: the tenant
// (the job's, i.e. the verified principal's at request time) is set as `astryon.tenant_id` inside the
// transaction; the role must NOT bypass RLS. The tenant is also in every WHERE clause.
type PGStatus struct {
	DB DB
	// RunningLease: a RUNNING row not updated for this long is considered abandoned and may be re-claimed
	// (redelivery after a hard crash). Zero = DefaultRunningLease. Must exceed the longest job.
	RunningLease time.Duration
}

const DefaultRunningLease = time.Hour

var _ StatusUpdater = (*PGStatus)(nil)

func (p *PGStatus) SetStatus(ctx context.Context, tenantID, sourceID string, version uint32, status string) error {
	if !ec.IsUUID(tenantID) || !ec.IsUUID(sourceID) || version < 1 || version > 1<<31-1 {
		return fmt.Errorf("replay-worker: invalid request identity")
	}
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op after Commit
	if _, err := tx.Exec(ctx, sqlSetTenant, tenantID); err != nil {
		return err
	}
	var n int64
	switch status {
	case StatusRunning:
		lease := p.RunningLease
		if lease <= 0 {
			lease = DefaultRunningLease
		}
		n, err = tx.Exec(ctx, sqlRunning, tenantID, sourceID, int32(version), lease.Seconds())
	case StatusCompleted, StatusFailed:
		n, err = tx.Exec(ctx, sqlTerminal, tenantID, sourceID, int32(version), status)
	default:
		return fmt.Errorf("replay-worker: unsupported status %q", status)
	}
	if err != nil {
		return err
	}
	if n == 0 {
		if status == StatusRunning {
			// A SELECT through Exec reports the number of rows it returned.
			rows, err := tx.Exec(ctx, sqlIsRunning, tenantID, sourceID, int32(version))
			if err != nil {
				return err
			}
			if rows > 0 {
				return ErrRunningRecently
			}
		}
		return ErrNotRunnable
	}
	return tx.Commit(ctx)
}
