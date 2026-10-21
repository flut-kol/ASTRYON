package retrybridge

import (
	"context"
	"database/sql"

	"github.com/astryon/file-ingestion/internal/jobs"
)

const (
	sqlRewind = `UPDATE jobs SET attempt_count = attempt_count - 1 WHERE id = $1::uuid AND tenant_id = $2::uuid AND locked_by = $3
		AND attempt_count = $4 AND attempt_count > 0 AND status = 'RUNNING' AND locked_until > now()`
	sqlEnd = `UPDATE job_attempts SET status = $4, finished_at = now(), error_code = NULLIF($5,'')
		WHERE tenant_id = $1::uuid AND job_id = $2::uuid AND attempt_no = $3`
)

// SQLLedger is the database/sql Ledger; DB must connect as the same non-superuser role as the job store so RLS applies.
type SQLLedger struct {
	DB       *sql.DB
	WorkerID string
}

func (l SQLLedger) tx(ctx context.Context, tenant string, f func(*sql.Tx) error) error {
	tx, err := l.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, tenant); err != nil {
		return err
	}
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (l SQLLedger) Rewind(ctx context.Context, j jobs.Job) error {
	return l.tx(ctx, j.TenantID, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, sqlRewind, j.ID, j.TenantID, l.WorkerID, j.Fence)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return jobs.ErrFenced
		}
		return nil
	})
}

func (l SQLLedger) End(ctx context.Context, j jobs.Job, status, code string) error {
	return l.tx(ctx, j.TenantID, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, sqlEnd, j.TenantID, j.ID, j.Fence, status, code)
		return err
	})
}
