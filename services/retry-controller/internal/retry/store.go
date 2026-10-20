package retry

import (
	"context"
	"database/sql"
	"time"
)

// PostgresStore schedules and claims retries atomically. It uses the existing
// scheduled_retries/jobs schema and never records status per event.
type PostgresStore struct{ DB *sql.DB }

// ClaimDue makes at most limit due retries visible to workers. FOR UPDATE SKIP
// LOCKED prevents two controller replicas from firing the same retry.
func (s PostgresStore) ClaimDue(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	result, err := s.DB.ExecContext(ctx, `
WITH due AS (
  SELECT id, tenant_id, job_id FROM scheduled_retries
  WHERE retry_at <= now() AND fired_at IS NULL AND cancelled_at IS NULL
  ORDER BY retry_at FOR UPDATE SKIP LOCKED LIMIT $1
), fired AS (
  UPDATE scheduled_retries r SET fired_at = now() FROM due
  WHERE r.id = due.id RETURNING due.tenant_id, due.job_id
)
UPDATE jobs j SET status = 'QUEUED', run_after = now(), locked_by = NULL, locked_until = NULL
FROM fired WHERE j.tenant_id = fired.tenant_id AND j.id = fired.job_id AND j.status = 'RETRYING'`, limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Schedule inserts retry metadata and transitions the tenant-scoped job in one
// transaction. The caller supplies only a classified, bounded delay.
func (s PostgresStore) Schedule(ctx context.Context, tenantID, jobID string, delay time.Duration, reason string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Keep the attempt budget in PostgreSQL so concurrent workers cannot bypass it.
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET status = 'RETRYING', attempt_count = attempt_count + 1, run_after = now() + $3::interval, locked_by = NULL, locked_until = NULL, last_error = $4 WHERE tenant_id = $1 AND id = $2 AND status = 'RUNNING' AND attempt_count < max_attempts`, tenantID, jobID, delay.String(), reason)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return sql.ErrNoRows
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO scheduled_retries (tenant_id, job_id, retry_at, reason, backoff_ms) VALUES ($1, $2, now() + $3::interval, $4, $5)`, tenantID, jobID, delay.String(), reason, delay.Milliseconds())
	if err != nil {
		return err
	}
	return tx.Commit()
}
