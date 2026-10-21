// Package pullstore persists REST pull state (migration 0011): durable cursor, retry schedule and a fenced lease.
package pullstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNoConnector = errors.New("pullstore: no connector is due")
	ErrFenced      = errors.New("pullstore: lease lost (fenced)")
)

// Claimed is a due REST_PULL connector held under a lease. Fence must be passed back on every write.
type Claimed struct {
	TenantID, ConnectorID, SourceID string
	Config                          map[string]any
	SecretRef                       string // from source_credentials.secret_ref, empty if the connector has none
	Cursor                          string
	CursorUpdatedAt                 time.Time
	ConsecutiveFailures             int
	Fence                           int64
}

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

// Cross-tenant scans switch platform_admin on with a constant; per-connector writes run under the connector's tenant.
const (
	sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	sqlAdminOn   = `SELECT set_config('astryon.platform_admin', 'on', true)`

	sqlSync = `INSERT INTO connector_pull_state (tenant_id, connector_id)
		SELECT tenant_id, id FROM connectors WHERE connector_type = 'REST_PULL' AND is_enabled AND deleted_at IS NULL
		ON CONFLICT DO NOTHING`
	sqlPick = `SELECT s.tenant_id::text, s.connector_id::text FROM connector_pull_state s
		JOIN connectors c ON c.tenant_id = s.tenant_id AND c.id = s.connector_id
		WHERE c.connector_type = 'REST_PULL' AND c.is_enabled AND c.deleted_at IS NULL
		  AND s.next_attempt_at <= now() AND (s.locked_until IS NULL OR s.locked_until < now())
		ORDER BY s.next_attempt_at, s.connector_id LIMIT 1 FOR UPDATE OF s SKIP LOCKED`
	sqlClaim = `UPDATE connector_pull_state SET locked_by = $3, locked_until = now() + make_interval(secs => $4), fence = fence + 1
		WHERE tenant_id = $1::uuid AND connector_id = $2::uuid
		RETURNING cursor, COALESCE(cursor_updated_at, 'epoch'::timestamptz), consecutive_failures, fence`
	sqlLoad = `SELECT c.source_id::text, c.config::text, COALESCE(cr.secret_ref, '')
		FROM connectors c LEFT JOIN source_credentials cr ON cr.id = c.credential_id AND cr.tenant_id = c.tenant_id
		WHERE c.tenant_id = $1::uuid AND c.id = $2::uuid`

	fenced = ` WHERE tenant_id = $1::uuid AND connector_id = $2::uuid AND locked_by = $3 AND fence = $4 AND locked_until > now()`

	sqlSucceed = `UPDATE connector_pull_state SET cursor = CASE WHEN $5 <> '' THEN $5 ELSE cursor END,
		cursor_updated_at = CASE WHEN $5 <> '' THEN now() ELSE cursor_updated_at END, consecutive_failures = 0,
		next_attempt_at = now() + make_interval(secs => $6), last_error = NULL, records_total = records_total + $7,
		locked_by = NULL, locked_until = NULL` + fenced
	sqlFail = `UPDATE connector_pull_state SET consecutive_failures = consecutive_failures + 1,
		next_attempt_at = now() + make_interval(secs => $5), last_error = $6, locked_by = NULL, locked_until = NULL` + fenced
	sqlRenew = `UPDATE connector_pull_state SET locked_until = now() + make_interval(secs => $5)` + fenced
)

// Store is the durable connector pull repository.
type Store struct {
	DB       DB
	WorkerID string
	Lease    time.Duration // default 60s
}

func (s *Store) lease() float64 {
	if s.Lease <= 0 {
		return 60
	}
	return s.Lease.Seconds()
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

// Sync creates state rows for new enabled REST_PULL connectors. Run it periodically on any replica.
func (s *Store) Sync(ctx context.Context) (int64, error) {
	var n int64
	err := s.inTx(ctx, true, "", func(tx Tx) (err error) { n, err = tx.Exec(ctx, sqlSync); return })
	return n, err
}

// Claim leases the next due connector (SKIP LOCKED, new fence) and loads its configuration.
func (s *Store) Claim(ctx context.Context) (Claimed, error) {
	var c Claimed
	err := s.inTx(ctx, true, "", func(tx Tx) error {
		if err := tx.QueryRow(ctx, sqlPick).Scan(&c.TenantID, &c.ConnectorID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNoConnector
			}
			return err
		}
		if err := tx.QueryRow(ctx, sqlClaim, c.TenantID, c.ConnectorID, s.WorkerID, s.lease()).
			Scan(&c.Cursor, &c.CursorUpdatedAt, &c.ConsecutiveFailures, &c.Fence); err != nil {
			return err
		}
		var cfg string
		if err := tx.QueryRow(ctx, sqlLoad, c.TenantID, c.ConnectorID).Scan(&c.SourceID, &cfg, &c.SecretRef); err != nil {
			return err
		}
		return json.Unmarshal([]byte(cfg), &c.Config)
	})
	if err != nil {
		return Claimed{}, err
	}
	return c, nil
}

func (s *Store) fenced(ctx context.Context, c Claimed, sql string, extra ...any) error {
	return s.inTx(ctx, false, c.TenantID, func(tx Tx) error {
		n, err := tx.Exec(ctx, sql, append([]any{c.TenantID, c.ConnectorID, s.WorkerID, c.Fence}, extra...)...)
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrFenced
		}
		return nil
	})
}

// Succeed records a fully handled page: the cursor advances only when nextCursor is non-empty, failures reset and the next
// pull is scheduled after `every`. Call it only after the page was accepted downstream.
func (s *Store) Succeed(ctx context.Context, c Claimed, nextCursor string, every time.Duration, records int64) error {
	return s.fenced(ctx, c, sqlSucceed, nextCursor, every.Seconds(), records)
}

// Fail schedules the retry (delay already includes backoff/Retry-After) and keeps the cursor.
func (s *Store) Fail(ctx context.Context, c Claimed, delay time.Duration, code string) error {
	return s.fenced(ctx, c, sqlFail, delay.Seconds(), code)
}

// Renew extends the lease during a slow page.
func (s *Store) Renew(ctx context.Context, c Claimed) error {
	return s.fenced(ctx, c, sqlRenew, s.lease())
}
