package agg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Same constants as services/export-worker (pg.go): the platform-admin RLS switch is a constant of this trusted
// platform job, never derived from input. Every read runs in a READ ONLY transaction with transaction-local settings.
const (
	sqlAdminOn  = `SELECT set_config('astryon.platform_admin', 'on', true)`
	sqlTimeout  = `SELECT set_config('statement_timeout', '30000', true)`
	regCols     = `tenant_id::text, source_id::text, id::text, status::text, events_accepted, events_rejected, created_at, updated_at`
	sqlCreated  = `SELECT ` + regCols + ` FROM ingestion_batches WHERE deleted_at IS NULL AND created_at >= $1 AND created_at < $2 ORDER BY created_at, id LIMIT $3`
	sqlByID     = `SELECT ` + regCols + ` FROM ingestion_batches WHERE deleted_at IS NULL AND id = ANY($1::uuid[])`
	maxIDsQuery = 5000
)

// PGRegistry reads ingestion_batches with a non-superuser role subject to RLS (policy tenant_isolation).
type PGRegistry struct{ Conn *pgx.Conn }

// NewPGRegistry connects to the registry (REGISTRY_PG_URL). The role needs SELECT on ingestion_batches only.
func NewPGRegistry(ctx context.Context, url string) (*PGRegistry, error) {
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	return &PGRegistry{Conn: conn}, nil
}

// Close releases the connection.
func (r *PGRegistry) Close(ctx context.Context) error { return r.Conn.Close(ctx) }

func (r *PGRegistry) query(ctx context.Context, sql string, args ...any) ([]RegBatch, error) {
	tx, err := r.Conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, s := range []string{sqlAdminOn, sqlTimeout} {
		if _, err := tx.Exec(ctx, s); err != nil {
			return nil, err
		}
	}
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RegBatch
	for rows.Next() {
		var b RegBatch
		if err := rows.Scan(&b.TenantID, &b.SourceID, &b.BatchID, &b.Status, &b.Accepted, &b.Rejected, &b.CreatedAt, &b.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (r *PGRegistry) BatchesCreated(ctx context.Context, from, to time.Time, limit int) ([]RegBatch, error) {
	return r.query(ctx, sqlCreated, from, to, limit)
}

func (r *PGRegistry) BatchesByID(ctx context.Context, ids []string) ([]RegBatch, error) {
	if len(ids) > maxIDsQuery {
		return nil, fmt.Errorf("too many batch ids (%d)", len(ids))
	}
	return r.query(ctx, sqlByID, ids)
}
