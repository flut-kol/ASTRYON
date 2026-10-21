package cdc

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGPositions stores the last emitted source position per connector in connector_cdc_position (migration 0013).
// Tenant comes from operator configuration (MapConfig.Tenant), applied via RLS setting, never from payloads.
type PGPositions struct {
	Pool   *pgxpool.Pool
	Tenant string
}

const sqlSavePosition = `INSERT INTO connector_cdc_position (tenant_id, connector_id, position)
	VALUES ($1::uuid, $2::uuid, $3)
	ON CONFLICT (tenant_id, connector_id) DO UPDATE SET position = EXCLUDED.position`

func (p *PGPositions) SavePosition(ctx context.Context, connectorID, position string) error {
	if p.Tenant == "" {
		return errors.New("cdc positions: tenant is required")
	}
	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, p.Tenant); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, sqlSavePosition, p.Tenant, connectorID, position); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// LoadPosition returns the stored position ("" if none).
func (p *PGPositions) LoadPosition(ctx context.Context, connectorID string) (string, error) {
	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, p.Tenant); err != nil {
		return "", err
	}
	var pos string
	err = tx.QueryRow(ctx, `SELECT position FROM connector_cdc_position WHERE tenant_id=$1::uuid AND connector_id=$2::uuid`, p.Tenant, connectorID).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return pos, err
}
