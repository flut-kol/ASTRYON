// Package connstore is the PostgreSQL implementation of adminapi.Store. Every call runs in a transaction with
// astryon.tenant_id set from the trusted tenant (RLS) and also filters on tenant_id explicitly.
package connstore

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/astryon/connector-manager/internal/adminapi"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ Pool *pgxpool.Pool }

const (
	sqlTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	cols      = `c.id::text, c.source_id::text, c.name, c.connector_type, c.is_enabled, COALESCE(cr.secret_ref, ''), c.config::text, c.created_at`
	from      = ` FROM connectors c LEFT JOIN source_credentials cr ON cr.id = c.credential_id AND cr.tenant_id = c.tenant_id `
)

func (s *Store) tx(ctx context.Context, tenant string, f func(pgx.Tx) error) error {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, sqlTenant, tenant); err != nil {
		return err
	}
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func scan(row pgx.Row) (adminapi.Connector, error) {
	var c adminapi.Connector
	var cfg string
	if err := row.Scan(&c.ID, &c.SourceID, &c.Name, &c.Kind, &c.Enabled, &c.SecretRef, &cfg, &c.CreatedAt); err != nil {
		return c, err
	}
	if err := json.Unmarshal([]byte(cfg), &c.Config); err != nil {
		return c, err
	}
	if c.Config == nil {
		c.Config = map[string]any{}
	}
	return c, nil
}

func (s *Store) List(ctx context.Context, tenant, after string, limit int) ([]adminapi.Connector, error) {
	var out []adminapi.Connector
	err := s.tx(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+cols+from+`WHERE c.tenant_id = $1::uuid AND c.deleted_at IS NULL
			AND ($2 = '' OR c.id > NULLIF($2, '')::uuid) ORDER BY c.id LIMIT $3`, tenant, after, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			c, err := scan(rows)
			if err != nil {
				return err
			}
			out = append(out, c)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) Get(ctx context.Context, tenant, id string) (adminapi.Connector, error) {
	var c adminapi.Connector
	err := s.tx(ctx, tenant, func(tx pgx.Tx) (err error) {
		c, err = scan(tx.QueryRow(ctx, `SELECT `+cols+from+`WHERE c.tenant_id = $1::uuid AND c.id = $2::uuid AND c.deleted_at IS NULL`, tenant, id))
		return
	})
	return c, mapErr(err)
}

func (s *Store) SetEnabled(ctx context.Context, tenant, id string, enabled bool) (adminapi.Connector, error) {
	var c adminapi.Connector
	err := s.tx(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE connectors SET is_enabled = $3 WHERE tenant_id = $1::uuid AND id = $2::uuid AND deleted_at IS NULL`, tenant, id, enabled)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return pgx.ErrNoRows
		}
		c, err = scan(tx.QueryRow(ctx, `SELECT `+cols+from+`WHERE c.tenant_id = $1::uuid AND c.id = $2::uuid`, tenant, id))
		return err
	})
	return c, mapErr(err)
}

func (s *Store) Create(ctx context.Context, tenant string, in adminapi.CreateInput) (adminapi.Connector, error) {
	var c adminapi.Connector
	cfg := in.Config
	if cfg == nil {
		cfg = map[string]string{}
	}
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return c, err
	}
	err = s.tx(ctx, tenant, func(tx pgx.Tx) error {
		var src string
		if err := tx.QueryRow(ctx, `SELECT id::text FROM sources WHERE tenant_id = $1::uuid AND id = $2::uuid AND deleted_at IS NULL`, tenant, in.SourceID).Scan(&src); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return adminapi.ErrBadReference
			}
			return err
		}
		var cred string
		if err := tx.QueryRow(ctx, `INSERT INTO source_credentials (tenant_id, source_id, name, secret_ref) VALUES ($1::uuid, $2::uuid, $3, $4) RETURNING id::text`,
			tenant, in.SourceID, in.Name, in.SecretRef).Scan(&cred); err != nil {
			return err
		}
		var id string
		if err := tx.QueryRow(ctx, `INSERT INTO connectors (tenant_id, source_id, credential_id, name, connector_type, config)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6::jsonb) RETURNING id::text`, tenant, in.SourceID, cred, in.Name, in.Kind, string(cfgJSON)).Scan(&id); err != nil {
			return err
		}
		c, err = scan(tx.QueryRow(ctx, `SELECT `+cols+from+`WHERE c.tenant_id = $1::uuid AND c.id = $2::uuid`, tenant, id))
		return err
	})
	return c, mapErr(err)
}

func mapErr(err error) error {
	var pe *pgconn.PgError
	switch {
	case err == nil:
		return nil
	case errors.Is(err, pgx.ErrNoRows):
		return adminapi.ErrNotFound
	case errors.As(err, &pe) && pe.Code == "23505":
		return adminapi.ErrConflict
	case errors.As(err, &pe) && pe.Code == "23503":
		return adminapi.ErrBadReference
	}
	return err
}

const sourceCols = `s.id::text, s.name, s.kind::text, s.is_enabled,
	(SELECT count(*) FROM connectors c WHERE c.tenant_id = s.tenant_id AND c.source_id = s.id AND c.deleted_at IS NULL)::int,
	s.created_at, s.updated_at`

// ListSources implements adminapi.SourceStore (read-only; config and credentials are never selected).
func (s *Store) ListSources(ctx context.Context, tenant, after string, limit int) ([]adminapi.Source, error) {
	var out []adminapi.Source
	err := s.tx(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+sourceCols+` FROM sources s WHERE s.tenant_id = $1::uuid AND s.deleted_at IS NULL
			AND ($2 = '' OR s.id > NULLIF($2, '')::uuid) ORDER BY s.id LIMIT $3`, tenant, after, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var x adminapi.Source
			if err := rows.Scan(&x.ID, &x.Name, &x.Kind, &x.Enabled, &x.ConnectorCount, &x.CreatedAt, &x.UpdatedAt); err != nil {
				return err
			}
			out = append(out, x)
		}
		return rows.Err()
	})
	return out, err
}

// GetSource implements adminapi.SourceStore.
func (s *Store) GetSource(ctx context.Context, tenant, id string) (adminapi.Source, error) {
	var x adminapi.Source
	err := s.tx(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT `+sourceCols+` FROM sources s WHERE s.tenant_id = $1::uuid AND s.id = $2::uuid AND s.deleted_at IS NULL`, tenant, id).
			Scan(&x.ID, &x.Name, &x.Kind, &x.Enabled, &x.ConnectorCount, &x.CreatedAt, &x.UpdatedAt)
	})
	return x, mapErr(err)
}
