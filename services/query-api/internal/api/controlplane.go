package api

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

type Source struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Kind      string    `json:"kind"`
	IsEnabled bool      `json:"is_enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Job struct {
	ID              string     `json:"id"`
	Kind            string     `json:"kind"`
	Status          string     `json:"status"`
	SourceID        *string    `json:"source_id"`
	BatchID         *string    `json:"batch_id"`
	AttemptCount    int        `json:"attempt_count"`
	MaxAttempts     int        `json:"max_attempts"`
	EventsProcessed int64      `json:"events_processed"`
	EventsFailed    int64      `json:"events_failed"`
	LastError       *string    `json:"last_error"`
	CreatedAt       time.Time  `json:"created_at"`
	StartedAt       *time.Time `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
}

// ListPage is a keyset page: items strictly older than After (created_at,id)
// descending. After==nil means first page. Implementations fetch Limit rows
// (callers pass limit+1 to detect a next page).
type ListPage struct {
	Limit int
	After *Keyset
}

type JobFilter struct {
	Status   string
	SourceID string
}

var JobStatuses = map[string]bool{"NEW": true, "QUEUED": true, "RUNNING": true, "RETRYING": true, "SUCCEEDED": true, "FAILED": true, "CANCELLED": true}

// ControlPlane is the PostgreSQL-backed read model (sources, jobs). The
// tenant always comes from the validated principal.
type ControlPlane interface {
	ListSources(ctx context.Context, tenant string, p ListPage) ([]Source, error)
	ListJobs(ctx context.Context, tenant string, f JobFilter, p ListPage) ([]Job, error)
	Ping(ctx context.Context) error
}

// PGControlPlane implements ControlPlane with database/sql (pgx driver).
// Defence in depth: every query has an explicit tenant_id predicate AND runs
// in a transaction with astryon.tenant_id set so row-level security applies.
type PGControlPlane struct{ DB *sql.DB }

func OpenPG(dsn string) (*PGControlPlane, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &PGControlPlane{DB: db}, nil
}

func (p *PGControlPlane) Ping(ctx context.Context) error { return p.DB.PingContext(ctx) }

func (p *PGControlPlane) tx(ctx context.Context, tenant string, fn func(*sql.Tx) error) error {
	tx, err := p.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, tenant); err != nil {
		return err
	}
	return fn(tx)
}

func (p *PGControlPlane) ListSources(ctx context.Context, tenant string, pg ListPage) ([]Source, error) {
	var out []Source
	err := p.tx(ctx, tenant, func(tx *sql.Tx) error {
		q := `SELECT id::text, name, kind::text, is_enabled, created_at, updated_at FROM sources
		      WHERE tenant_id = $1::uuid AND deleted_at IS NULL`
		args := []any{tenant}
		if pg.After != nil {
			q += ` AND (created_at, id) < ($2, $3::uuid)`
			args = append(args, pg.After.Time, pg.After.ID)
		}
		q += ` ORDER BY created_at DESC, id DESC LIMIT ` + itoa(pg.Limit)
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var s Source
			if err := rows.Scan(&s.ID, &s.Name, &s.Kind, &s.IsEnabled, &s.CreatedAt, &s.UpdatedAt); err != nil {
				return err
			}
			out = append(out, s)
		}
		return rows.Err()
	})
	return out, err
}

func (p *PGControlPlane) ListJobs(ctx context.Context, tenant string, f JobFilter, pg ListPage) ([]Job, error) {
	var out []Job
	err := p.tx(ctx, tenant, func(tx *sql.Tx) error {
		q := `SELECT id::text, kind, status::text, source_id::text, batch_id::text, attempt_count, max_attempts,
		        events_processed, events_failed, last_error, created_at, started_at, finished_at
		      FROM jobs WHERE tenant_id = $1::uuid AND deleted_at IS NULL`
		args := []any{tenant}
		if f.Status != "" {
			args = append(args, f.Status)
			q += ` AND status = $` + itoa(len(args)) + `::job_status`
		}
		if f.SourceID != "" {
			args = append(args, f.SourceID)
			q += ` AND source_id = $` + itoa(len(args)) + `::uuid`
		}
		if pg.After != nil {
			args = append(args, pg.After.Time, pg.After.ID)
			q += ` AND (created_at, id) < ($` + itoa(len(args)-1) + `, $` + itoa(len(args)) + `::uuid)`
		}
		q += ` ORDER BY created_at DESC, id DESC LIMIT ` + itoa(pg.Limit)
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var j Job
			if err := rows.Scan(&j.ID, &j.Kind, &j.Status, &j.SourceID, &j.BatchID, &j.AttemptCount, &j.MaxAttempts,
				&j.EventsProcessed, &j.EventsFailed, &j.LastError, &j.CreatedAt, &j.StartedAt, &j.FinishedAt); err != nil {
				return err
			}
			out = append(out, j)
		}
		return rows.Err()
	})
	return out, err
}

func itoa(i int) string { return fmtInt(i) }

// ErrControlPlaneDisabled is returned when no DATABASE_URL is configured.
var ErrControlPlaneDisabled = errors.New("control plane (PostgreSQL) not configured")

// DisabledControlPlane is the ControlPlane used without PostgreSQL.
type DisabledControlPlane struct{}

func (DisabledControlPlane) ListSources(context.Context, string, ListPage) ([]Source, error) {
	return nil, ErrControlPlaneDisabled
}
func (DisabledControlPlane) ListJobs(context.Context, string, JobFilter, ListPage) ([]Job, error) {
	return nil, ErrControlPlaneDisabled
}
func (DisabledControlPlane) Ping(context.Context) error { return ErrControlPlaneDisabled }
