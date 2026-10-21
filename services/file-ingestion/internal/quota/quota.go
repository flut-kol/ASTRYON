// Package quota enforces per-tenant upload quotas for file-api: file size, concurrently open uploads and bytes
// registered in the last 24 hours. The tenant is always passed in by the caller from the validated Principal.
//
// Usage is derived from file_objects (migration 0004), so there is no extra state to drift: an "open" upload is a
// row that is neither committed nor deleted and is younger than OpenWindow; "bytes per day" sums the declared size
// of every non-deleted row created in the last 24 hours (rejected rows set deleted_at and stop counting).
// Per-tenant overrides live in tenant_quotas (migration 0006), resources upload_file_max_bytes (period total),
// upload_open_max (total) and upload_bytes (day); a missing row falls back to the service defaults, 0 = unlimited.
package quota

import (
	"context"
	"errors"
	"time"

	"github.com/astryon/file-ingestion/internal/jobs"
)

var (
	ErrFileTooLarge   = errors.New("quota: file exceeds the tenant maximum file size")
	ErrTooManyOpen    = errors.New("quota: too many concurrently open uploads")
	ErrDailyBytesFull = errors.New("quota: daily upload byte quota exceeded")
)

// OpenWindow bounds how long an uncompleted upload keeps counting as open (part URLs expire after 15 minutes).
const OpenWindow = time.Hour

// Limits are one tenant's effective limits; 0 means unlimited.
type Limits struct {
	MaxFileBytes int64
	MaxOpen      int64
	BytesPerDay  int64
}

// Usage is the tenant's current consumption.
type Usage struct {
	Open  int64
	Bytes int64
}

// Store reads tenant overrides and usage.
type Store interface {
	// Overrides returns tenant_quotas values keyed by resource name (only rows that exist).
	Overrides(ctx context.Context, tenant string) (map[string]int64, error)
	Usage(ctx context.Context, tenant string) (Usage, error)
}

// Checker applies defaults, overrides and usage to a new upload request.
type Checker struct {
	Store    Store
	Defaults Limits
}

// Effective merges the tenant's overrides over the defaults.
func (c Checker) Effective(ctx context.Context, tenant string) (Limits, error) {
	l := c.Defaults
	ov, err := c.Store.Overrides(ctx, tenant)
	if err != nil {
		return Limits{}, err
	}
	if v, ok := ov[ResFileMax]; ok {
		l.MaxFileBytes = v
	}
	if v, ok := ov[ResOpenMax]; ok {
		l.MaxOpen = v
	}
	if v, ok := ov[ResBytesDay]; ok {
		l.BytesPerDay = v
	}
	return l, nil
}

// Check returns nil, ErrFileTooLarge, ErrTooManyOpen or ErrDailyBytesFull (or a store error, which callers
// should treat as unavailable: quotas fail closed).
func (c Checker) Check(ctx context.Context, tenant string, size int64) error {
	l, err := c.Effective(ctx, tenant)
	if err != nil {
		return err
	}
	if l.MaxFileBytes > 0 && size > l.MaxFileBytes {
		return ErrFileTooLarge
	}
	if l.MaxOpen == 0 && l.BytesPerDay == 0 {
		return nil
	}
	u, err := c.Store.Usage(ctx, tenant)
	if err != nil {
		return err
	}
	if l.MaxOpen > 0 && u.Open >= l.MaxOpen {
		return ErrTooManyOpen
	}
	if l.BytesPerDay > 0 && u.Bytes+size > l.BytesPerDay {
		return ErrDailyBytesFull
	}
	return nil
}

// tenant_quotas resource names.
const (
	ResFileMax  = "upload_file_max_bytes"
	ResOpenMax  = "upload_open_max"
	ResBytesDay = "upload_bytes"
)

const (
	sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	sqlUsage     = `SELECT count(*) FILTER (WHERE committed_at IS NULL AND created_at > now() - make_interval(secs => $2)),
		COALESCE(sum(size_bytes), 0)::bigint
		FROM file_objects WHERE tenant_id = $1::uuid AND deleted_at IS NULL AND created_at > now() - interval '24 hours'`
	sqlOverrides = `SELECT
		COALESCE(max(limit_value) FILTER (WHERE resource = 'upload_file_max_bytes' AND period = 'total'), -1),
		COALESCE(max(limit_value) FILTER (WHERE resource = 'upload_open_max' AND period = 'total'), -1),
		COALESCE(max(limit_value) FILTER (WHERE resource = 'upload_bytes' AND period = 'day'), -1)
		FROM tenant_quotas WHERE tenant_id = $1::uuid AND deleted_at IS NULL`
)

// PGStore is the PostgreSQL Store under tenant RLS.
type PGStore struct{ DB jobs.DB }

func (p PGStore) begin(ctx context.Context, tenant string) (jobs.Tx, error) {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, sqlSetTenant, tenant); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func (p PGStore) Usage(ctx context.Context, tenant string) (Usage, error) {
	tx, err := p.begin(ctx, tenant)
	if err != nil {
		return Usage{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var u Usage
	err = tx.QueryRow(ctx, sqlUsage, tenant, OpenWindow.Seconds()).Scan(&u.Open, &u.Bytes)
	return u, err
}

func (p PGStore) Overrides(ctx context.Context, tenant string) (map[string]int64, error) {
	tx, err := p.begin(ctx, tenant)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var f, o, d int64
	if err := tx.QueryRow(ctx, sqlOverrides, tenant).Scan(&f, &o, &d); err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for k, v := range map[string]int64{ResFileMax: f, ResOpenMax: o, ResBytesDay: d} {
		if v >= 0 {
			out[k] = v
		}
	}
	return out, nil
}
