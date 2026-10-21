package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Replay request statuses (enum replay_status in db/postgres/migrations/0005).
const (
	StatusRequested = "REQUESTED"
	StatusFailed    = "FAILED"
	StatusCancelled = "CANCELLED"
)

// Row is the subset of pgx.Row the allocator needs.
type Row interface{ Scan(dest ...any) error }

// Tx is the subset of a PostgreSQL transaction the allocator needs (fakeable in unit tests).
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) error
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

func (t pgxTx) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := t.tx.Exec(ctx, sql, args...)
	return err
}
func (t pgxTx) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return t.tx.QueryRow(ctx, sql, args...)
}
func (t pgxTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t pgxTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

// ReplayRequest is a durable reservation: one replay_requests row.
type ReplayRequest struct {
	ID                string
	ProcessingVersion uint32
}

// RequestStore is the optional richer contract of a durable allocator. The Planner uses it, when the
// configured allocator implements it, to persist the time range and to record the final status.
type RequestStore interface {
	// Reserve allocates the next version AND inserts the REQUESTED row in one transaction.
	Reserve(ctx context.Context, tenantID, sourceID string, from, to time.Time) (ReplayRequest, error)
	// SetStatus moves the row to a terminal status (e.g. FAILED when publishing failed). The version stays burned.
	SetStatus(ctx context.Context, tenantID, requestID, status string) error
}

const (
	sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	sqlLock      = `SELECT pg_advisory_xact_lock(hashtextextended($1::text || $2::text, 0))`
	// Includes soft-deleted rows on purpose: a version is burned forever.
	sqlMax = `SELECT COALESCE(MAX(target_processing_version), 0) FROM replay_requests WHERE tenant_id = $1::uuid AND source_id = $2::uuid`
	sqlIns = `INSERT INTO replay_requests (tenant_id, source_id, target_processing_version, time_from, time_to, status)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, 'REQUESTED') RETURNING id::text`
	sqlStatus = `UPDATE replay_requests SET status = $2::replay_status, completed_at = now() WHERE id = $1::uuid AND tenant_id = $3::uuid AND deleted_at IS NULL`
)

// PGAllocator is a VersionAllocator that is safe across replicas: versions are serialized per
// (tenant, source) by a transaction-scoped advisory lock and recorded in replay_requests.
// Version = GREATEST(max over replay_requests incl. soft-deleted, ClickHouse max) + 1.
// Tenant comes only from the caller (the verified principal) and is set as the RLS session
// setting `astryon.tenant_id` inside the transaction (migration 0001).
type PGAllocator struct {
	DB DB
	// CH supplies the highest version already stored in ClickHouse (only Peek is used). May be nil.
	CH VersionAllocator
}

func (a *PGAllocator) chMax(ctx context.Context, tenantID, sourceID string) (uint32, error) {
	if a.CH == nil {
		return 0, nil
	}
	return a.CH.Peek(ctx, tenantID, sourceID)
}

func (a *PGAllocator) begin(ctx context.Context, tenantID string) (Tx, error) {
	if !uuidRe.MatchString(tenantID) {
		return nil, fmt.Errorf("%w: tenant", ErrInvalid)
	}
	tx, err := a.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err := tx.Exec(ctx, sqlSetTenant, tenantID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func maxVersion(ctx context.Context, tx Tx, tenantID, sourceID string) (uint32, error) {
	var n int32
	if err := tx.QueryRow(ctx, sqlMax, tenantID, sourceID).Scan(&n); err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, errors.New("replay: negative stored version")
	}
	return uint32(n), nil
}

// Peek returns GREATEST(stored replay_requests max, ClickHouse max); no side effects.
func (a *PGAllocator) Peek(ctx context.Context, tenantID, sourceID string) (uint32, error) {
	ch, err := a.chMax(ctx, tenantID, sourceID)
	if err != nil {
		return 0, err
	}
	tx, err := a.begin(ctx, tenantID)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	pg, err := maxVersion(ctx, tx, tenantID, sourceID)
	if err != nil {
		return 0, err
	}
	return max(pg, ch), nil
}

// Allocate reserves a version without a time range (VersionAllocator contract).
func (a *PGAllocator) Allocate(ctx context.Context, tenantID, sourceID string) (uint32, error) {
	r, err := a.Reserve(ctx, tenantID, sourceID, time.Time{}, time.Time{})
	return r.ProcessingVersion, err
}

// Reserve implements RequestStore. The ClickHouse max is read BEFORE taking the lock: it can only
// grow, and any version handed out after it is already in replay_requests, so the lock+max below
// can never produce a duplicate.
func (a *PGAllocator) Reserve(ctx context.Context, tenantID, sourceID string, from, to time.Time) (ReplayRequest, error) {
	if !uuidRe.MatchString(sourceID) {
		return ReplayRequest{}, fmt.Errorf("%w: source_id", ErrInvalid)
	}
	ch, err := a.chMax(ctx, tenantID, sourceID)
	if err != nil {
		return ReplayRequest{}, err
	}
	tx, err := a.begin(ctx, tenantID)
	if err != nil {
		return ReplayRequest{}, err
	}
	defer tx.Rollback(ctx) // no-op after Commit
	if err := tx.Exec(ctx, sqlLock, tenantID, sourceID); err != nil {
		return ReplayRequest{}, err
	}
	pg, err := maxVersion(ctx, tx, tenantID, sourceID)
	if err != nil {
		return ReplayRequest{}, err
	}
	next := max(pg, ch) + 1
	if next > 1<<31-1 {
		return ReplayRequest{}, errors.New("replay: processing_version overflow")
	}
	var tf, tt any
	if !from.IsZero() {
		tf = from.UTC()
	}
	if !to.IsZero() {
		tt = to.UTC()
	}
	var id string
	if err := tx.QueryRow(ctx, sqlIns, tenantID, sourceID, int32(next), tf, tt).Scan(&id); err != nil {
		if isVersionConflict(err) {
			return ReplayRequest{}, fmt.Errorf("%w: version %d already reserved", ErrConflict, next)
		}
		return ReplayRequest{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ReplayRequest{}, err
	}
	return ReplayRequest{ID: id, ProcessingVersion: next}, nil
}

// SetStatus implements RequestStore.
func (a *PGAllocator) SetStatus(ctx context.Context, tenantID, requestID, status string) error {
	tx, err := a.begin(ctx, tenantID)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := tx.Exec(ctx, sqlStatus, requestID, status, tenantID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// isVersionConflict: unique violation (23505) of replay_requests_version_uq.
func isVersionConflict(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505" && pe.ConstraintName == "replay_requests_version_uq"
}
