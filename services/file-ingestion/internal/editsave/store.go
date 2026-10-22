package editsave

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/astryon/file-ingestion/internal/transform"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Version is a file_edit_versions row.
type Version struct {
	ID, FileObjectID                string
	VersionNumber                   int
	Ops                             []transform.OpSpec
	InputHeaderRow, OutputHeaderRow bool
	OutputDelimiter                 string
	Header                          []string
	Bucket, ObjectKey               string
	ChecksumSHA256                  string
	SizeBytes, RowCount             int64
	RequestedBy                     string
	CreatedAt                       time.Time
}

// NewVersion is what Insert persists; VersionNumber must come from a prior NextVersionNumber call.
type NewVersion struct {
	VersionNumber                     int
	Ops                               []transform.OpSpec
	InputHeaderRow, OutputHeaderRow   bool
	OutputDelimiter                   string
	Header                            []string
	Bucket, ObjectKey, ChecksumSHA256 string
	SizeBytes, RowCount               int64
	RequestedBy                       string
}

// Store is the file_edit_versions repository (tenant always comes from a trusted caller, never a request).
type Store interface {
	NextVersionNumber(ctx context.Context, tenant, fileObjectID string) (int, error)
	Insert(ctx context.Context, tenant, fileObjectID string, v NewVersion) (Version, error)
	List(ctx context.Context, tenant, fileObjectID string) ([]Version, error)
	Get(ctx context.Context, tenant, fileObjectID, versionID string) (Version, error)
}

const (
	sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	sqlNextVer   = `SELECT COALESCE(MAX(version_number), 0) + 1 FROM file_edit_versions
		WHERE tenant_id = $1::uuid AND file_object_id = $2::uuid`
	sqlInsert = `INSERT INTO file_edit_versions
		(tenant_id, file_object_id, version_number, ops, input_header_row, output_header_row, output_delimiter,
		 header, bucket, object_key, checksum_sha256, size_bytes, row_count, requested_by)
		VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, NULLIF($14, ''))
		RETURNING id::text, created_at`
	sqlList = `SELECT id::text, version_number, ops::text, input_header_row, output_header_row, output_delimiter,
			header::text, bucket, object_key, checksum_sha256, size_bytes, row_count, COALESCE(requested_by, ''), created_at
		FROM file_edit_versions WHERE tenant_id = $1::uuid AND file_object_id = $2::uuid AND deleted_at IS NULL
		ORDER BY version_number DESC LIMIT 100`
	sqlGet = `SELECT id::text, version_number, ops::text, input_header_row, output_header_row, output_delimiter,
			header::text, bucket, object_key, checksum_sha256, size_bytes, row_count, COALESCE(requested_by, ''), created_at
		FROM file_edit_versions WHERE tenant_id = $1::uuid AND file_object_id = $2::uuid AND id = $3::uuid AND deleted_at IS NULL`
)

// ErrNotFound mirrors objects.ErrNotFound's role for this store.
var ErrNotFound = errors.New("editsave: version not found")

// PGStore is the PostgreSQL Store under tenant RLS (migration 0015); each call runs in its own tenant-scoped
// transaction, same pattern as listing.PGStore.
type PGStore struct{ Pool *pgxpool.Pool }

func (p PGStore) begin(ctx context.Context, tenant string) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, sqlSetTenant, tenant); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func (p PGStore) NextVersionNumber(ctx context.Context, tenant, fileObjectID string) (int, error) {
	tx, err := p.begin(ctx, tenant)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var n int
	if err := tx.QueryRow(ctx, sqlNextVer, tenant, fileObjectID).Scan(&n); err != nil {
		return 0, err
	}
	return n, tx.Commit(ctx)
}

func (p PGStore) Insert(ctx context.Context, tenant, fileObjectID string, v NewVersion) (Version, error) {
	tx, err := p.begin(ctx, tenant)
	if err != nil {
		return Version{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	ops, _ := json.Marshal(v.Ops)
	header, _ := json.Marshal(v.Header)
	out := Version{
		FileObjectID: fileObjectID, VersionNumber: v.VersionNumber, Ops: v.Ops,
		InputHeaderRow: v.InputHeaderRow, OutputHeaderRow: v.OutputHeaderRow, OutputDelimiter: v.OutputDelimiter,
		Header: v.Header, Bucket: v.Bucket, ObjectKey: v.ObjectKey, ChecksumSHA256: v.ChecksumSHA256,
		SizeBytes: v.SizeBytes, RowCount: v.RowCount, RequestedBy: v.RequestedBy,
	}
	err = tx.QueryRow(ctx, sqlInsert, tenant, fileObjectID, v.VersionNumber, string(ops), v.InputHeaderRow,
		v.OutputHeaderRow, v.OutputDelimiter, string(header), v.Bucket, v.ObjectKey, v.ChecksumSHA256,
		v.SizeBytes, v.RowCount, v.RequestedBy).Scan(&out.ID, &out.CreatedAt)
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		return Version{}, ErrVersionConflict
	}
	if err != nil {
		return Version{}, err
	}
	return out, tx.Commit(ctx)
}

func scanVersion(row pgx.Row) (Version, error) {
	var v Version
	var ops, header string
	err := row.Scan(&v.ID, &v.VersionNumber, &ops, &v.InputHeaderRow, &v.OutputHeaderRow, &v.OutputDelimiter,
		&header, &v.Bucket, &v.ObjectKey, &v.ChecksumSHA256, &v.SizeBytes, &v.RowCount, &v.RequestedBy, &v.CreatedAt)
	if err != nil {
		return Version{}, err
	}
	_ = json.Unmarshal([]byte(ops), &v.Ops)
	_ = json.Unmarshal([]byte(header), &v.Header)
	return v, nil
}

func (p PGStore) Get(ctx context.Context, tenant, fileObjectID, versionID string) (Version, error) {
	tx, err := p.begin(ctx, tenant)
	if err != nil {
		return Version{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	v, err := scanVersion(tx.QueryRow(ctx, sqlGet, tenant, fileObjectID, versionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, ErrNotFound
	}
	if err != nil {
		return Version{}, err
	}
	v.FileObjectID = fileObjectID
	return v, tx.Commit(ctx)
}

func (p PGStore) List(ctx context.Context, tenant, fileObjectID string) ([]Version, error) {
	tx, err := p.begin(ctx, tenant)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, sqlList, tenant, fileObjectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Version
	for rows.Next() {
		v, err := scanVersion(rows)
		if err != nil {
			return nil, err
		}
		v.FileObjectID = fileObjectID
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, tx.Commit(ctx)
}
