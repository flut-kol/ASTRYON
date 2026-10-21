// Package objects connects file_objects rows to S3: registering an upload, verifying its SHA-256 before the
// row is committed, and exposing only verified objects to workers (worker.Source).
package objects

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrChecksumMismatch = errors.New("objects: uploaded content does not match the declared SHA-256 or size")
	ErrNotVerified      = errors.New("objects: file object is not committed")
	ErrNotFound         = errors.New("objects: file object not found")
	ErrUnknownSource    = errors.New("objects: source does not exist for this tenant")
	ErrAlreadyExists    = errors.New("objects: file object already registered")
)

// File is the file_objects row the workers and the verifier need.
type File struct {
	ID, SourceID, Bucket, Key, SHA256 string
	Size                              int64  // declared size, -1 if unknown
	ContentType                       string // declared MIME type, persisted: it selects the worker parser
	Committed                         bool
	SourceType                        string            // processing profile name (internal/profile)
	Sensitivity                       string            // public|internal|restricted
	Tags                              map[string]string // client tags, validated by the API
}

// Store is the file_objects repository (tenant always comes from a trusted caller, never from a request).
type Store interface {
	Register(ctx context.Context, tenant, source string, f File) (string, error)
	Get(ctx context.Context, tenant, id string) (File, error)
	MarkCommitted(ctx context.Context, tenant, id string, size int64) error
	MarkRejected(ctx context.Context, tenant, id string) error
}

// Objects is the S3 subset needed (satisfied by *s3.Client).
type Objects interface {
	Head(ctx context.Context, key string) (int64, error)
	Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}

// Verify streams the object once (bounded memory), compares size and SHA-256 with the declared values and only
// then commits the row. A mismatch deletes the object and rejects the row, so nothing unverified can be processed.
func Verify(ctx context.Context, st Store, o Objects, tenant, id string) error {
	f, err := st.Get(ctx, tenant, id)
	if err != nil {
		return err
	}
	if f.Committed {
		return nil
	}
	rc, err := o.Get(ctx, f.Key, 0)
	if err != nil {
		return err
	}
	defer rc.Close()
	h := sha256.New()
	n, err := io.Copy(h, rc)
	if err != nil {
		return err
	}
	if !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), f.SHA256) || (f.Size >= 0 && n != f.Size) {
		if derr := o.Delete(ctx, f.Key); derr != nil {
			return fmt.Errorf("%w (and delete failed: %v)", ErrChecksumMismatch, derr)
		}
		if rerr := st.MarkRejected(ctx, tenant, id); rerr != nil {
			return errors.Join(ErrChecksumMismatch, rerr)
		}
		return ErrChecksumMismatch
	}
	return st.MarkCommitted(ctx, tenant, id, n)
}

// Source implements worker.Source: only committed (checksum-verified) objects of the job's own tenant are read,
// and the verified SHA-256 is the fingerprint that binds checkpoints to the exact content.
type Source struct {
	Store  Store
	Objs   Objects
	Bucket string // the configured bucket; rows pointing elsewhere are refused
}

func (s Source) Open(ctx context.Context, j jobs.Job, offset int64) (io.ReadCloser, string, error) {
	f, err := s.Store.Get(ctx, j.TenantID, j.FileObjectID)
	if err != nil {
		return nil, "", err
	}
	if !f.Committed || f.SHA256 == "" {
		return nil, "", ErrNotVerified
	}
	if f.Bucket != s.Bucket {
		return nil, "", fmt.Errorf("objects: row bucket %q is not the configured bucket", f.Bucket)
	}
	rc, err := s.Objs.Get(ctx, f.Key, offset)
	if err != nil {
		return nil, "", err
	}
	return rc, strings.ToLower(f.SHA256), nil
}

// OpenAt implements worker.RandomSource: the same verification as Open, then a ReaderAt that issues one ranged
// GET per read (the caller buffers; parquet reads in large chunks). The size is the verified size.
func (s Source) OpenAt(ctx context.Context, j jobs.Job) (io.ReaderAt, int64, string, error) {
	f, err := s.Store.Get(ctx, j.TenantID, j.FileObjectID)
	if err != nil {
		return nil, 0, "", err
	}
	if !f.Committed || f.SHA256 == "" {
		return nil, 0, "", ErrNotVerified
	}
	if f.Bucket != s.Bucket {
		return nil, 0, "", fmt.Errorf("objects: row bucket %q is not the configured bucket", f.Bucket)
	}
	size := f.Size
	if size < 0 {
		if size, err = s.Objs.Head(ctx, f.Key); err != nil {
			return nil, 0, "", err
		}
	}
	return RangeReaderAt{Ctx: ctx, Objs: s.Objs, Key: f.Key, Size: size}, size, strings.ToLower(f.SHA256), nil
}

// RangeReaderAt adapts Objects.Get(offset) to io.ReaderAt. Each call opens a ranged read, reads len(p) bytes and
// closes it, so it holds no connection between calls and is safe for concurrent use.
type RangeReaderAt struct {
	Ctx  context.Context
	Objs Objects
	Key  string
	Size int64
}

func (r RangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("objects: negative offset")
	}
	if off >= r.Size {
		return 0, io.EOF
	}
	want := p
	if rem := r.Size - off; int64(len(want)) > rem {
		want = p[:rem]
	}
	rc, err := r.Objs.Get(r.Ctx, r.Key, off)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	n, err := io.ReadFull(rc, want)
	if err == nil && len(want) < len(p) {
		err = io.EOF
	}
	return n, err
}

const (
	sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	sqlRegister  = `INSERT INTO file_objects (tenant_id, source_id, bucket, object_key, size_bytes, content_type, checksum_sha256, source_type, sensitivity, tags)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, NULLIF($6,''), $7, $8, $9, $10::jsonb) RETURNING id::text`
	sqlGet = `SELECT id::text, source_id::text, bucket, object_key, COALESCE(checksum_sha256,''), COALESCE(size_bytes,-1), committed_at IS NOT NULL, source_type, sensitivity, tags::text
		FROM file_objects WHERE id = $1::uuid AND tenant_id = $2::uuid AND deleted_at IS NULL`
	sqlCommit = `UPDATE file_objects SET size_bytes = $3, committed_at = now()
		WHERE id = $1::uuid AND tenant_id = $2::uuid AND committed_at IS NULL AND deleted_at IS NULL`
	sqlReject = `UPDATE file_objects SET deleted_at = now() WHERE id = $1::uuid AND tenant_id = $2::uuid AND committed_at IS NULL AND deleted_at IS NULL`
)

// PGStore is the PostgreSQL Store (file_objects, migration 0004) under tenant RLS.
type PGStore struct{ DB jobs.DB }

func (p PGStore) tx(ctx context.Context, tenant string, f func(jobs.Tx) error) error {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, sqlSetTenant, tenant); err != nil {
		return err
	}
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p PGStore) Register(ctx context.Context, tenant, source string, f File) (string, error) {
	var id string
	st, sens := f.SourceType, f.Sensitivity
	if st == "" {
		st = "unknown"
	}
	if sens == "" {
		sens = "internal"
	}
	tags, _ := json.Marshal(f.Tags)
	if f.Tags == nil {
		tags = []byte("{}")
	}
	err := p.tx(ctx, tenant, func(tx jobs.Tx) error {
		return tx.QueryRow(ctx, sqlRegister, tenant, source, f.Bucket, f.Key, f.Size, f.ContentType, f.SHA256, st, sens, string(tags)).Scan(&id)
	})
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "23503":
			return "", ErrUnknownSource
		case "23505":
			return "", ErrAlreadyExists
		}
	}
	return id, err
}

func (p PGStore) Get(ctx context.Context, tenant, id string) (File, error) {
	var f File
	var tags string
	err := p.tx(ctx, tenant, func(tx jobs.Tx) error {
		return tx.QueryRow(ctx, sqlGet, id, tenant).Scan(&f.ID, &f.SourceID, &f.Bucket, &f.Key, &f.SHA256, &f.Size, &f.Committed, &f.SourceType, &f.Sensitivity, &tags)
	})
	if err == nil {
		_ = json.Unmarshal([]byte(tags), &f.Tags)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return File{}, ErrNotFound
	}
	return f, err
}

func (p PGStore) exec1(ctx context.Context, tenant, sql string, args ...any) error {
	return p.tx(ctx, tenant, func(tx jobs.Tx) error {
		n, err := tx.Exec(ctx, sql, args...)
		if err == nil && n != 1 {
			err = ErrNotFound
		}
		return err
	})
}

func (p PGStore) MarkCommitted(ctx context.Context, tenant, id string, size int64) error {
	return p.exec1(ctx, tenant, sqlCommit, id, tenant, size)
}

func (p PGStore) MarkRejected(ctx context.Context, tenant, id string) error {
	return p.exec1(ctx, tenant, sqlReject, id, tenant)
}
