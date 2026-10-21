// Package listing implements the tenant-scoped, keyset-paginated upload list behind GET /v1/files/uploads.
// The tenant is always an argument from the validated Principal (and RLS applies on top); matched content values are
// never read, only the stored label names/counts.
package listing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DefaultLimit = 50
	MaxLimit     = 200
	MaxTagFilter = 5
)

// ErrBadCursor means the cursor is not one this service issued.
var ErrBadCursor = errors.New("listing: bad cursor")

// Tag is a tag filter; HasValue=false matches any value of Key.
type Tag struct {
	Key, Value string
	HasValue   bool
}

// Filter is the validated query. Zero values mean "no filter".
type Filter struct {
	SourceType, Sensitivity, Status string
	Label                           classify.Label
	Tags                            []Tag
	From, To                        time.Time // created_at >= From, < To
	Limit                           int
	Cursor                          string
}

// Item is one list row. Job fields are empty until the upload is completed.
type Item struct {
	FileObjectID, SourceID, SourceType, Sensitivity string
	Committed                                       bool
	SizeBytes                                       int64
	CreatedAt                                       time.Time
	Tags                                            map[string]string
	JobID, Status, Error                            string
	Labels                                          classify.Labels
}

// Page is one result page; NextCursor is empty on the last page.
type Page struct {
	Items      []Item
	NextCursor string
}

type cursor struct {
	T  time.Time `json:"t"`
	ID string    `json:"i"`
}

func encodeCursor(t time.Time, id string) string {
	b, _ := json.Marshal(cursor{t.UTC(), id})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (cursor, error) {
	var c cursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || json.Unmarshal(b, &c) != nil || len(c.ID) != 36 || c.T.IsZero() {
		return cursor{}, ErrBadCursor
	}
	return c, nil
}

// Build returns the SQL and args (tenant is $1). Order is created_at DESC, id DESC, which the keyset condition mirrors.
// Tag equality uses jsonb containment (served by the tags GIN jsonb_path_ops index); key-only filters use `?`.
func Build(tenant string, f Filter) (string, []any, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}
	if limit > MaxLimit {
		limit = MaxLimit
	}
	if len(f.Tags) > MaxTagFilter {
		return "", nil, fmt.Errorf("listing: more than %d tag filters", MaxTagFilter)
	}
	args := []any{tenant}
	arg := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }
	where := []string{"f.tenant_id = $1::uuid", "f.deleted_at IS NULL"}
	if f.SourceType != "" {
		where = append(where, "f.source_type = "+arg(f.SourceType))
	}
	if f.Sensitivity != "" {
		where = append(where, "f.sensitivity = "+arg(f.Sensitivity))
	}
	for _, t := range f.Tags {
		if t.HasValue {
			b, _ := json.Marshal(map[string]string{t.Key: t.Value})
			where = append(where, "f.tags @> "+arg(string(b))+"::jsonb")
		} else {
			where = append(where, "f.tags ? "+arg(t.Key))
		}
	}
	if f.Status != "" {
		where = append(where, "j.status = "+arg(f.Status)+"::job_status")
	}
	if f.Label != "" {
		where = append(where, "j.content_labels ? "+arg(string(f.Label)))
	}
	if !f.From.IsZero() {
		where = append(where, "f.created_at >= "+arg(f.From))
	}
	if !f.To.IsZero() {
		where = append(where, "f.created_at < "+arg(f.To))
	}
	if f.Cursor != "" {
		c, err := decodeCursor(f.Cursor)
		if err != nil {
			return "", nil, err
		}
		where = append(where, fmt.Sprintf("(f.created_at, f.id) < (%s, %s::uuid)", arg(c.T), arg(c.ID)))
	}
	q := `SELECT f.id::text, f.source_id::text, f.source_type, f.sensitivity, f.committed_at IS NOT NULL, COALESCE(f.size_bytes,0),
	f.created_at, f.tags::text, COALESCE(j.id::text,''), COALESCE(j.status::text,''), COALESCE(j.last_error,''), COALESCE(j.content_labels::text,'{}')
FROM file_objects f
LEFT JOIN LATERAL (SELECT id, status, last_error, content_labels FROM jobs
	WHERE tenant_id = f.tenant_id AND file_object_id = f.id AND kind = 'process_file' AND deleted_at IS NULL
	ORDER BY created_at DESC LIMIT 1) j ON true
WHERE ` + strings.Join(where, " AND ") + `
ORDER BY f.created_at DESC, f.id DESC LIMIT ` + arg(limit+1)
	return q, args, nil
}

// PGStore runs the list inside a tenant-scoped transaction (astryon.tenant_id set, so RLS applies).
type PGStore struct{ Pool *pgxpool.Pool }

func (s PGStore) List(ctx context.Context, tenant string, f Filter) (Page, error) {
	q, args, err := Build(tenant, f)
	if err != nil {
		return Page{}, err
	}
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}
	limit = min(limit, MaxLimit)
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return Page{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, tenant); err != nil {
		return Page{}, err
	}
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	var p Page
	for rows.Next() {
		var it Item
		var tags, labels string
		if err := rows.Scan(&it.FileObjectID, &it.SourceID, &it.SourceType, &it.Sensitivity, &it.Committed, &it.SizeBytes,
			&it.CreatedAt, &tags, &it.JobID, &it.Status, &it.Error, &labels); err != nil {
			return Page{}, err
		}
		it.Tags = map[string]string{}
		_ = json.Unmarshal([]byte(tags), &it.Tags)
		it.Labels = classify.Labels{}
		_ = json.Unmarshal([]byte(labels), &it.Labels)
		p.Items = append(p.Items, it)
	}
	if err := rows.Err(); err != nil {
		return Page{}, err
	}
	if len(p.Items) > limit {
		p.Items = p.Items[:limit]
		last := p.Items[limit-1]
		p.NextCursor = encodeCursor(last.CreatedAt, last.FileObjectID)
	}
	return p, nil
}
