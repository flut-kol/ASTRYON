// Package joblist implements the tenant-scoped, keyset-paginated read-only job view behind GET /v1/jobs[/{id}].
// The tenant is always an argument from the validated Principal (RLS applies on top). Selected columns are run-level
// state and aggregate counters only: payload, idempotency key and lease owner are never read, and content labels are
// reduced to per-label counts.
package joblist

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

var (
	// ErrBadCursor means the cursor is not one this service issued.
	ErrBadCursor = errors.New("joblist: bad cursor")
	// ErrNotFound is returned for unknown ids and for another tenant's ids.
	ErrNotFound = errors.New("joblist: not found")

	codeRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
)

// Filter is the validated query. Zero values mean "no filter".
type Filter struct {
	Status, SourceID string
	Limit            int
	Cursor           string
}

// Job is one row.
type Job struct {
	ID, Kind, Status          string
	SourceID, FileObjectID    string // empty = none
	AttemptCount, MaxAttempts int
	EventsProcessed           int64
	EventsFailed              int64
	Error                     string
	Labels                    map[string]int64
	CreatedAt                 time.Time
	StartedAt, FinishedAt     *time.Time
}

// Page is one result page; NextCursor is empty on the last page.
type Page struct {
	Jobs       []Job
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

const cols = `id::text, kind, status::text, COALESCE(source_id::text,''), COALESCE(file_object_id::text,''), attempt_count, max_attempts,
	events_processed, events_failed, COALESCE(last_error,''), content_labels::text, created_at, started_at, finished_at`

func clamp(n int) int {
	if n <= 0 {
		return DefaultLimit
	}
	return min(n, MaxLimit)
}

// Build returns the list SQL and args (tenant is $1). Order is created_at DESC, id DESC, mirrored by the keyset condition.
func Build(tenant string, f Filter) (string, []any, error) {
	args := []any{tenant}
	arg := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }
	where := []string{"tenant_id = $1::uuid", "deleted_at IS NULL"}
	if f.Status != "" {
		where = append(where, "status = "+arg(f.Status)+"::job_status")
	}
	if f.SourceID != "" {
		where = append(where, "source_id = "+arg(f.SourceID)+"::uuid")
	}
	if f.Cursor != "" {
		c, err := decodeCursor(f.Cursor)
		if err != nil {
			return "", nil, err
		}
		where = append(where, fmt.Sprintf("(created_at, id) < (%s, %s::uuid)", arg(c.T), arg(c.ID)))
	}
	q := `SELECT ` + cols + ` FROM jobs WHERE ` + strings.Join(where, " AND ") +
		` ORDER BY created_at DESC, id DESC LIMIT ` + arg(clamp(f.Limit)+1)
	return q, args, nil
}

// PGStore runs reads inside a read-only, tenant-scoped transaction (astryon.tenant_id set, so RLS applies).
type PGStore struct{ Pool *pgxpool.Pool }

func (s PGStore) tx(ctx context.Context, tenant string, f func(pgx.Tx) error) error {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, tenant); err != nil {
		return err
	}
	return f(tx)
}

func scan(row pgx.Row) (Job, error) {
	var j Job
	var labels string
	if err := row.Scan(&j.ID, &j.Kind, &j.Status, &j.SourceID, &j.FileObjectID, &j.AttemptCount, &j.MaxAttempts,
		&j.EventsProcessed, &j.EventsFailed, &j.Error, &labels, &j.CreatedAt, &j.StartedAt, &j.FinishedAt); err != nil {
		return j, err
	}
	j.Labels, j.Error = Counts(labels), SafeError(j.Error)
	return j, nil
}

// Counts reduces stored content_labels ({label: {count, fields[]}}) to {label: count}; field names are dropped.
func Counts(raw string) map[string]int64 {
	out := map[string]int64{}
	var in map[string]struct {
		Count int64 `json:"count"`
	}
	if json.Unmarshal([]byte(raw), &in) == nil {
		for k, v := range in {
			out[k] = v.Count
		}
	}
	return out
}

// SafeError passes a plain error code through and hides anything else (free text could carry internals).
func SafeError(s string) string {
	if codeRe.MatchString(s) {
		return s
	}
	return ""
}

// List returns one page of the tenant's jobs.
func (s PGStore) List(ctx context.Context, tenant string, f Filter) (Page, error) {
	q, args, err := Build(tenant, f)
	if err != nil {
		return Page{}, err
	}
	limit := clamp(f.Limit)
	var p Page
	err = s.tx(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			j, err := scan(rows)
			if err != nil {
				return err
			}
			p.Jobs = append(p.Jobs, j)
		}
		return rows.Err()
	})
	if err != nil {
		return Page{}, err
	}
	if len(p.Jobs) > limit {
		p.Jobs = p.Jobs[:limit]
		last := p.Jobs[limit-1]
		p.NextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return p, nil
}

// Get returns one job of the tenant, or ErrNotFound.
func (s PGStore) Get(ctx context.Context, tenant, id string) (Job, error) {
	var j Job
	err := s.tx(ctx, tenant, func(tx pgx.Tx) (err error) {
		j, err = scan(tx.QueryRow(ctx, `SELECT `+cols+` FROM jobs WHERE tenant_id = $1::uuid AND id = $2::uuid AND deleted_at IS NULL`, tenant, id))
		return
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}
