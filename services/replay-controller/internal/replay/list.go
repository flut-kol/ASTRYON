package replay

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ScopeReplayRead is required for GET /v1/replays and GET /v1/replays/{id}.
const ScopeReplayRead = "astryon:replay:read"

// Info is the read model of one replay_requests row. It carries no payload and no actor ids.
type Info struct {
	ID                string     `json:"id"`
	SourceID          string     `json:"source_id"`
	ProcessingVersion int32      `json:"processing_version"`
	Status            string     `json:"status"`
	TimeFrom          *time.Time `json:"time_from,omitempty"`
	TimeTo            *time.Time `json:"time_to,omitempty"`
	JobID             string     `json:"job_id,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
}

// ListFilter: tenant comes from the principal only. Limit is 1..100.
type ListFilter struct {
	SourceID string
	Status   string
	Limit    int
	After    *Cursor
}

// Cursor is a keyset position (created_at DESC, id DESC).
type Cursor struct {
	T  time.Time `json:"t"`
	ID string    `json:"id"`
}

// Reader lists and fetches replay requests of one tenant.
type Reader interface {
	List(ctx context.Context, tenantID string, f ListFilter) (items []Info, next *Cursor, err error)
	Get(ctx context.Context, tenantID, id string) (Info, error)
}

var replayStatuses = map[string]bool{"REQUESTED": true, "APPROVED": true, "RUNNING": true, "COMPLETED": true, "FAILED": true, "CANCELLED": true}

const sqlInfoCols = `id::text, source_id::text, target_processing_version, status::text, time_from, time_to, COALESCE(job_id::text, ''), created_at, updated_at, completed_at`

const sqlList = `SELECT ` + sqlInfoCols + ` FROM replay_requests
WHERE tenant_id = $1::uuid AND deleted_at IS NULL
  AND ($2 = '' OR source_id = NULLIF($2, '')::uuid)
  AND ($3 = '' OR status = NULLIF($3, '')::replay_status)
  AND ($4::timestamptz IS NULL OR (created_at, id) < ($4::timestamptz, $5::uuid))
ORDER BY created_at DESC, id DESC LIMIT $6`

const sqlGet = `SELECT ` + sqlInfoCols + ` FROM replay_requests WHERE tenant_id = $1::uuid AND id = $2::uuid AND deleted_at IS NULL`

// PGReader reads under the RLS tenant setting (a role without BYPASSRLS is required, like the allocator).
type PGReader struct{ Pool *pgxpool.Pool }

func (r PGReader) tx(ctx context.Context, tenantID string) (pgx.Tx, error) {
	if !uuidRe.MatchString(tenantID) {
		return nil, ErrInvalid
	}
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, sqlSetTenant, tenantID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func scanInfo(row pgx.Row) (Info, error) {
	var i Info
	err := row.Scan(&i.ID, &i.SourceID, &i.ProcessingVersion, &i.Status, &i.TimeFrom, &i.TimeTo, &i.JobID, &i.CreatedAt, &i.UpdatedAt, &i.CompletedAt)
	return i, err
}

func (r PGReader) List(ctx context.Context, tenantID string, f ListFilter) ([]Info, *Cursor, error) {
	if f.Limit < 1 || f.Limit > 100 {
		f.Limit = 50
	}
	tx, err := r.tx(ctx, tenantID)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)
	var ct *time.Time
	cid := "00000000-0000-0000-0000-000000000000"
	if f.After != nil {
		ct, cid = &f.After.T, f.After.ID
	}
	rows, err := tx.Query(ctx, sqlList, tenantID, f.SourceID, f.Status, ct, cid, f.Limit+1)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var out []Info
	for rows.Next() {
		i, err := scanInfo(rows)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, i)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	var next *Cursor
	if len(out) > f.Limit {
		out = out[:f.Limit]
		last := out[len(out)-1]
		next = &Cursor{T: last.CreatedAt, ID: last.ID}
	}
	return out, next, nil
}

func (r PGReader) Get(ctx context.Context, tenantID, id string) (Info, error) {
	if !uuidRe.MatchString(id) {
		return Info{}, ErrNotFound
	}
	tx, err := r.tx(ctx, tenantID)
	if err != nil {
		return Info{}, err
	}
	defer tx.Rollback(ctx)
	i, err := scanInfo(tx.QueryRow(ctx, sqlGet, tenantID, id))
	if isNoRows(err) {
		return Info{}, ErrNotFound // another tenant's id looks the same
	}
	return i, err
}

func encodeCursor(c *Cursor) string {
	if c == nil {
		return ""
	}
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (*Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	var c Cursor
	if err != nil || json.Unmarshal(b, &c) != nil || !uuidRe.MatchString(c.ID) || c.T.IsZero() {
		return nil, ErrInvalid
	}
	return &c, nil
}

func (h *Handler) authorizeRead(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	p, err := h.Auth.Authenticate(r)
	if err != nil {
		h.auditDenied(r, ReasonUnauthenticated, "")
		writeErr(w, 401, "unauthenticated", "authentication required")
		return Principal{}, false
	}
	if !p.HasScope(ScopeReplayRead) {
		h.auditDeniedPrincipal(r, ReasonMissingScope, p)
		writeErr(w, 403, "forbidden", "missing scope "+ScopeReplayRead)
		return Principal{}, false
	}
	return p, true
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	p, ok := h.authorizeRead(w, r)
	if !ok {
		return
	}
	if h.Reader == nil {
		writeErr(w, 501, "not_configured", "listing needs REPLAY_PG_URL")
		return
	}
	q := r.URL.Query()
	f := ListFilter{SourceID: q.Get("source_id"), Status: q.Get("status"), Limit: 50}
	if f.SourceID != "" && !uuidRe.MatchString(f.SourceID) {
		writeErr(w, 400, "invalid_request", "source_id must be a UUID")
		return
	}
	if f.Status != "" && !replayStatuses[f.Status] {
		writeErr(w, 400, "invalid_request", "unknown status")
		return
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeErr(w, 400, "invalid_request", "limit must be 1..100")
			return
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		c, err := decodeCursor(v)
		if err != nil {
			writeErr(w, 400, "invalid_request", "invalid cursor")
			return
		}
		f.After = c
	}
	items, next, err := h.Reader.List(r.Context(), p.TenantID, f)
	if err != nil {
		writeErr(w, 503, "unavailable", "replay store unavailable")
		return
	}
	if items == nil {
		items = []Info{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "next_cursor": encodeCursor(next)})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	p, ok := h.authorizeRead(w, r)
	if !ok {
		return
	}
	if h.Reader == nil {
		writeErr(w, 501, "not_configured", "listing needs REPLAY_PG_URL")
		return
	}
	i, err := h.Reader.Get(r.Context(), p.TenantID, r.PathValue("id"))
	switch {
	case errors.Is(err, ErrNotFound):
		writeErr(w, 404, "not_found", "replay not found")
		return
	case err != nil:
		writeErr(w, 503, "unavailable", "replay store unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(i)
}
