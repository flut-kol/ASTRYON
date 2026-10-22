package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Filter narrows a List. TenantID is a separate argument of List and must come from validated claims.
type Filter struct {
	Category Category
	Outcome  Outcome
	Action   string // exact
	Subject  string // actor_aas_sub exact
	Since    time.Time
	Until    time.Time
	Limit    int // default 50, max 500
	// Cursor from a previous page (NextCursor); zero = first page.
	Cursor Cursor
}

// Cursor is a keyset position (occurred_at, id) for descending order.
type Cursor struct {
	OccurredAt time.Time
	ID         string
}

// Record is a stored audit row.
type Record struct {
	Event
}

const maxLimit = 500

// List returns tenant-scoped records newest first plus the cursor for the next page (zero when exhausted).
// The tenant is applied through RLS (SET LOCAL astryon.tenant_id) and an explicit predicate.
func List(ctx context.Context, db TxBeginner, tenantID string, f Filter) ([]Record, Cursor, error) {
	if !uuidRe.MatchString(tenantID) {
		return nil, Cursor{}, bad("tenant_id must be a UUID taken from token claims")
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	q := `SELECT id::text, tenant_id::text, occurred_at, category, outcome, action, entity_type,
 COALESCE(entity_id::text, resource_ref, ''), COALESCE(actor_aas_sub,''), COALESCE(actor_client_id,''),
 COALESCE(auth_acr,''), COALESCE(request_id,''), COALESCE(trace_id,''), details
 FROM audit_log WHERE tenant_id = $1`
	args := []any{tenantID}
	add := func(cond string, v any) {
		args = append(args, v)
		q += fmt.Sprintf(" AND "+cond, len(args))
	}
	if f.Category != "" {
		add("category = $%d", string(f.Category))
	}
	if f.Outcome != "" {
		add("outcome = $%d", string(f.Outcome))
	}
	if f.Action != "" {
		add("action = $%d", f.Action)
	}
	if f.Subject != "" {
		add("actor_aas_sub = $%d", f.Subject)
	}
	if !f.Since.IsZero() {
		add("occurred_at >= $%d", f.Since)
	}
	if !f.Until.IsZero() {
		add("occurred_at < $%d", f.Until)
	}
	if !f.Cursor.OccurredAt.IsZero() {
		args = append(args, f.Cursor.OccurredAt, f.Cursor.ID)
		q += fmt.Sprintf(" AND (occurred_at, id) < ($%d, $%d::uuid)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	q += fmt.Sprintf(" ORDER BY occurred_at DESC, id DESC LIMIT $%d", len(args))

	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, Cursor{}, err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, sqlSetTenant, tenantID); err != nil {
		return nil, Cursor{}, err
	}
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return nil, Cursor{}, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		var cat, outc, rid string
		var raw []byte
		if err := rows.Scan(&r.ID, &r.TenantID, &r.OccurredAt, &cat, &outc, &r.Action, &r.Resource.Type, &rid,
			&r.Subject, &r.ClientID, &r.ACR, &r.RequestID, &r.TraceID, &raw); err != nil {
			return nil, Cursor{}, err
		}
		r.Details = decodeDetails(raw)
		r.Category, r.Outcome, r.Resource.ID = Category(cat), Outcome(outc), rid
		r.OccurredAt = r.OccurredAt.UTC()
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, Cursor{}, err
	}
	var next Cursor
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = Cursor{OccurredAt: last.OccurredAt, ID: last.ID}
	}
	return out, next, nil
}

// decodeDetails tolerates legacy rows whose details hold non-string values.
func decodeDetails(raw []byte) map[string]string {
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil || len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		} else {
			b, _ := json.Marshal(v)
			out[k] = string(b)
		}
	}
	return out
}
