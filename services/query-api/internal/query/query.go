// Package query implements the read API (DPP-029): tenant-filtered event search with keyset (cursor)
// pagination and whitelisted aggregations over ClickHouse. No caller-supplied SQL; every value is bound
// through ClickHouse query parameters; tenant_id comes only from the authenticated Principal.
package query

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultLimit = 100
	MaxLimit     = 1000
	MaxRange     = 31 * 24 * time.Hour
	maxBody      = 1 << 20
)

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Server serves /v1/query/*.
type Server struct {
	Auth                              Authenticator
	CHURL, CHDatabase, CHUser, CHPass string
	Table                             string // default events_normalized_distributed
	HTTP                              *http.Client
	Limiter                           *Limiter        // per-tenant rate/concurrency protection; nil disables (tests only)
	Caps                              Caps            // ClickHouse hard caps; zero fields use DefaultCaps
	Versions                          VersionResolver // active processing_version per source; nil = processing_version required
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/query/events", s.wrap(s.events))
	mux.HandleFunc("POST /v1/query/aggregate", s.wrap(s.aggregate))
	s.summaryHandlers(mux)
	return mux
}

type apiError struct {
	status  int
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *apiError) Error() string { return e.Code + ": " + e.Message }

func bad(code, msg string) *apiError { return &apiError{status: 400, Code: code, Message: msg} }

type handler func(ctx context.Context, p Principal, body []byte) (any, error)

func (s *Server) wrap(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { s.serve(w, r, h) }
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request, h handler) {
	rid := r.Header.Get("X-Request-ID")
	if rid == "" || len(rid) > 64 {
		rid = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	w.Header().Set("X-Request-ID", rid)
	fail := func(e *apiError) {
		Stats.record(e.status)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(e.status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
			"code": e.Code, "message": e.Message, "details": e.Details, "trace_id": rid}})
	}
	p, err := s.Auth.Authenticate(r)
	if err != nil {
		fail(&apiError{status: 401, Code: "unauthenticated", Message: "invalid or missing credentials"})
		return
	}
	if !p.HasScope(ScopeQueryRead) {
		fail(&apiError{status: 403, Code: "forbidden", Message: "missing scope " + ScopeQueryRead})
		return
	}
	if !uuidRe.MatchString(p.TenantID) {
		fail(&apiError{status: 403, Code: "forbidden", Message: "principal has no valid tenant"})
		return
	}
	if s.Limiter != nil {
		release, retryAfter, lerr := s.Limiter.AcquireCtx(r.Context(), p.TenantID)
		if lerr != nil {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			fail(lerr)
			return
		}
		defer release()
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		fail(&apiError{status: 413, Code: "body_too_large", Message: "request body too large"})
		return
	}
	out, err := h(r.Context(), p, body)
	if err != nil {
		var ae *apiError
		if errors.As(err, &ae) {
			fail(ae)
		} else {
			log.Printf("request_id=%q backend error: %v", rid, err) // #nosec G706 -- logged value is operator config or sanitized, reviewed 2026-10-21
			fail(&apiError{status: 502, Code: "backend_error", Message: "query backend failed"})
		}
		return
	}
	Stats.record(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func decode(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return bad("invalid_request", "malformed JSON: "+err.Error())
	}
	return nil
}

// Filter is the common structured filter.
type Filter struct {
	From              time.Time `json:"from"`
	To                time.Time `json:"to"`
	ProcessingVersion *uint32   `json:"processing_version"` // explicit wins; omitted -> active version of source_id (VersionResolver), else 400
	SourceID          string    `json:"source_id"`
	EventType         string    `json:"event_type"`
	EntityID          string    `json:"entity_id"`
}

type where struct {
	sql    []string
	params url.Values
}

func (w *where) add(cond, name, val string) {
	w.sql = append(w.sql, cond)
	w.params.Set("param_"+name, val)
}

const chTime = "2006-01-02 15:04:05.000"

func (f Filter) validateRange() error {
	if f.From.IsZero() || f.To.IsZero() || !f.To.After(f.From) {
		return bad("invalid_range", "from and to are required and to must be after from")
	}
	if f.To.Sub(f.From) > MaxRange {
		return bad("range_too_large", "time range exceeds 31 days")
	}
	return nil
}

func (f Filter) build(p Principal) (*where, error) {
	if err := f.validateRange(); err != nil {
		return nil, err
	}
	if f.ProcessingVersion == nil { // callers resolve it first (resolveVersion)
		return nil, bad("invalid_request", "processing_version is required")
	}
	if f.SourceID != "" && !uuidRe.MatchString(f.SourceID) {
		return nil, bad("invalid_request", "source_id must be a UUID")
	}
	if len(f.EventType) > 256 || len(f.EntityID) > 512 {
		return nil, bad("invalid_request", "filter value too long")
	}
	w := &where{params: url.Values{}}
	w.add("tenant_id = {p_tenant:UUID}", "p_tenant", p.TenantID)
	w.add("event_time >= {p_from:DateTime64(3, 'UTC')}", "p_from", f.From.UTC().Format(chTime))
	w.add("event_time < {p_to:DateTime64(3, 'UTC')}", "p_to", f.To.UTC().Format(chTime))
	w.add("processing_version = {p_pv:UInt32}", "p_pv", fmt.Sprint(*f.ProcessingVersion))
	if f.SourceID != "" {
		w.add("source_id = {p_source:UUID}", "p_source", f.SourceID)
	}
	if f.EventType != "" {
		w.add("event_type = {p_type:String}", "p_type", f.EventType)
	}
	if f.EntityID != "" {
		w.add("entity_id = {p_entity:String}", "p_entity", f.EntityID)
	}
	return w, nil
}

// EventsRequest is the body of POST /v1/query/events.
type EventsRequest struct {
	Filter
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor"`
}

type cursor struct {
	T string `json:"t"` // event_time, chTime
	E string `json:"e"` // event_id
}

func encodeCursor(c cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (cursor, error) {
	var c cursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || json.Unmarshal(b, &c) != nil || !uuidRe.MatchString(c.E) {
		return c, bad("invalid_cursor", "cursor is malformed")
	}
	if _, err := time.Parse(chTime, c.T); err != nil {
		return c, bad("invalid_cursor", "cursor is malformed")
	}
	return c, nil
}

func (s *Server) table() string {
	if s.Table != "" {
		return s.Table
	}
	return "events_normalized_distributed"
}

func (s *Server) events(ctx context.Context, p Principal, body []byte) (any, error) {
	var req EventsRequest
	if err := decode(body, &req); err != nil {
		return nil, err
	}
	if req.Limit == 0 {
		req.Limit = DefaultLimit
	}
	if req.Limit < 0 || req.Limit > MaxLimit {
		return nil, bad("invalid_limit", fmt.Sprintf("limit must be 1..%d", MaxLimit))
	}
	if err := s.resolveVersion(ctx, p, &req.Filter); err != nil {
		return nil, err
	}
	w, err := req.Filter.build(p)
	if err != nil {
		return nil, err
	}
	if req.Cursor != "" {
		c, err := decodeCursor(req.Cursor)
		if err != nil {
			return nil, err
		}
		w.add("(event_time, event_id) > ({p_ct:DateTime64(3, 'UTC')}, {p_ce:UUID})", "p_ct", c.T)
		w.params.Set("param_p_ce", c.E)
	}
	q := fmt.Sprintf(`SELECT event_id, source_id, batch_id, processing_version, event_type, event_time, entity_id, attributes, payload, quality_flags
FROM %s WHERE %s ORDER BY event_time, event_id
LIMIT 1 BY tenant_id, source_id, event_id, processing_version
LIMIT %d FORMAT JSONEachRow`, s.table(), strings.Join(w.sql, " AND "), req.Limit+1)
	rows, err := s.run(ctx, q, w.params)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(rows) > req.Limit {
		rows = rows[:req.Limit]
		last := rows[len(rows)-1]
		t, _ := last["event_time"].(string)
		id, _ := last["event_id"].(string)
		if len(t) >= 23 {
			next = encodeCursor(cursor{T: t[:23], E: id})
		}
	}
	return map[string]any{"items": rows, "next_cursor": next}, nil
}

// aggregates / dimensions are whitelists: nothing else reaches the SQL text.
var aggregates = map[string]string{
	"count":             "count()",
	"bytes":             "sum(length(payload))",
	"distinct_entities": "uniqExact(entity_id)",
	"distinct_batches":  "uniqExact(batch_id)",
}

var dimensions = map[string]string{
	"source_id":  "toString(source_id)",
	"event_type": "event_type",
	"hour":       "toStartOfHour(event_time, 'UTC')",
	"day":        "toDate(event_time, 'UTC')",
}

// AggregateRequest is the body of POST /v1/query/aggregate.
type AggregateRequest struct {
	Filter
	Metrics []string `json:"metrics"`
	GroupBy []string `json:"group_by"`
	Limit   int      `json:"limit"`
}

func (s *Server) aggregate(ctx context.Context, p Principal, body []byte) (any, error) {
	var req AggregateRequest
	if err := decode(body, &req); err != nil {
		return nil, err
	}
	if len(req.Metrics) == 0 || len(req.Metrics) > len(aggregates) || len(req.GroupBy) > 3 {
		return nil, bad("invalid_request", "1..4 metrics and at most 3 group_by fields are allowed")
	}
	if req.Limit == 0 {
		req.Limit = DefaultLimit
	}
	if req.Limit < 0 || req.Limit > MaxLimit {
		return nil, bad("invalid_limit", fmt.Sprintf("limit must be 1..%d", MaxLimit))
	}
	if err := s.resolveVersion(ctx, p, &req.Filter); err != nil {
		return nil, err
	}
	w, err := req.Filter.build(p)
	if err != nil {
		return nil, err
	}
	var sel, grp []string
	seen := map[string]bool{}
	for _, d := range req.GroupBy {
		expr, ok := dimensions[d]
		if !ok || seen[d] {
			return nil, &apiError{status: 400, Code: "invalid_group_by", Message: "unsupported group_by: " + d, Details: keys(dimensions)}
		}
		seen[d] = true
		sel = append(sel, expr+" AS "+d)
		grp = append(grp, d)
	}
	for _, m := range req.Metrics {
		expr, ok := aggregates[m]
		if !ok || seen[m] {
			return nil, &apiError{status: 400, Code: "invalid_metric", Message: "unsupported metric: " + m, Details: keys(aggregates)}
		}
		seen[m] = true
		sel = append(sel, expr+" AS "+m)
	}
	// Dedup first, then aggregate, so retried blocks are never double counted (see 0001 dedup strategy).
	q := fmt.Sprintf(`SELECT %s FROM (SELECT * FROM %s WHERE %s LIMIT 1 BY tenant_id, source_id, event_id, processing_version)`,
		strings.Join(sel, ", "), s.table(), strings.Join(w.sql, " AND "))
	if len(grp) > 0 {
		q += " GROUP BY " + strings.Join(grp, ", ") + " ORDER BY " + strings.Join(grp, ", ")
	}
	q += fmt.Sprintf(" LIMIT %d FORMAT JSONEachRow", req.Limit)
	rows, err := s.run(ctx, q, w.params)
	if err != nil {
		return nil, err
	}
	return map[string]any{"rows": rows}, nil
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// run executes q with bound parameters and hard server-side limits, decoding JSONEachRow.
func (s *Server) run(ctx context.Context, q string, params url.Values) ([]map[string]any, error) {
	v := url.Values{}
	for k, vs := range params {
		v[k] = vs
	}
	if s.CHDatabase != "" {
		v.Set("database", s.CHDatabase)
	}
	v.Set("readonly", "1")
	caps := s.Caps.withDefaults()
	v.Set("max_execution_time", strconv.Itoa(caps.MaxExecutionSeconds))
	v.Set("max_result_rows", strconv.Itoa(caps.MaxResultRows))
	v.Set("result_overflow_mode", "break")
	v.Set("max_memory_usage", strconv.FormatInt(caps.MaxMemoryBytes, 10))
	c := s.HTTP
	if c == nil {
		c = &http.Client{Timeout: time.Duration(caps.MaxExecutionSeconds)*time.Second + 15*time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.CHURL, "/")+"/?"+v.Encode(), strings.NewReader(q))
	if err != nil {
		return nil, err
	}
	if s.CHUser != "" {
		req.Header.Set("X-ClickHouse-User", s.CHUser)
		req.Header.Set("X-ClickHouse-Key", s.CHPass)
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("clickhouse %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	rows := []map[string]any{}
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			return nil, err
		}
		rows = append(rows, m)
	}
	return rows, nil
}
