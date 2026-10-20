package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	maxBodyBytes   = 64 << 10
	queryTimeout   = 25 * time.Second
	cursorTTL      = 15 * time.Minute
	defaultSummary = 24 * time.Hour
)

type Server struct {
	auth    Authenticator
	an      Analytics
	cp      ControlPlane
	cursors *Cursors
	rl      *RateLimiter
	log     *slog.Logger
}

func NewServer(cfg Config, auth Authenticator, an Analytics, cp ControlPlane, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{auth: auth, an: an, cp: cp, cursors: NewCursors(cfg.CursorKey, cursorTTL),
		rl: NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst), log: log}
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
	TraceID string `json:"trace_id"`
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /v1/query/events", s.handleEvents)
	mux.HandleFunc("POST /v1/query/aggregate", s.handleAggregate)
	mux.HandleFunc("GET /v1/metrics/summary", s.handleMetrics)
	mux.HandleFunc("GET /v1/quality/summary", s.handleQuality)
	mux.HandleFunc("GET /v1/sources", s.handleSources)
	mux.HandleFunc("GET /v1/jobs", s.handleJobs)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 || !utf8.ValidString(id) || hasCtl(id) {
			id = newUUID()
		}
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), reqIDKey, id)))
	})
}

func hasCtl(s string) bool {
	for _, c := range s {
		if c < 0x20 || c == 0x7f {
			return true
		}
	}
	return false
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, r *http.Request, status int, code, msg string, details any) {
	writeJSON(w, status, apiError{code, msg, details, requestID(r.Context())})
}

// prelude: authenticate, authorize scope, rate limit. The returned tenant is
// the one from the validated token and is the only tenant ever queried.
func (s *Server) prelude(w http.ResponseWriter, r *http.Request, scope string) (string, bool) {
	p, err := s.auth.Authenticate(r)
	if err != nil {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeErr(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required", nil)
		return "", false
	}
	if !isUUID(p.TenantID) || !p.HasScope(scope) {
		writeErr(w, r, http.StatusForbidden, "FORBIDDEN", "missing required scope "+scope, nil)
		return "", false
	}
	if ok, ra := s.rl.Allow(p.TenantID); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(ra))
		writeErr(w, r, http.StatusTooManyRequests, "RATE_LIMITED", "too many requests", map[string]int{"retry_after_seconds": ra})
		return "", false
	}
	return p.TenantID, true
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, r, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", fmt.Sprintf("body must be at most %d bytes", maxBodyBytes), nil)
		return false
	}
	dec := json.NewDecoder(bytesReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil || dec.More() {
		writeErr(w, r, http.StatusBadRequest, "BODY_INVALID", "body must be a single valid JSON object with known fields only", nil)
		return false
	}
	return true
}

func (s *Server) upstream(w http.ResponseWriter, r *http.Request, what string, err error) {
	s.log.Error("upstream failure", "what", what, "request_id", requestID(r.Context()), "err", err)
	w.Header().Set("Retry-After", "2")
	writeErr(w, r, http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", what+" is temporarily unavailable", nil)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.prelude(w, r, ScopeEventsRead)
	if !ok {
		return
	}
	var req EventsRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if err := req.Normalize(); err != nil {
		writeErr(w, r, http.StatusBadRequest, "REQUEST_INVALID", err.Error(), nil)
		return
	}
	hash := req.queryHash()
	var after *Keyset
	if req.Cursor != "" {
		t, id, err := s.cursors.Decode(req.Cursor, "events", tenant, hash)
		if err != nil || !isUUID(id) {
			writeErr(w, r, http.StatusBadRequest, "CURSOR_INVALID", "cursor is invalid or expired", nil)
			return
		}
		after = &Keyset{Time: t, ID: id}
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()
	items, err := s.an.QueryEvents(ctx, tenant, req, after)
	if err != nil {
		s.upstream(w, r, "event store", err)
		return
	}
	resp := struct {
		Items      []Event `json:"items"`
		NextCursor *string `json:"next_cursor"`
	}{Items: items}
	if len(items) > req.Limit {
		items = items[:req.Limit]
		resp.Items = items
		last := items[len(items)-1]
		c := s.cursors.Encode("events", tenant, hash, last.EventTime, last.EventID)
		resp.NextCursor = &c
	}
	if resp.Items == nil {
		resp.Items = []Event{}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAggregate(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.prelude(w, r, ScopeEventsRead)
	if !ok {
		return
	}
	var req AggregateRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if err := req.Normalize(); err != nil {
		writeErr(w, r, http.StatusBadRequest, "REQUEST_INVALID", err.Error(), nil)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()
	res, err := s.an.Aggregate(ctx, tenant, req)
	if err != nil {
		s.upstream(w, r, "event store", err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// parseRange reads optional from/to (RFC 3339); default is the last 24h.
func parseRange(r *http.Request) (TimeRange, error) {
	tr := TimeRange{To: time.Now().UTC().Truncate(time.Second)}
	tr.From = tr.To.Add(-defaultSummary)
	var err error
	if v := r.URL.Query().Get("to"); v != "" {
		if tr.To, err = time.Parse(time.RFC3339, v); err != nil {
			return tr, fmt.Errorf("to must be RFC 3339")
		}
		tr.From = tr.To.Add(-defaultSummary)
	}
	if v := r.URL.Query().Get("from"); v != "" {
		if tr.From, err = time.Parse(time.RFC3339, v); err != nil {
			return tr, fmt.Errorf("from must be RFC 3339")
		}
	}
	tr.From, tr.To = tr.From.UTC(), tr.To.UTC()
	return tr, tr.validate()
}

func (s *Server) summaryParams(w http.ResponseWriter, r *http.Request, scope string) (tenant string, tr TimeRange, src string, ok bool) {
	if tenant, ok = s.prelude(w, r, scope); !ok {
		return
	}
	var err error
	if tr, err = parseRange(r); err != nil {
		writeErr(w, r, http.StatusBadRequest, "REQUEST_INVALID", err.Error(), nil)
		return "", tr, "", false
	}
	if src = r.URL.Query().Get("source_id"); src != "" && !isUUID(src) {
		writeErr(w, r, http.StatusBadRequest, "REQUEST_INVALID", "source_id must be a UUID", nil)
		return "", tr, "", false
	}
	return tenant, tr, src, true
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	tenant, tr, src, ok := s.summaryParams(w, r, ScopeMetricsRead)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()
	res, err := s.an.MetricsSummary(ctx, tenant, tr, src)
	if err != nil {
		s.upstream(w, r, "metrics store", err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleQuality(w http.ResponseWriter, r *http.Request) {
	tenant, tr, src, ok := s.summaryParams(w, r, ScopeMetricsRead)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()
	res, err := s.an.QualitySummary(ctx, tenant, tr, src)
	if err != nil {
		s.upstream(w, r, "metrics store", err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func pageLimit(r *http.Request) (int, error) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return DefaultLimit, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 200 {
		return 0, errors.New("limit must be between 1 and 200")
	}
	return n, nil
}

func (s *Server) handleSources(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.prelude(w, r, ScopeSourcesRead)
	if !ok {
		return
	}
	limit, err := pageLimit(r)
	if err != nil {
		writeErr(w, r, http.StatusBadRequest, "REQUEST_INVALID", err.Error(), nil)
		return
	}
	hash := hashOf("sources")
	var after *Keyset
	if c := r.URL.Query().Get("cursor"); c != "" {
		t, id, err := s.cursors.Decode(c, "sources", tenant, hash)
		if err != nil || !isUUID(id) {
			writeErr(w, r, http.StatusBadRequest, "CURSOR_INVALID", "cursor is invalid or expired", nil)
			return
		}
		after = &Keyset{Time: t, ID: id}
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()
	items, err := s.cp.ListSources(ctx, tenant, ListPage{Limit: limit + 1, After: after})
	if err != nil {
		s.upstream(w, r, "control plane", err)
		return
	}
	resp := struct {
		Items      []Source `json:"items"`
		NextCursor *string  `json:"next_cursor"`
	}{Items: items}
	if len(items) > limit {
		items = items[:limit]
		resp.Items = items
		c := s.cursors.Encode("sources", tenant, hash, items[limit-1].CreatedAt, items[limit-1].ID)
		resp.NextCursor = &c
	}
	if resp.Items == nil {
		resp.Items = []Source{}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.prelude(w, r, ScopeJobsRead)
	if !ok {
		return
	}
	limit, err := pageLimit(r)
	if err != nil {
		writeErr(w, r, http.StatusBadRequest, "REQUEST_INVALID", err.Error(), nil)
		return
	}
	f := JobFilter{Status: r.URL.Query().Get("status"), SourceID: r.URL.Query().Get("source_id")}
	if f.Status != "" && !JobStatuses[f.Status] {
		writeErr(w, r, http.StatusBadRequest, "REQUEST_INVALID", "unknown status", nil)
		return
	}
	if f.SourceID != "" && !isUUID(f.SourceID) {
		writeErr(w, r, http.StatusBadRequest, "REQUEST_INVALID", "source_id must be a UUID", nil)
		return
	}
	hash := hashOf(f)
	var after *Keyset
	if c := r.URL.Query().Get("cursor"); c != "" {
		t, id, err := s.cursors.Decode(c, "jobs", tenant, hash)
		if err != nil || !isUUID(id) {
			writeErr(w, r, http.StatusBadRequest, "CURSOR_INVALID", "cursor is invalid or expired", nil)
			return
		}
		after = &Keyset{Time: t, ID: id}
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()
	items, err := s.cp.ListJobs(ctx, tenant, f, ListPage{Limit: limit + 1, After: after})
	if err != nil {
		s.upstream(w, r, "control plane", err)
		return
	}
	resp := struct {
		Items      []Job   `json:"items"`
		NextCursor *string `json:"next_cursor"`
	}{Items: items}
	if len(items) > limit {
		items = items[:limit]
		resp.Items = items
		c := s.cursors.Encode("jobs", tenant, hash, items[limit-1].CreatedAt, items[limit-1].ID)
		resp.NextCursor = &c
	}
	if resp.Items == nil {
		resp.Items = []Job{}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	checks := map[string]string{"clickhouse": "ok", "postgres": "ok"}
	status := http.StatusOK
	if err := s.an.Ping(ctx); err != nil {
		checks["clickhouse"] = "down"
		status = http.StatusServiceUnavailable
	}
	if err := s.cp.Ping(ctx); err != nil {
		checks["postgres"] = "down"
		status = http.StatusServiceUnavailable
	}
	st := "ok"
	if status != http.StatusOK {
		st = "degraded"
	}
	writeJSON(w, status, map[string]any{"status": st, "checks": checks})
}
