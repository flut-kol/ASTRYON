// Package adminapi is the connector admin REST API (list/get/create/pause/resume). The tenant comes only from the
// authenticated Principal; request bodies cannot carry it (unknown fields are rejected). Responses expose SecretRef
// names, never secret values.
package adminapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/astryon/connector-manager/internal/registry"
)

// SourcesReadScope gates the read-only source endpoints.
const SourcesReadScope = "astryon:sources:read"

const (
	MaxBodyBytes = 64 << 10
	maxConfig    = 32
	defaultLimit = 50
	maxLimit     = 200
)

var (
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrNotFound        = errors.New("connector not found")
	ErrConflict        = errors.New("connector name already exists for source")
	ErrBadReference    = errors.New("source not found")

	uuidRE      = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	secretKeyRE = regexp.MustCompile(`(?i)(secret|password|passwd|token|apikey|api_key|credential|private)`)
)

// Connector is the API view. SecretRef is a reference (e.g. env://NAME), never a value.
type Connector struct {
	ID        string         `json:"id"`
	SourceID  string         `json:"source_id"`
	Name      string         `json:"name"`
	Kind      string         `json:"kind"`
	Enabled   bool           `json:"enabled"`
	SecretRef string         `json:"secret_ref,omitempty"`
	Config    map[string]any `json:"config"`
	CreatedAt time.Time      `json:"created_at"`
}

type CreateInput struct {
	SourceID  string            `json:"source_id"`
	Name      string            `json:"name"`
	Kind      string            `json:"kind"`
	SecretRef string            `json:"secret_ref"`
	Config    map[string]string `json:"config"`
}

// Store persists connectors; every call is scoped to the trusted tenant.
type Store interface {
	List(ctx context.Context, tenant, after string, limit int) ([]Connector, error)
	Get(ctx context.Context, tenant, id string) (Connector, error)
	Create(ctx context.Context, tenant string, in CreateInput) (Connector, error)
	SetEnabled(ctx context.Context, tenant, id string, enabled bool) (Connector, error)
}

// Source is the read-only API view of a source. Config and credential references are deliberately not exposed.
type Source struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Kind           string    `json:"kind"`
	Enabled        bool      `json:"enabled"`
	ConnectorCount int       `json:"connector_count"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// SourceStore is the read-only source view; every call is scoped to the trusted tenant. ErrNotFound for unknown ids.
type SourceStore interface {
	ListSources(ctx context.Context, tenant, after string, limit int) ([]Source, error)
	GetSource(ctx context.Context, tenant, id string) (Source, error)
}

type Authenticator interface {
	Authenticate(*http.Request) (registry.Principal, error)
}

type Server struct {
	Auth  Authenticator
	Store Store
	// Sources serves GET /v1/sources[/{id}] (nil = 503); needs SourcesReadScope.
	Sources SourceStore
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/connectors", s.guard(registry.ManageScope, s.list))
	mux.HandleFunc("POST /v1/connectors", s.guard(registry.ManageScope, s.create))
	mux.HandleFunc("GET /v1/connectors/{id}", s.guard(registry.ManageScope, s.get))
	mux.HandleFunc("POST /v1/connectors/{id}/pause", s.guard(registry.ManageScope, s.toggle(false)))
	mux.HandleFunc("POST /v1/connectors/{id}/resume", s.guard(registry.ManageScope, s.toggle(true)))
	mux.HandleFunc("GET /v1/sources", s.guard(SourcesReadScope, s.listSources))
	mux.HandleFunc("GET /v1/sources/{id}", s.guard(SourcesReadScope, s.getSource))
	return mux
}

type handler func(w http.ResponseWriter, r *http.Request, tenant string)

func (s *Server) guard(scope string, h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		p, err := s.Auth.Authenticate(r)
		if err != nil || p.TenantID == "" {
			writeErr(w, r, http.StatusUnauthorized, "unauthenticated", "authentication required")
			return
		}
		if !p.Scopes[scope] {
			writeErr(w, r, http.StatusForbidden, "forbidden", "missing scope "+scope)
			return
		}
		h(w, r, p.TenantID)
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request, tenant string) {
	limit := defaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			writeErr(w, r, http.StatusBadRequest, "invalid_request", "limit must be 1.."+strconv.Itoa(maxLimit))
			return
		}
		limit = n
	}
	after := r.URL.Query().Get("cursor")
	if after != "" && !uuidRE.MatchString(after) {
		writeErr(w, r, http.StatusBadRequest, "invalid_request", "invalid cursor")
		return
	}
	items, err := s.Store.List(r.Context(), tenant, after, limit+1)
	if err != nil {
		storeErr(w, r, err)
		return
	}
	next := ""
	if len(items) > limit {
		items = items[:limit]
		next = items[limit-1].ID
	}
	if items == nil {
		items = []Connector{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request, tenant string) {
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		writeErr(w, r, http.StatusBadRequest, "invalid_request", "invalid connector id")
		return
	}
	c, err := s.Store.Get(r.Context(), tenant, id)
	if err != nil {
		storeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Server) toggle(enabled bool) handler {
	return func(w http.ResponseWriter, r *http.Request, tenant string) {
		id := r.PathValue("id")
		if !uuidRE.MatchString(id) {
			writeErr(w, r, http.StatusBadRequest, "invalid_request", "invalid connector id")
			return
		}
		c, err := s.Store.SetEnabled(r.Context(), tenant, id, enabled)
		if err != nil {
			storeErr(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, c)
	}
}

func (s *Server) create(w http.ResponseWriter, r *http.Request, tenant string) {
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		writeErr(w, r, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	dec.DisallowUnknownFields()
	var in CreateInput
	if err := dec.Decode(&in); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErr(w, r, http.StatusRequestEntityTooLarge, "payload_too_large", "request body too large")
			return
		}
		writeErr(w, r, http.StatusBadRequest, "invalid_request", "malformed JSON body")
		return
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		writeErr(w, r, http.StatusBadRequest, "invalid_request", "trailing data after JSON body")
		return
	}
	if msg := validate(in, tenant); msg != "" {
		writeErr(w, r, http.StatusUnprocessableEntity, "invalid_connector", msg)
		return
	}
	c, err := s.Store.Create(r.Context(), tenant, in)
	if err != nil {
		storeErr(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/connectors/"+c.ID)
	writeJSON(w, http.StatusCreated, c)
}

func validate(in CreateInput, tenant string) string {
	if !uuidRE.MatchString(in.SourceID) {
		return "source_id must be a UUID"
	}
	if len(in.Name) > 128 || len(in.SecretRef) > 512 {
		return "name or secret_ref too long"
	}
	if len(in.Config) > maxConfig {
		return "too many config entries"
	}
	cfg := in.Config
	if cfg == nil {
		cfg = map[string]string{}
	}
	for k, v := range cfg {
		if len(k) == 0 || len(k) > 64 || len(v) > 1024 {
			return "config key or value size out of range"
		}
		if secretKeyRE.MatchString(k) {
			return "config must not hold secrets; use secret_ref (key " + k + ")"
		}
	}
	// Reuse the registry rules; the tenant is the trusted one, so only the connector's own fields can fail.
	c := registry.Connector{ID: "new", TenantID: tenant, Name: in.Name, SecretRef: in.SecretRef, Kind: registry.Kind(in.Kind), Config: cfg}
	if c.Validate(tenant) != nil {
		return "name, kind (POSTGRES_CDC|MYSQL_CDC|REST_PULL) and secret_ref (scheme://name) are required"
	}
	return ""
}

func storeErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeErr(w, r, http.StatusNotFound, "not_found", "connector not found")
	case errors.Is(err, ErrConflict):
		writeErr(w, r, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, ErrBadReference):
		writeErr(w, r, http.StatusUnprocessableEntity, "invalid_connector", err.Error())
	default:
		writeErr(w, r, http.StatusInternalServerError, "internal", "internal error")
	}
}

type errBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
	TraceID string         `json:"trace_id"`
}

func writeErr(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	writeJSON(w, status, errBody{Code: code, Message: msg, TraceID: traceID(w, r)})
}

func traceID(w http.ResponseWriter, r *http.Request) string {
	id := r.Header.Get("X-Request-ID")
	if id == "" || len(id) > 64 || strings.ContainsAny(id, "\r\n") {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		id = hex.EncodeToString(b)
	}
	w.Header().Set("X-Request-ID", id)
	return id
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
