// Package httpx provides stdlib-only HTTP helpers: request-ID, panic
// recovery, access log, body limit, JSON errors and an authn adapter.
// The error shape and request-ID rules mirror services/gateway.
package httpx

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/astryon/go-common/pkg/authn"
	"github.com/astryon/go-common/pkg/logging"
)

// RequestIDHeader is the request correlation header.
const RequestIDHeader = "X-Request-ID"

type ctxKey int

const (
	reqIDKey ctxKey = iota
	claimsKey
)

// Middleware wraps a handler.
type Middleware func(http.Handler) http.Handler

// Chain applies middlewares so the first listed is outermost.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// APIError is the JSON error body (same as gateway's apiError).
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
	TraceID string `json:"trace_id"`
}

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes an APIError; trace_id is the request ID.
func WriteError(w http.ResponseWriter, r *http.Request, status int, code, msg string, details any) {
	WriteJSON(w, status, APIError{code, msg, details, RequestID(r.Context())})
}

// RequestID returns the request ID from ctx, or "".
func RequestID(ctx context.Context) string {
	s, _ := ctx.Value(reqIDKey).(string)
	return s
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func validID(id string) bool {
	if id == "" || len(id) > 128 || !utf8.ValidString(id) {
		return false
	}
	for _, c := range id {
		if c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

// RequestIDMiddleware reuses a valid inbound X-Request-ID or generates one,
// echoes it, stores it in ctx and derives a logger carrying request_id.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !validID(id) {
			id = newUUID()
		}
		w.Header().Set(RequestIDHeader, id)
		ctx := context.WithValue(r.Context(), reqIDKey, id)
		ctx = logging.WithContext(ctx, logging.FromContext(ctx).With("request_id", id))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
	wrote  bool
}

func (s *statusWriter) WriteHeader(code int) {
	if !s.wrote {
		s.status, s.wrote = code, true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if !s.wrote {
		s.status, s.wrote = http.StatusOK, true
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += n
	return n, err
}

func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Recover converts panics into a 500 INTERNAL JSON error and logs them.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw, ok := w.(*statusWriter)
		if !ok {
			sw = &statusWriter{ResponseWriter: w, status: http.StatusOK}
		}
		defer func() {
			if p := recover(); p != nil {
				if p == http.ErrAbortHandler {
					panic(p)
				}
				logging.FromContext(r.Context()).Error("panic recovered", "panic", fmt.Sprint(p))
				if !sw.wrote {
					WriteError(sw, r, http.StatusInternalServerError, "INTERNAL", "internal error", nil)
				}
			}
		}()
		next.ServeHTTP(sw, r)
	})
}

// AccessLog logs one line per request (method, path, status, bytes, duration).
// Place it outside Recover so recovered 500s are logged with their status.
func AccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		logging.FromContext(r.Context()).LogAttrs(r.Context(), slog.LevelInfo, "http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", sw.status),
			slog.Int("bytes", sw.bytes),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

// BodyLimit caps request bodies at limit bytes. Reads beyond the cap fail
// with *http.MaxBytesError; use IsBodyTooLarge / WriteBodyError to respond.
func BodyLimit(limit int64) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > limit {
				WriteError(w, r, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE",
					fmt.Sprintf("body exceeds %d bytes", limit), nil)
				return
			}
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// IsBodyTooLarge reports whether err came from the BodyLimit cap.
func IsBodyTooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// WriteBodyError writes the gateway-shaped 413/400 error for a body read error.
func WriteBodyError(w http.ResponseWriter, r *http.Request, err error, limit int64) {
	if IsBodyTooLarge(err) {
		WriteError(w, r, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", fmt.Sprintf("body exceeds %d bytes", limit), nil)
		return
	}
	WriteError(w, r, http.StatusBadRequest, "BODY_READ_ERROR", "cannot read body", nil)
}

// TokenVerifier is satisfied by *authn.Verifier.
type TokenVerifier interface {
	Verify(ctx context.Context, rawToken string) (*authn.Claims, error)
}

// Authn verifies the Bearer token and stores the claims in ctx. Tenant
// identity comes ONLY from these claims; no header or body is consulted.
func Authn(v TokenVerifier) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok, ok := bearer(r.Header.Get("Authorization"))
			if !ok {
				WriteError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required", nil)
				return
			}
			c, err := v.Verify(r.Context(), tok)
			if err != nil {
				if errors.Is(err, authn.ErrDiscoveryUnavailable) || errors.Is(err, authn.ErrJWKSUnavailable) {
					WriteError(w, r, http.StatusServiceUnavailable, "AUTH_UNAVAILABLE", "authentication service unavailable", nil)
					return
				}
				WriteError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required", nil)
				return
			}
			if c == nil || c.TenantID == "" {
				WriteError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required", nil)
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsKey, c)))
		})
	}
}

func bearer(h string) (string, bool) {
	const p = "bearer "
	if len(h) <= len(p) || !strings.EqualFold(h[:len(p)], p) {
		return "", false
	}
	t := strings.TrimSpace(h[len(p):])
	return t, t != ""
}

// ClaimsFrom returns the verified claims stored by Authn.
func ClaimsFrom(ctx context.Context) (*authn.Claims, bool) {
	c, ok := ctx.Value(claimsKey).(*authn.Claims)
	return c, ok && c != nil
}

// TenantID returns the tenant_id from verified claims, or "".
func TenantID(ctx context.Context) string {
	if c, ok := ClaimsFrom(ctx); ok {
		return c.TenantID
	}
	return ""
}
