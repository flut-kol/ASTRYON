// Package logging provides a minimal structured JSON logging primitive built
// on top of the standard library's log/slog, meant to be shared by ASTRYON
// services (gateway, clickhouse-sink, ...).
//
// It intentionally stays small: a constructor for a service-scoped JSON
// logger, and context plumbing so a per-request logger can be threaded
// through call chains (see DEVELOPMENT_PLAN.md §13, Observability). Callers
// are responsible for enriching the context logger with request-specific
// fields such as trace_id or tenant_id; this package does not invent or
// assume any such fields.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// ctxKey is an unexported type used as the context.Context key for storing a
// *slog.Logger, avoiding collisions with keys defined by other packages.
type ctxKey struct{}

// New returns a *slog.Logger configured with a JSON handler that writes to
// stdout. Every record emitted by the returned logger carries a "service"
// attribute set to serviceName.
//
// level is case-insensitive and may be one of "debug", "info", "warn", or
// "error". Any other value (including empty string) defaults to "info".
func New(serviceName string, level string) *slog.Logger {
	return newLogger(os.Stdout, serviceName, level)
}

// newLogger builds a logger writing to the given writer. It is unexported
// and used internally so tests can capture output without depending on the
// process's stdout.
func newLogger(w io.Writer, serviceName string, level string) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: parseLevel(level),
	})
	return slog.New(handler).With("service", serviceName)
}

// parseLevel maps a level string to a slog.Level, defaulting to LevelInfo
// for unrecognized input.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// WithContext returns a copy of ctx carrying logger, so downstream code can
// retrieve a request-scoped logger via FromContext. Callers typically derive
// logger from a base logger (e.g. via New) and attach request-specific
// attributes (trace_id, tenant_id, etc.) before storing it.
func WithContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, logger)
}

// FromContext returns the logger previously stored in ctx via WithContext.
// If ctx has no logger attached, it returns slog.Default() so callers always
// get a usable logger.
func FromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}
	return slog.Default()
}
