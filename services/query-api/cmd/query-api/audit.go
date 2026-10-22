package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/astryon/audit"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultAuditDrainTimeout = 10 * time.Second

type auditRuntime struct {
	writer       *audit.Writer
	pool         *pgxpool.Pool
	drainTimeout time.Duration
}

func auditConfig(getenv func(string) string) (string, audit.Config, time.Duration, error) {
	url := getenv("QUERY_AUDIT_PG_URL")
	if url == "" {
		return "", audit.Config{}, defaultAuditDrainTimeout, nil
	}
	cfg := audit.Config{}
	parse := func(name string, dst *int) error {
		raw := getenv(name)
		if raw == "" {
			return nil
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return fmt.Errorf("%s must be a positive integer", name)
		}
		*dst = n
		return nil
	}
	if err := parse("QUERY_AUDIT_QUEUE_SIZE", &cfg.QueueSize); err != nil {
		return "", cfg, 0, err
	}
	if err := parse("QUERY_AUDIT_BATCH_SIZE", &cfg.BatchSize); err != nil {
		return "", cfg, 0, err
	}
	var flushMS, writeMS, drainSeconds int
	if err := parse("QUERY_AUDIT_FLUSH_INTERVAL_MS", &flushMS); err != nil {
		return "", cfg, 0, err
	}
	if err := parse("QUERY_AUDIT_WRITE_TIMEOUT_MS", &writeMS); err != nil {
		return "", cfg, 0, err
	}
	if err := parse("QUERY_AUDIT_DRAIN_TIMEOUT_SECONDS", &drainSeconds); err != nil {
		return "", cfg, 0, err
	}
	if flushMS > 0 {
		cfg.FlushInterval = time.Duration(flushMS) * time.Millisecond
	}
	if writeMS > 0 {
		cfg.WriteTimeout = time.Duration(writeMS) * time.Millisecond
	}
	drain := defaultAuditDrainTimeout
	if drainSeconds > 0 {
		drain = time.Duration(drainSeconds) * time.Second
	}
	return url, cfg, drain, nil
}

func startAudit(ctx context.Context, getenv func(string) string) (*auditRuntime, error) {
	url, cfg, drain, err := auditConfig(getenv)
	if err != nil || url == "" {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("QUERY_AUDIT_PG_URL: %w", err)
	}
	return &auditRuntime{writer: audit.NewWriter(pool, cfg), pool: pool, drainTimeout: drain}, nil
}

func (a *auditRuntime) Close() error {
	if a == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.drainTimeout)
	defer cancel()
	err := a.writer.Close(ctx)
	a.pool.Close()
	return err
}

func (a *auditRuntime) WritePrometheus(w io.Writer) {
	var enabled int
	var s audit.Stats
	if a != nil {
		enabled = 1
		s = a.writer.Stats()
	}
	fmt.Fprint(w, "# HELP astryon_query_api_audit_enabled Whether the PostgreSQL audit writer is configured\n# TYPE astryon_query_api_audit_enabled gauge\n")
	fmt.Fprintf(w, "astryon_query_api_audit_enabled %d\n", enabled)
	fmt.Fprint(w, "# HELP astryon_query_api_audit_events_total Audit writer events by outcome\n# TYPE astryon_query_api_audit_events_total counter\n")
	fmt.Fprintf(w, "astryon_query_api_audit_events_total{outcome=\"queued\"} %d\n", s.Queued)
	fmt.Fprintf(w, "astryon_query_api_audit_events_total{outcome=\"written\"} %d\n", s.Written)
	fmt.Fprintf(w, "astryon_query_api_audit_events_total{outcome=\"dropped\"} %d\n", s.Dropped)
	fmt.Fprintf(w, "astryon_query_api_audit_events_total{outcome=\"rejected\"} %d\n", s.Rejected)
}
