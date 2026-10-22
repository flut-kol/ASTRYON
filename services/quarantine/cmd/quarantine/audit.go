package main

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/astryon/audit"
	"github.com/astryon/quarantine/internal/replay"
	"github.com/jackc/pgx/v5/pgxpool"
)

// auditMirror adapts the bounded libs/audit writer. It is deliberately
// best-effort: Kafka audit remains the required, authoritative stream and its
// before-publish ordering is unchanged.
type auditMirror struct{ writer *audit.Writer }

func (m auditMirror) RecordMirror(ctx context.Context, e replay.MirrorEvent) error {
	outcome := audit.OutcomeFailure
	if e.Outcome == "success" {
		outcome = audit.OutcomeSuccess
	} else if e.Outcome == "denied" {
		outcome = audit.OutcomeDenied
	}
	ev := audit.NewEvent(audit.Claims{TenantID: e.TenantID, Subject: e.Subject, ClientID: e.ClientID, ACR: e.ACR}, audit.CategorySecurity, e.Action, outcome, audit.Resource{Type: e.ResourceType, ID: e.ResourceID}).WithTrace(e.TraceID, e.RequestID)
	return m.writer.Submit(ev)
}

func newAuditMirror(ctx context.Context, getenv func(string) string) (*auditMirror, *pgxpool.Pool, error) {
	url := getenv("QUARANTINE_AUDIT_PG_URL")
	if url == "" {
		return nil, nil, nil
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, err
	}
	m := &auditMirror{writer: audit.NewWriter(pool, audit.Config{
		QueueSize:     getenvInt(getenv, "QUARANTINE_AUDIT_QUEUE_SIZE", 1024),
		BatchSize:     getenvInt(getenv, "QUARANTINE_AUDIT_BATCH_SIZE", 100),
		FlushInterval: getenvDuration(getenv, "QUARANTINE_AUDIT_FLUSH_INTERVAL", 500*time.Millisecond),
		WriteTimeout:  getenvDuration(getenv, "QUARANTINE_AUDIT_WRITE_TIMEOUT", 5*time.Second),
		OnDrop:        func(_ audit.Event, reason error) { log.Printf("quarantine audit mirror dropped event: %v", reason) },
	})}
	return m, pool, nil
}

func (m *auditMirror) close(ctx context.Context) error {
	if m == nil || m.writer == nil {
		return nil
	}
	return m.writer.Close(ctx)
}

func getenvInt(getenv func(string) string, key string, fallback int) int {
	v, err := strconv.Atoi(getenv(key))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func getenvDuration(getenv func(string) string, key string, fallback time.Duration) time.Duration {
	v, err := time.ParseDuration(getenv(key))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}
