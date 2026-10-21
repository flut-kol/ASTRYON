package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/astryon/audit"
)

func TestLoadAuditConfigBounded(t *testing.T) {
	env := map[string]string{
		"ASTRYON_AUDIT_PG_URL":         "postgres://audit",
		"ASTRYON_AUDIT_QUEUE_SIZE":     "32",
		"ASTRYON_AUDIT_BATCH_SIZE":     "8",
		"ASTRYON_AUDIT_FLUSH_INTERVAL": "20ms",
		"ASTRYON_AUDIT_WRITE_TIMEOUT":  "2s",
		"ASTRYON_AUDIT_DRAIN_TIMEOUT":  "3s",
		"ASTRYON_AUDIT_RETRIES":        "0",
		"ASTRYON_AUDIT_PG_MAX_CONNS":   "2",
	}
	c, err := loadAuditConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.PGURL != "postgres://audit" || c.QueueSize != 32 || c.BatchSize != 8 || c.Retries != 0 || c.PGMaxConns != 2 {
		t.Fatalf("config: %+v", c)
	}
	for _, bad := range []map[string]string{
		{"ASTRYON_AUDIT_QUEUE_SIZE": "0"},
		{"ASTRYON_AUDIT_QUEUE_SIZE": "8", "ASTRYON_AUDIT_BATCH_SIZE": "9"},
		{"ASTRYON_AUDIT_BATCH_SIZE": "1001"},
		{"ASTRYON_AUDIT_FLUSH_INTERVAL": "1ms"},
		{"ASTRYON_AUDIT_WRITE_TIMEOUT": "2m"},
		{"ASTRYON_AUDIT_DRAIN_TIMEOUT": "3m"},
		{"ASTRYON_AUDIT_RETRIES": "11"},
		{"ASTRYON_AUDIT_PG_MAX_CONNS": "33"},
	} {
		if _, err := loadAuditConfig(func(k string) string { return bad[k] }); err == nil {
			t.Fatalf("accepted invalid config: %+v", bad)
		}
	}
}

func TestWriteAuditMetrics(t *testing.T) {
	var b bytes.Buffer
	writeAuditStats(&b, audit.Stats{Queued: 7, Written: 5, Dropped: 2, Rejected: 1})
	for _, want := range []string{
		"astryon_audit_queued_total 7", "astryon_audit_written_total 5",
		"astryon_audit_dropped_total 2", "astryon_audit_rejected_total 1",
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %q in %s", want, b.String())
		}
	}
	b.Reset()
	writeAuditMetrics(&b, nil)
	if b.Len() != 0 {
		t.Fatalf("nil writer emitted metrics: %q", b.String())
	}
}
