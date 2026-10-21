package main

import (
	"strings"
	"testing"
	"time"
)

func envMap(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadAuditConfig(t *testing.T) {
	c, err := loadAuditConfig(envMap(map[string]string{
		"BFF_AUDIT_PG_URL":         "postgres://audit",
		"BFF_AUDIT_QUEUE_SIZE":     "64",
		"BFF_AUDIT_BATCH_SIZE":     "16",
		"BFF_AUDIT_FLUSH_INTERVAL": "20ms",
		"BFF_AUDIT_WRITE_TIMEOUT":  "2s",
		"BFF_AUDIT_RETRIES":        "0",
		"BFF_AUDIT_DRAIN_TIMEOUT":  "3s",
		"BFF_AUDIT_PG_MAX_CONNS":   "2",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.PGURL != "postgres://audit" || c.QueueSize != 64 || c.BatchSize != 16 || c.FlushInterval != 20*time.Millisecond || c.WriteTimeout != 2*time.Second || c.Retries != 0 || c.DrainTimeout != 3*time.Second || c.PGMaxConns != 2 {
		t.Fatalf("config = %+v", c)
	}
}

func TestLoadAuditConfigBounds(t *testing.T) {
	for key, value := range map[string]string{
		"BFF_AUDIT_QUEUE_SIZE": "0", "BFF_AUDIT_BATCH_SIZE": "1001", "BFF_AUDIT_FLUSH_INTERVAL": "1ms",
		"BFF_AUDIT_WRITE_TIMEOUT": "2m", "BFF_AUDIT_RETRIES": "11", "BFF_AUDIT_DRAIN_TIMEOUT": "3m", "BFF_AUDIT_PG_MAX_CONNS": "33",
	} {
		t.Run(key, func(t *testing.T) {
			if _, err := loadAuditConfig(envMap(map[string]string{key: value})); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if _, err := loadAuditConfig(envMap(map[string]string{"BFF_AUDIT_QUEUE_SIZE": "5", "BFF_AUDIT_BATCH_SIZE": "6"})); err == nil {
		t.Fatal("batch larger than queue accepted")
	}
}
