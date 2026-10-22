package main

import (
	"strings"
	"testing"
	"time"
)

func TestAuditConfigOptInAndBounds(t *testing.T) {
	env := map[string]string{}
	get := func(k string) string { return env[k] }
	url, _, drain, err := auditConfig(get)
	if err != nil || url != "" || drain != defaultAuditDrainTimeout {
		t.Fatalf("off by default: url=%q drain=%s err=%v", url, drain, err)
	}
	env = map[string]string{
		"QUERY_AUDIT_PG_URL":                "postgres://audit",
		"QUERY_AUDIT_QUEUE_SIZE":            "17",
		"QUERY_AUDIT_BATCH_SIZE":            "3",
		"QUERY_AUDIT_FLUSH_INTERVAL_MS":     "25",
		"QUERY_AUDIT_WRITE_TIMEOUT_MS":      "40",
		"QUERY_AUDIT_DRAIN_TIMEOUT_SECONDS": "7",
	}
	url, cfg, drain, err := auditConfig(get)
	if err != nil || url != "postgres://audit" || cfg.QueueSize != 17 || cfg.BatchSize != 3 ||
		cfg.FlushInterval != 25*time.Millisecond || cfg.WriteTimeout != 40*time.Millisecond || drain != 7*time.Second {
		t.Fatalf("url=%q cfg=%+v drain=%s err=%v", url, cfg, drain, err)
	}
	for _, name := range []string{"QUERY_AUDIT_QUEUE_SIZE", "QUERY_AUDIT_BATCH_SIZE", "QUERY_AUDIT_FLUSH_INTERVAL_MS", "QUERY_AUDIT_WRITE_TIMEOUT_MS", "QUERY_AUDIT_DRAIN_TIMEOUT_SECONDS"} {
		t.Run(name, func(t *testing.T) {
			env[name] = "0"
			defer func() { delete(env, name) }()
			if _, _, _, err := auditConfig(get); err == nil {
				t.Fatal("invalid bound accepted")
			}
		})
	}
}

func TestAuditMetricsDisabled(t *testing.T) {
	var b strings.Builder
	var runtime *auditRuntime
	runtime.WritePrometheus(&b)
	for _, want := range []string{"astryon_query_api_audit_enabled 0", `audit_events_total{outcome="dropped"} 0`, `audit_events_total{outcome="rejected"} 0`} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %q in\n%s", want, b.String())
		}
	}
}
