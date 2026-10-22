package gateway

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is loaded from environment only.
type Config struct {
	Addr           string
	MaxBodyBytes   int64 // POST /v1/events
	MaxBatchBytes  int64 // POST /v1/events/batch
	MaxBatchEvents int

	DevAuth       bool
	DevToken      string
	DevTenantID   string
	DevScopes     []string
	DevFakeBroker bool

	AuditPGURL         string
	AuditQueueSize     int
	AuditBatchSize     int
	AuditFlushInterval time.Duration
	AuditWriteTimeout  time.Duration
	AuditRetries       int
	AuditDrainTimeout  time.Duration
	AuditPGMaxConns    int32
}

// LoadConfig reads ASTRYON_* variables. Secrets have no defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{
		Addr:               def(getenv("ASTRYON_GATEWAY_ADDR"), ":8080"),
		MaxBodyBytes:       2 << 20,
		MaxBatchBytes:      8 << 20,
		MaxBatchEvents:     100,
		DevAuth:            getenv("ASTRYON_DEV_AUTH") == "1",
		DevToken:           getenv("ASTRYON_DEV_TOKEN"),
		DevTenantID:        getenv("ASTRYON_DEV_TENANT_ID"),
		DevScopes:          strings.Fields(strings.ReplaceAll(def(getenv("ASTRYON_DEV_SCOPES"), ScopeEventsWrite), ",", " ")),
		DevFakeBroker:      getenv("ASTRYON_DEV_FAKE_PUBLISHER") == "1",
		AuditPGURL:         getenv("ASTRYON_AUDIT_PG_URL"),
		AuditQueueSize:     1024,
		AuditBatchSize:     100,
		AuditFlushInterval: 500 * time.Millisecond,
		AuditWriteTimeout:  5 * time.Second,
		AuditRetries:       2,
		AuditDrainTimeout:  10 * time.Second,
		AuditPGMaxConns:    4,
	}
	var err error
	if c.MaxBodyBytes, err = envInt64(getenv, "ASTRYON_GATEWAY_MAX_BODY_BYTES", c.MaxBodyBytes); err != nil {
		return c, err
	}
	if c.MaxBatchBytes, err = envInt64(getenv, "ASTRYON_GATEWAY_MAX_BATCH_BYTES", c.MaxBatchBytes); err != nil {
		return c, err
	}
	n, err := envInt64(getenv, "ASTRYON_GATEWAY_MAX_BATCH_EVENTS", int64(c.MaxBatchEvents))
	if err != nil {
		return c, err
	}
	c.MaxBatchEvents = int(n)
	if c.AuditQueueSize, err = envPositiveInt(getenv, "ASTRYON_AUDIT_QUEUE_SIZE", c.AuditQueueSize); err != nil {
		return c, err
	}
	if c.AuditBatchSize, err = envPositiveInt(getenv, "ASTRYON_AUDIT_BATCH_SIZE", c.AuditBatchSize); err != nil {
		return c, err
	}
	if c.AuditFlushInterval, err = envPositiveDuration(getenv, "ASTRYON_AUDIT_FLUSH_INTERVAL", c.AuditFlushInterval); err != nil {
		return c, err
	}
	if c.AuditWriteTimeout, err = envPositiveDuration(getenv, "ASTRYON_AUDIT_WRITE_TIMEOUT", c.AuditWriteTimeout); err != nil {
		return c, err
	}
	if c.AuditDrainTimeout, err = envPositiveDuration(getenv, "ASTRYON_AUDIT_DRAIN_TIMEOUT", c.AuditDrainTimeout); err != nil {
		return c, err
	}
	if c.AuditRetries, err = envNonNegativeInt(getenv, "ASTRYON_AUDIT_RETRIES", c.AuditRetries); err != nil {
		return c, err
	}
	maxConns, err := envInt64(getenv, "ASTRYON_AUDIT_PG_MAX_CONNS", int64(c.AuditPGMaxConns))
	if err != nil || maxConns > 32 {
		return c, fmt.Errorf("ASTRYON_AUDIT_PG_MAX_CONNS must be between 1 and 32")
	}
	c.AuditPGMaxConns = int32(maxConns)
	if c.DevAuth && (c.DevToken == "" || c.DevTenantID == "") {
		return c, fmt.Errorf("ASTRYON_DEV_AUTH=1 requires ASTRYON_DEV_TOKEN and ASTRYON_DEV_TENANT_ID")
	}
	return c, nil
}

func envPositiveInt(getenv func(string) string, k string, d int) (int, error) {
	n, err := envInt64(getenv, k, int64(d))
	return int(n), err
}

func envNonNegativeInt(getenv func(string) string, k string, d int) (int, error) {
	v := getenv(k)
	if v == "" {
		return d, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", k)
	}
	return int(n), nil
}

func envPositiveDuration(getenv func(string) string, k string, d time.Duration) (time.Duration, error) {
	v := getenv(k)
	if v == "" {
		return d, nil
	}
	n, err := time.ParseDuration(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", k)
	}
	return n, nil
}

func def(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func envInt64(getenv func(string) string, k string, d int64) (int64, error) {
	v := getenv(k)
	if v == "" {
		return d, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", k)
	}
	return n, nil
}

// Getenv is os.Getenv, injectable for tests.
var Getenv = os.Getenv
