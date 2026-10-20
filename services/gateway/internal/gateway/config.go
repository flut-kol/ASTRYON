package gateway

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
}

// LoadConfig reads ASTRYON_* variables. Secrets have no defaults.
func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{
		Addr:           def(getenv("ASTRYON_GATEWAY_ADDR"), ":8080"),
		MaxBodyBytes:   2 << 20,
		MaxBatchBytes:  8 << 20,
		MaxBatchEvents: 100,
		DevAuth:        getenv("ASTRYON_DEV_AUTH") == "1",
		DevToken:       getenv("ASTRYON_DEV_TOKEN"),
		DevTenantID:    getenv("ASTRYON_DEV_TENANT_ID"),
		DevScopes:      strings.Fields(strings.ReplaceAll(def(getenv("ASTRYON_DEV_SCOPES"), ScopeEventsWrite), ",", " ")),
		DevFakeBroker:  getenv("ASTRYON_DEV_FAKE_PUBLISHER") == "1",
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
	if c.DevAuth && (c.DevToken == "" || c.DevTenantID == "") {
		return c, fmt.Errorf("ASTRYON_DEV_AUTH=1 requires ASTRYON_DEV_TOKEN and ASTRYON_DEV_TENANT_ID")
	}
	return c, nil
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
