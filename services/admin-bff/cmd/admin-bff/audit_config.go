package main

import (
	"fmt"
	"strconv"
	"time"
)

type auditConfig struct {
	PGURL         string
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	WriteTimeout  time.Duration
	Retries       int
	DrainTimeout  time.Duration
	PGMaxConns    int32
}

func loadAuditConfig(getenv func(string) string) (auditConfig, error) {
	c := auditConfig{PGURL: getenv("BFF_AUDIT_PG_URL"), QueueSize: 1024, BatchSize: 100,
		FlushInterval: 500 * time.Millisecond, WriteTimeout: 5 * time.Second, Retries: 2,
		DrainTimeout: 10 * time.Second, PGMaxConns: 4}
	var err error
	if c.QueueSize, err = boundedInt(getenv, "BFF_AUDIT_QUEUE_SIZE", c.QueueSize, 1, 65536); err != nil {
		return c, err
	}
	if c.BatchSize, err = boundedInt(getenv, "BFF_AUDIT_BATCH_SIZE", c.BatchSize, 1, 1000); err != nil {
		return c, err
	}
	if c.BatchSize > c.QueueSize {
		return c, fmt.Errorf("BFF_AUDIT_BATCH_SIZE must not exceed BFF_AUDIT_QUEUE_SIZE")
	}
	if c.FlushInterval, err = boundedDuration(getenv, "BFF_AUDIT_FLUSH_INTERVAL", c.FlushInterval, 10*time.Millisecond, time.Minute); err != nil {
		return c, err
	}
	if c.WriteTimeout, err = boundedDuration(getenv, "BFF_AUDIT_WRITE_TIMEOUT", c.WriteTimeout, 100*time.Millisecond, time.Minute); err != nil {
		return c, err
	}
	if c.DrainTimeout, err = boundedDuration(getenv, "BFF_AUDIT_DRAIN_TIMEOUT", c.DrainTimeout, 100*time.Millisecond, 2*time.Minute); err != nil {
		return c, err
	}
	if c.Retries, err = boundedInt(getenv, "BFF_AUDIT_RETRIES", c.Retries, 0, 10); err != nil {
		return c, err
	}
	maxConns, err := boundedInt(getenv, "BFF_AUDIT_PG_MAX_CONNS", int(c.PGMaxConns), 1, 32)
	if err != nil {
		return c, err
	}
	c.PGMaxConns = int32(maxConns)
	return c, nil
}

func boundedInt(getenv func(string) string, key string, fallback, min, max int) (int, error) {
	v := getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s must be between %d and %d", key, min, max)
	}
	return n, nil
}

func boundedDuration(getenv func(string) string, key string, fallback, min, max time.Duration) (time.Duration, error) {
	v := getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < min || d > max {
		return 0, fmt.Errorf("%s must be between %s and %s", key, min, max)
	}
	return d, nil
}
