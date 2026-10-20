package sink

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is read from environment variables only.
type Config struct {
	Brokers         []string
	Topic           string
	QuarantineTopic string
	GroupID         string
	CHURL           string
	CHDatabase      string
	CHUser          string
	CHPassword      string
	CHTable         string
	BatchSize       int
	BatchInterval   time.Duration
	MaxRetries      int // 0 = retry until shutdown
	RetryBackoff    time.Duration
	RetryBackoffMax time.Duration
	ShutdownTimeout time.Duration
	MetricsAddr     string
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return d, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return n, nil
}

func envDur(k string, d time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return d, nil
	}
	x, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return x, nil
}

// LoadConfig reads CHSINK_* variables. Defaults match infra/docker/docker-compose.dev.yml.
func LoadConfig() (Config, error) {
	c := Config{
		Brokers:         strings.Split(env("CHSINK_KAFKA_BROKERS", "127.0.0.1:19092"), ","),
		Topic:           env("CHSINK_TOPIC", "processed.events.v1"),
		QuarantineTopic: env("CHSINK_QUARANTINE_TOPIC", "events.quarantine.v1"),
		GroupID:         env("CHSINK_GROUP_ID", "clickhouse-sink"),
		CHURL:           env("CHSINK_CH_URL", "http://127.0.0.1:18124"),
		CHDatabase:      env("CHSINK_CH_DATABASE", "astryon"),
		CHUser:          env("CHSINK_CH_USER", "astryon"),
		CHPassword:      os.Getenv("CHSINK_CH_PASSWORD"),
		CHTable:         env("CHSINK_CH_TABLE", "events_normalized_local"),
		MetricsAddr:     os.Getenv("CHSINK_METRICS_ADDR"),
	}
	var err error
	if c.BatchSize, err = envInt("CHSINK_BATCH_SIZE", 5000); err != nil {
		return c, err
	}
	if c.MaxRetries, err = envInt("CHSINK_MAX_RETRIES", 0); err != nil {
		return c, err
	}
	if c.BatchInterval, err = envDur("CHSINK_BATCH_INTERVAL", 2*time.Second); err != nil {
		return c, err
	}
	if c.RetryBackoff, err = envDur("CHSINK_RETRY_BACKOFF", 500*time.Millisecond); err != nil {
		return c, err
	}
	if c.RetryBackoffMax, err = envDur("CHSINK_RETRY_BACKOFF_MAX", 30*time.Second); err != nil {
		return c, err
	}
	if c.ShutdownTimeout, err = envDur("CHSINK_SHUTDOWN_TIMEOUT", 30*time.Second); err != nil {
		return c, err
	}
	if c.BatchSize < 1 {
		return c, fmt.Errorf("CHSINK_BATCH_SIZE must be >= 1")
	}
	if c.BatchInterval <= 0 {
		return c, fmt.Errorf("CHSINK_BATCH_INTERVAL must be > 0")
	}
	return c, nil
}
