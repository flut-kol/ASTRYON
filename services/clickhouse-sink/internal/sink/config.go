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
	Brokers                []string
	KafkaTLS               bool   // CHSINK_KAFKA_TLS=1|true
	KafkaTLSCAFile         string // optional PEM bundle; system roots if empty
	KafkaTLSCertFile       string // optional client certificate (mTLS); with KafkaTLSKeyFile
	KafkaTLSKeyFile        string
	KafkaTLSReloadInterval time.Duration // re-read changed CA/cert/key files this often (0 = never)
	KafkaSASLMech          string        // "", PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	KafkaSASLUser          string
	KafkaSASLPass          string
	Topic                  string
	QuarantineTopic        string
	QuarantineTimeout      time.Duration // CHSINK_QUARANTINE_TIMEOUT: max wait for a quarantine produce ack (default 15s)
	GroupID                string
	CHURL                  string
	CHDatabase             string
	CHUser                 string
	CHPassword             string
	CHTable                string
	IntentTable            string        // optional durable chunk boundaries (migration 0004); empty = disabled
	IntentWait             time.Duration // max time to wait for redelivery of a recorded range before giving up on it
	BatchSize              int
	BatchInterval          time.Duration
	MaxRetries             int // 0 = retry until shutdown
	RetryBackoff           time.Duration
	RetryBackoffMax        time.Duration
	ShutdownTimeout        time.Duration
	MetricsAddr            string
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
		Brokers:          strings.Split(env("CHSINK_KAFKA_BROKERS", "127.0.0.1:19092"), ","),
		KafkaTLSCAFile:   os.Getenv("CHSINK_KAFKA_TLS_CA_FILE"),
		KafkaTLSCertFile: os.Getenv("CHSINK_KAFKA_TLS_CERT_FILE"),
		KafkaTLSKeyFile:  os.Getenv("CHSINK_KAFKA_TLS_KEY_FILE"),
		KafkaSASLMech:    strings.ToUpper(strings.TrimSpace(os.Getenv("CHSINK_KAFKA_SASL_MECHANISM"))),
		KafkaSASLUser:    os.Getenv("CHSINK_KAFKA_SASL_USERNAME"),
		KafkaSASLPass:    os.Getenv("CHSINK_KAFKA_SASL_PASSWORD"),
		Topic:            env("CHSINK_TOPIC", "processed.events.v1"),
		QuarantineTopic:  env("CHSINK_QUARANTINE_TOPIC", "events.quarantine.v1"),
		GroupID:          env("CHSINK_GROUP_ID", "clickhouse-sink"),
		CHURL:            env("CHSINK_CH_URL", "http://127.0.0.1:18124"),
		CHDatabase:       env("CHSINK_CH_DATABASE", "astryon"),
		CHUser:           env("CHSINK_CH_USER", "astryon"),
		CHPassword:       os.Getenv("CHSINK_CH_PASSWORD"),
		CHTable:          env("CHSINK_CH_TABLE", "events_normalized_local"),
		IntentTable:      os.Getenv("CHSINK_INTENT_TABLE"),
		MetricsAddr:      os.Getenv("CHSINK_METRICS_ADDR"),
	}
	var err error
	if c.KafkaTLS, err = parseTLSFlag(os.Getenv("CHSINK_KAFKA_TLS")); err != nil {
		return c, err
	}
	if c.KafkaTLSReloadInterval, err = parseTLSReloadInterval("CHSINK_KAFKA", os.Getenv("CHSINK_KAFKA_TLS_RELOAD_INTERVAL")); err != nil {
		return c, err
	}
	if err = c.validateKafkaSecurity(); err != nil {
		return c, err
	}
	if c.QuarantineTimeout, err = envDur("CHSINK_QUARANTINE_TIMEOUT", defaultQuarantineTimeout); err != nil {
		return c, err
	}
	if c.QuarantineTimeout <= 0 {
		return c, fmt.Errorf("CHSINK_QUARANTINE_TIMEOUT must be > 0")
	}
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
	if c.IntentWait, err = envDur("CHSINK_INTENT_WAIT", 30*time.Second); err != nil {
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

// parseTLSFlag accepts 1/true/0/false/empty; anything else is an error so a typo
// (e.g. "yes") cannot silently downgrade the connection to plaintext.
func parseTLSFlag(v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "0", "false":
		return false, nil
	case "1", "true":
		return true, nil
	}
	return false, fmt.Errorf("CHSINK_KAFKA_TLS must be 1/true or 0/false, got %q", v)
}

// validateKafkaSecurity fails closed: a security setting is never silently ignored.
func (c Config) validateKafkaSecurity() error {
	switch c.KafkaSASLMech {
	case "":
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if c.KafkaSASLUser == "" || c.KafkaSASLPass == "" {
			return fmt.Errorf("CHSINK_KAFKA_SASL_MECHANISM requires CHSINK_KAFKA_SASL_USERNAME and CHSINK_KAFKA_SASL_PASSWORD")
		}
	default:
		return fmt.Errorf("unsupported CHSINK_KAFKA_SASL_MECHANISM %q", c.KafkaSASLMech)
	}
	if c.KafkaTLSCAFile != "" && !c.KafkaTLS {
		return fmt.Errorf("CHSINK_KAFKA_TLS_CA_FILE is set but CHSINK_KAFKA_TLS is off (refusing to fall back to plaintext)")
	}
	if c.KafkaSASLMech == "PLAIN" && !c.KafkaTLS {
		return fmt.Errorf("CHSINK_KAFKA_SASL_MECHANISM=PLAIN requires CHSINK_KAFKA_TLS=1 (password would be sent in clear)")
	}
	if err := c.tlsMaterial().validate(c.KafkaTLS); err != nil {
		return err
	}
	return nil
}
