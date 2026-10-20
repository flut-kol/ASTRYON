package processor

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	ec "github.com/astryon/event-contracts"
)

// Config is read from environment variables only (STREAMPROC_*); no secrets in code.
type Config struct {
	Brokers         []string
	GroupID         string
	InputTopic      string
	OutputTopic     string
	QuarantineTopic string
	DLQTopic        string
	PipelineVersion int
	BatchSize       int
	BatchInterval   time.Duration
	DedupTTL        time.Duration
	DedupMaxEntries int
	WarmStart       bool // rebuild dedup window from processed topic on start
	MaxRecordBytes  int
	MaxRetries      int
	RetryBackoff    time.Duration
	RetryBackoffMax time.Duration
	MetricsAddr     string
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// LoadConfig reads and validates the configuration.
func LoadConfig() (Config, error) {
	var err error
	ei := func(k string, d int) int {
		v := os.Getenv(k)
		if v == "" || err != nil {
			return d
		}
		n, e := strconv.Atoi(v)
		if e != nil {
			err = fmt.Errorf("%s: %w", k, e)
		}
		return n
	}
	ed := func(k string, d time.Duration) time.Duration {
		v := os.Getenv(k)
		if v == "" || err != nil {
			return d
		}
		x, e := time.ParseDuration(v)
		if e != nil {
			err = fmt.Errorf("%s: %w", k, e)
		}
		return x
	}
	c := Config{
		Brokers:         strings.Split(env("STREAMPROC_BROKERS", "127.0.0.1:19092"), ","),
		GroupID:         env("STREAMPROC_GROUP_ID", "stream-processor"),
		InputTopic:      env("STREAMPROC_INPUT_TOPIC", ec.TopicRawEvents),
		OutputTopic:     env("STREAMPROC_OUTPUT_TOPIC", ec.TopicProcessedEvents),
		QuarantineTopic: env("STREAMPROC_QUARANTINE_TOPIC", ec.TopicQuarantine),
		DLQTopic:        env("STREAMPROC_DLQ_TOPIC", ec.TopicDLQ),
		PipelineVersion: ei("STREAMPROC_PIPELINE_VERSION", 1),
		BatchSize:       ei("STREAMPROC_BATCH_SIZE", 500),
		BatchInterval:   ed("STREAMPROC_BATCH_INTERVAL", 500*time.Millisecond),
		DedupTTL:        ed("STREAMPROC_DEDUP_TTL", time.Hour),
		DedupMaxEntries: ei("STREAMPROC_DEDUP_MAX_ENTRIES", 2_000_000),
		WarmStart:       env("STREAMPROC_WARM_START", "true") == "true",
		MaxRecordBytes:  ei("STREAMPROC_MAX_RECORD_BYTES", 1<<20+64<<10),
		MaxRetries:      ei("STREAMPROC_MAX_RETRIES", 0),
		RetryBackoff:    ed("STREAMPROC_RETRY_BACKOFF", 200*time.Millisecond),
		RetryBackoffMax: ed("STREAMPROC_RETRY_BACKOFF_MAX", 10*time.Second),
		MetricsAddr:     env("STREAMPROC_METRICS_ADDR", ""),
	}
	if err != nil {
		return c, err
	}
	return c, c.Validate()
}

// Validate rejects unbounded or nonsensical settings.
func (c Config) Validate() error {
	switch {
	case len(c.Brokers) == 0 || c.Brokers[0] == "":
		return fmt.Errorf("brokers required")
	case c.DedupTTL <= 0:
		return fmt.Errorf("dedup ttl must be > 0")
	case c.DedupMaxEntries <= 0:
		return fmt.Errorf("dedup max entries must be > 0 (state must be bounded)")
	case c.PipelineVersion < 1:
		return fmt.Errorf("pipeline version must be >= 1")
	case c.BatchSize < 1 || c.BatchInterval <= 0:
		return fmt.Errorf("batch size/interval must be positive")
	case c.OutputTopic == c.InputTopic:
		return fmt.Errorf("output topic must differ from input topic")
	}
	return nil
}

// Options derives runner options.
func (c Config) Options() Options {
	return Options{
		Normalizer:  Normalizer{PipelineVersion: c.PipelineVersion, MaxRecordBytes: c.MaxRecordBytes},
		OutputTopic: c.OutputTopic, QuarantineTopic: c.QuarantineTopic, DLQTopic: c.DLQTopic,
		BatchInterval: c.BatchInterval, MaxRetries: c.MaxRetries,
		RetryBackoff: c.RetryBackoff, RetryBackoffMax: c.RetryBackoffMax,
	}
}
