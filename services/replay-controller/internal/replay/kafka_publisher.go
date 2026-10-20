package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// TopicReplayJobs is the topic consumed by services/replay-worker.
const TopicReplayJobs = "replay.jobs.v1"

// KafkaConfig is the producer configuration, loaded from env only (same variables as the gateway).
type KafkaConfig struct {
	Brokers         []string
	ClientID        string
	TLS             bool
	TLSCAFile       string // optional PEM bundle; system roots if empty
	SASLMech        string // "", PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUser        string
	SASLPass        string
	DeliveryTimeout time.Duration
}

// LoadKafkaConfig reads ASTRYON_KAFKA_BROKERS (required), ASTRYON_KAFKA_CLIENT_ID, ASTRYON_KAFKA_TLS (1/true/0/false; anything else is an error),
// ASTRYON_KAFKA_TLS_CA_FILE, ASTRYON_KAFKA_SASL_MECHANISM/USERNAME/PASSWORD, ASTRYON_KAFKA_DELIVERY_TIMEOUT.
func LoadKafkaConfig(getenv func(string) string) (KafkaConfig, error) {
	c := KafkaConfig{
		ClientID:        "astryon-replay-controller",
		TLSCAFile:       getenv("ASTRYON_KAFKA_TLS_CA_FILE"),
		SASLMech:        strings.ToUpper(strings.TrimSpace(getenv("ASTRYON_KAFKA_SASL_MECHANISM"))),
		SASLUser:        getenv("ASTRYON_KAFKA_SASL_USERNAME"),
		SASLPass:        getenv("ASTRYON_KAFKA_SASL_PASSWORD"),
		DeliveryTimeout: 30 * time.Second,
	}
	tlsOn, err := parseTLSFlag(getenv("ASTRYON_KAFKA_TLS"))
	if err != nil {
		return c, err
	}
	c.TLS = tlsOn
	if v := getenv("ASTRYON_KAFKA_CLIENT_ID"); v != "" {
		c.ClientID = v
	}
	for _, b := range strings.Split(getenv("ASTRYON_KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.Brokers = append(c.Brokers, b)
		}
	}
	if len(c.Brokers) == 0 {
		return c, errors.New("ASTRYON_KAFKA_BROKERS is required (comma-separated host:port list)")
	}
	if v := getenv("ASTRYON_KAFKA_DELIVERY_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return c, errors.New("ASTRYON_KAFKA_DELIVERY_TIMEOUT must be a positive duration")
		}
		c.DeliveryTimeout = d
	}
	switch c.SASLMech {
	case "":
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if c.SASLUser == "" || c.SASLPass == "" {
			return c, errors.New("ASTRYON_KAFKA_SASL_MECHANISM requires ASTRYON_KAFKA_SASL_USERNAME and ASTRYON_KAFKA_SASL_PASSWORD")
		}
	default:
		return c, fmt.Errorf("unsupported ASTRYON_KAFKA_SASL_MECHANISM %q", c.SASLMech)
	}
	// Fail closed: never let a security setting be silently ignored.
	if c.TLSCAFile != "" && !c.TLS {
		return c, errors.New("ASTRYON_KAFKA_TLS_CA_FILE is set but ASTRYON_KAFKA_TLS is off (refusing to fall back to plaintext)")
	}
	if c.SASLMech == "PLAIN" && !c.TLS {
		return c, errors.New("ASTRYON_KAFKA_SASL_MECHANISM=PLAIN requires ASTRYON_KAFKA_TLS=1 (password would be sent in clear)")
	}
	return c, nil
}

// kafkaProducer is the seam between KafkaJobPublisher and the client library.
// produce returns nil only after the broker acknowledged (acks=all).
type kafkaProducer interface {
	produce(ctx context.Context, topic string, key, value []byte) error
	close()
}

// KafkaJobPublisher is the production JobPublisher: JSON ReplayJob on replay.jobs.v1, acks=all,
// idempotent producer, key = tenant_id + 0x1f + source_id (all jobs of one source stay ordered).
type KafkaJobPublisher struct {
	p     kafkaProducer
	topic string
}

// NewKafkaJobPublisher builds the client; the broker need not be reachable yet (Publish fails until it is).
func NewKafkaJobPublisher(c KafkaConfig) (*KafkaJobPublisher, error) {
	p, err := newFranzProducer(c)
	if err != nil {
		return nil, err
	}
	return &KafkaJobPublisher{p: p, topic: TopicReplayJobs}, nil
}

// JobKey is the Kafka key of a job: tenant_id 0x1f source_id.
func JobKey(job ReplayJob) []byte { return []byte(job.TenantID + "\x1f" + job.SourceID) }

// Publish returns nil only after durable broker acknowledgement.
func (k *KafkaJobPublisher) Publish(ctx context.Context, job ReplayJob) error {
	if job.TenantID == "" || job.SourceID == "" {
		return fmt.Errorf("%w: job without tenant/source", ErrInvalid)
	}
	b, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err := k.p.produce(ctx, k.topic, JobKey(job), b); err != nil {
		return fmt.Errorf("kafka publish to %s: %w", k.topic, err)
	}
	return nil
}

// Close releases connections (Publish is synchronous, nothing is left buffered).
func (k *KafkaJobPublisher) Close() { k.p.close() }

var _ JobPublisher = (*KafkaJobPublisher)(nil)
