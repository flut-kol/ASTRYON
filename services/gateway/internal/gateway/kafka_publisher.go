package gateway

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// KafkaConfig is the producer configuration, loaded from env only.
type KafkaConfig struct {
	Brokers   []string
	ClientID  string
	TLS       bool
	TLSCAFile string // optional PEM bundle; system roots if empty
	SASLMech  string // "", PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUser  string
	SASLPass  string
	// DeliveryTimeout bounds one record from produce call to ack (incl. retries).
	DeliveryTimeout time.Duration
}

// LoadKafkaConfig reads ASTRYON_KAFKA_* variables:
//
//	ASTRYON_KAFKA_BROKERS (required, comma list), ASTRYON_KAFKA_CLIENT_ID,
//	ASTRYON_KAFKA_TLS=1, ASTRYON_KAFKA_TLS_CA_FILE,
//	ASTRYON_KAFKA_SASL_MECHANISM, ASTRYON_KAFKA_SASL_USERNAME, ASTRYON_KAFKA_SASL_PASSWORD,
//	ASTRYON_KAFKA_DELIVERY_TIMEOUT (Go duration, default 30s).
func LoadKafkaConfig(getenv func(string) string) (KafkaConfig, error) {
	c := KafkaConfig{
		ClientID:        def(getenv("ASTRYON_KAFKA_CLIENT_ID"), "astryon-gateway"),
		TLS:             getenv("ASTRYON_KAFKA_TLS") == "1",
		TLSCAFile:       getenv("ASTRYON_KAFKA_TLS_CA_FILE"),
		SASLMech:        strings.ToUpper(strings.TrimSpace(getenv("ASTRYON_KAFKA_SASL_MECHANISM"))),
		SASLUser:        getenv("ASTRYON_KAFKA_SASL_USERNAME"),
		SASLPass:        getenv("ASTRYON_KAFKA_SASL_PASSWORD"),
		DeliveryTimeout: 30 * time.Second,
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
	return c, nil
}

func (c KafkaConfig) saslMechanism() sasl.Mechanism {
	switch c.SASLMech {
	case "PLAIN":
		return plain.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsMechanism()
	case "SCRAM-SHA-256":
		return scram.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsSha256Mechanism()
	case "SCRAM-SHA-512":
		return scram.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsSha512Mechanism()
	}
	return nil
}

// kafkaProducer is the seam between KafkaPublisher and the client library.
// produce must return nil only after the broker acknowledged (acks=all).
type kafkaProducer interface {
	produce(ctx context.Context, topic string, key, value []byte) error
	close()
}

type franzProducer struct{ cl *kgo.Client }

func (p franzProducer) produce(ctx context.Context, topic string, key, value []byte) error {
	// ProduceSync blocks until the record is acked or permanently failed.
	return p.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr()
}

func (p franzProducer) close() { p.cl.Close() }

// KafkaPublisher is the production Publisher: acks=all, idempotent producer
// (franz-go default; requires acks=all), retries within DeliveryTimeout.
type KafkaPublisher struct {
	p kafkaProducer
}

// NewKafkaPublisher builds the client. It does not require the broker to be
// reachable at construction; Publish fails (no ack) until it is.
func NewKafkaPublisher(c KafkaConfig) (*KafkaPublisher, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotence is on by default; do NOT call DisableIdempotentWrite.
		kgo.RecordRetries(1 << 30), // bounded by RecordDeliveryTimeout instead
		kgo.RecordDeliveryTimeout(c.DeliveryTimeout),
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.RetryBackoffFn(func(n int) time.Duration {
			d := 100 * time.Millisecond << min(n, 5)
			return d
		}),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<10, true, true, nil)),
		kgo.ProducerLinger(0), // latency-first: each Publish is awaited individually
	}
	if c.TLS {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if c.TLSCAFile != "" {
			pem, err := os.ReadFile(c.TLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("read ASTRYON_KAFKA_TLS_CA_FILE: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("ASTRYON_KAFKA_TLS_CA_FILE: no valid PEM certificates")
			}
			tc.RootCAs = pool
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	if m := c.saslMechanism(); m != nil {
		opts = append(opts, kgo.SASL(m))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &KafkaPublisher{p: franzProducer{cl}}, nil
}

// Publish returns nil only after durable broker acknowledgement.
func (k *KafkaPublisher) Publish(ctx context.Context, topic string, key, value []byte) error {
	if err := k.p.produce(ctx, topic, key, value); err != nil {
		return fmt.Errorf("kafka publish to %s: %w", topic, err)
	}
	return nil
}

// Close flushes buffered records (none are left unacked by Publish, as it is
// synchronous) and releases connections. Safe to call once at shutdown.
func (k *KafkaPublisher) Close() { k.p.close() }

var _ Publisher = (*KafkaPublisher)(nil)
