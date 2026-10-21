// Package kafkaemit is the runner Emitter backed by franz-go: envelopes go to the raw ingest topic, keyed by
// tenant + entity (event-contracts PartitionKey), acks=all, idempotent producer, bounded delivery timeout.
package kafkaemit

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	ec "github.com/astryon/event-contracts"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config is loaded from ASTRYON_KAFKA_* env only (same names as the gateway).
type Config struct {
	Brokers         []string
	ClientID        string
	TLS             bool
	TLSCAFile       string
	SASLMech        string
	SASLUser        string
	SASLPass        string
	DeliveryTimeout time.Duration
}

func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{ClientID: "astryon-connector-manager", TLSCAFile: getenv("ASTRYON_KAFKA_TLS_CA_FILE"),
		SASLMech: strings.ToUpper(strings.TrimSpace(getenv("ASTRYON_KAFKA_SASL_MECHANISM"))),
		SASLUser: getenv("ASTRYON_KAFKA_SASL_USERNAME"), SASLPass: getenv("ASTRYON_KAFKA_SASL_PASSWORD"), DeliveryTimeout: 30 * time.Second}
	if v := getenv("ASTRYON_KAFKA_CLIENT_ID"); v != "" {
		c.ClientID = v
	}
	switch strings.ToLower(strings.TrimSpace(getenv("ASTRYON_KAFKA_TLS"))) {
	case "", "0", "false":
	case "1", "true":
		c.TLS = true
	default:
		return c, errors.New("ASTRYON_KAFKA_TLS must be 1/true or 0/false")
	}
	for _, b := range strings.Split(getenv("ASTRYON_KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.Brokers = append(c.Brokers, b)
		}
	}
	if len(c.Brokers) == 0 {
		return c, errors.New("ASTRYON_KAFKA_BROKERS is required")
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
			return c, errors.New("SASL mechanism requires ASTRYON_KAFKA_SASL_USERNAME and ASTRYON_KAFKA_SASL_PASSWORD")
		}
		if c.SASLMech == "PLAIN" && !c.TLS {
			return c, errors.New("SASL PLAIN requires ASTRYON_KAFKA_TLS=1")
		}
	default:
		return c, fmt.Errorf("unsupported ASTRYON_KAFKA_SASL_MECHANISM %q", c.SASLMech)
	}
	if c.TLSCAFile != "" && !c.TLS {
		return c, errors.New("ASTRYON_KAFKA_TLS_CA_FILE is set but ASTRYON_KAFKA_TLS is off (refusing plaintext)")
	}
	return c, nil
}

func (c Config) mechanism() sasl.Mechanism {
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

// Producer is the seam to the Kafka client: ProduceSync returns nil only when every record is acked.
type Producer interface {
	ProduceSync(ctx context.Context, recs ...*kgo.Record) error
	Close()
}

type franzProducer struct{ cl *kgo.Client }

func (p franzProducer) ProduceSync(ctx context.Context, recs ...*kgo.Record) error {
	return p.cl.ProduceSync(ctx, recs...).FirstErr()
}
func (p franzProducer) Close() { p.cl.Close() }

// Emitter implements runner.Emitter.
type Emitter struct {
	P       Producer
	Topic   string
	Timeout time.Duration
}

// New builds the franz-go producer (acks=all, idempotent). The broker need not be reachable yet.
func New(c Config) (*Emitter, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()), // idempotence stays on (franz-go default)
		kgo.RecordRetries(1 << 30), kgo.RecordDeliveryTimeout(c.DeliveryTimeout),
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<10, true, true, nil)),
	}
	if c.TLS {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if c.TLSCAFile != "" {
			pem, err := os.ReadFile(c.TLSCAFile) // #nosec G304 -- operator-supplied path from env
			if err != nil {
				return nil, err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("ASTRYON_KAFKA_TLS_CA_FILE has no valid certificates")
			}
			tc.RootCAs = pool
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	if m := c.mechanism(); m != nil {
		opts = append(opts, kgo.SASL(m))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Emitter{P: franzProducer{cl}, Topic: ec.TopicRawEvents, Timeout: c.DeliveryTimeout}, nil
}

func (e *Emitter) Close() { e.P.Close() }

// Emit publishes the page; nil only when every record was acked. The envelope's tenant must equal the trusted
// tenant argument (which comes from the claimed connector row), so a payload can never redirect data.
func (e *Emitter) Emit(ctx context.Context, tenant, source string, events [][]byte) error {
	recs := make([]*kgo.Record, 0, len(events))
	for i, raw := range events {
		var env ec.Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return fmt.Errorf("event %d: invalid envelope: %w", i, err)
		}
		if env.TenantID != tenant || env.SourceID != source {
			return fmt.Errorf("event %d: tenant/source mismatch with connector", i)
		}
		recs = append(recs, &kgo.Record{Topic: e.Topic, Key: env.PartitionKey(), Value: raw})
	}
	if len(recs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, e.Timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.P.ProduceSync(ctx, recs...) }() // franz-go may block past ctx while retrying metadata
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("kafka emit to %s: %w", e.Topic, err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("kafka emit to %s: %w", e.Topic, ctx.Err())
	}
}
