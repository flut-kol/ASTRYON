package replay

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	TopicQuarantineReplayJobs = "quarantine.replay.jobs.v1"
	TopicAudit                = "audit.events.v1"
)

// KafkaConfig follows the repository-wide ASTRYON_KAFKA_* producer contract.
// TLS/SASL errors fail startup rather than silently falling back to plaintext.
type KafkaConfig struct {
	Brokers         []string
	ClientID        string
	TLS             bool
	TLSCAFile       string
	TLSCertFile     string
	TLSKeyFile      string
	SASLMechanism   string
	SASLUsername    string
	SASLPassword    string
	DeliveryTimeout time.Duration
	ReplayTopic     string
}

func LoadKafkaConfig(getenv func(string) string) (KafkaConfig, error) {
	c := KafkaConfig{
		ClientID: "astryon-quarantine", TLSCAFile: getenv("ASTRYON_KAFKA_TLS_CA_FILE"),
		TLSCertFile: getenv("ASTRYON_KAFKA_TLS_CERT_FILE"), TLSKeyFile: getenv("ASTRYON_KAFKA_TLS_KEY_FILE"),
		SASLMechanism: strings.ToUpper(strings.TrimSpace(getenv("ASTRYON_KAFKA_SASL_MECHANISM"))),
		SASLUsername:  getenv("ASTRYON_KAFKA_SASL_USERNAME"), SASLPassword: getenv("ASTRYON_KAFKA_SASL_PASSWORD"),
		DeliveryTimeout: 30 * time.Second, ReplayTopic: TopicQuarantineReplayJobs,
	}
	for _, broker := range strings.Split(getenv("ASTRYON_KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			c.Brokers = append(c.Brokers, broker)
		}
	}
	if len(c.Brokers) == 0 {
		return c, errors.New("ASTRYON_KAFKA_BROKERS is required")
	}
	if v := getenv("ASTRYON_KAFKA_CLIENT_ID"); v != "" {
		c.ClientID = v
	}
	if v := getenv("QUARANTINE_REPLAY_TOPIC"); v != "" {
		c.ReplayTopic = v
	}
	if !kafkaTopic.MatchString(c.ReplayTopic) || c.ReplayTopic == "." || c.ReplayTopic == ".." {
		return c, errors.New("QUARANTINE_REPLAY_TOPIC is invalid")
	}
	var err error
	if c.TLS, err = parseKafkaBool(getenv("ASTRYON_KAFKA_TLS")); err != nil {
		return c, err
	}
	if v := getenv("ASTRYON_KAFKA_DELIVERY_TIMEOUT"); v != "" {
		if c.DeliveryTimeout, err = time.ParseDuration(v); err != nil || c.DeliveryTimeout <= 0 {
			return c, errors.New("ASTRYON_KAFKA_DELIVERY_TIMEOUT must be a positive duration")
		}
	}
	if (c.TLSCAFile != "" || c.TLSCertFile != "" || c.TLSKeyFile != "") && !c.TLS {
		return c, errors.New("Kafka TLS material configured while ASTRYON_KAFKA_TLS is off")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return c, errors.New("ASTRYON_KAFKA_TLS_CERT_FILE and ASTRYON_KAFKA_TLS_KEY_FILE must be set together")
	}
	switch c.SASLMechanism {
	case "":
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if c.SASLUsername == "" || c.SASLPassword == "" {
			return c, errors.New("Kafka SASL mechanism requires username and password")
		}
	default:
		return c, fmt.Errorf("unsupported Kafka SASL mechanism %q", c.SASLMechanism)
	}
	if c.SASLMechanism == "PLAIN" && !c.TLS {
		return c, errors.New("Kafka PLAIN requires TLS")
	}
	return c, nil
}

var kafkaTopic = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

func parseKafkaBool(v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "0", "false":
		return false, nil
	case "1", "true":
		return true, nil
	default:
		return false, fmt.Errorf("ASTRYON_KAFKA_TLS must be 1/true or 0/false, got %q", v)
	}
}

type kafkaRecordProducer interface {
	Produce(context.Context, string, []byte, []byte) error
	Close()
}

type franzRecordProducer struct {
	client  *kgo.Client
	timeout time.Duration
}

func (p *franzRecordProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- p.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *franzRecordProducer) Close() { p.client.Close() }

type KafkaPublisher struct {
	producer    kafkaRecordProducer
	replayTopic string
}

func NewKafkaPublisher(c KafkaConfig) (*KafkaPublisher, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.ClientID), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(1 << 30), kgo.RecordDeliveryTimeout(c.DeliveryTimeout), kgo.ProduceRequestTimeout(10 * time.Second),
	}
	if c.TLS {
		tlsConfig, err := loadKafkaTLS(c)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if mechanism := kafkaSASL(c); mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("Kafka client: %w", err)
	}
	return &KafkaPublisher{producer: &franzRecordProducer{client: client, timeout: c.DeliveryTimeout}, replayTopic: c.ReplayTopic}, nil
}

func loadKafkaTLS(c KafkaConfig) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.TLSCAFile != "" {
		pem, err := os.ReadFile(c.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("Kafka CA: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("Kafka CA contains no valid PEM certificates")
		}
		config.RootCAs = pool
	}
	if c.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("Kafka client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}
	return config, nil
}

func kafkaSASL(c KafkaConfig) sasl.Mechanism {
	switch c.SASLMechanism {
	case "PLAIN":
		return plain.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsMechanism()
	case "SCRAM-SHA-256":
		return scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha256Mechanism()
	case "SCRAM-SHA-512":
		return scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha512Mechanism()
	default:
		return nil
	}
}

func (p *KafkaPublisher) PublishReplayJob(ctx context.Context, job ReplayJob) error {
	if err := job.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err := p.producer.Produce(ctx, p.replayTopic, []byte(job.TenantID+"\x1f"+job.SourceID), body); err != nil {
		return fmt.Errorf("Kafka replay publish: %w", err)
	}
	return nil
}

func (p *KafkaPublisher) RecordReplayAudit(ctx context.Context, event ReplayAuditEvent) error {
	if event.EventID == "" || !tenantUUID.MatchString(event.TenantID) || event.Action == "" {
		return errors.New("invalid replay audit event")
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := p.producer.Produce(ctx, TopicAudit, []byte(event.TenantID), body); err != nil {
		return fmt.Errorf("Kafka audit publish: %w", err)
	}
	return nil
}

func (p *KafkaPublisher) Close() { p.producer.Close() }

var _ ReplayJobPublisher = (*KafkaPublisher)(nil)
var _ ReplayAuditSink = (*KafkaPublisher)(nil)
