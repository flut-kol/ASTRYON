package retryclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Producer is the seam to the Kafka client (fake in tests).
type Producer interface {
	ProduceSync(ctx context.Context, recs ...*kgo.Record) error
	Close()
}

// KafkaPublisher publishes Messages keyed by tenant/job so one job's retries stay ordered.
type KafkaPublisher struct {
	P       Producer
	Topic   string
	Timeout time.Duration
}

func (k KafkaPublisher) Publish(ctx context.Context, m Message) error {
	b, err := EncodeMessage(m)
	if err != nil {
		return err
	}
	if k.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, k.Timeout)
		defer cancel()
	}
	return k.P.ProduceSync(ctx, &kgo.Record{Topic: k.Topic, Key: []byte(m.TenantID + "/" + m.JobID), Value: b,
		Headers: []kgo.RecordHeader{{Key: "attempt", Value: []byte(strconv.Itoa(m.Attempt))}}})
}

func (k KafkaPublisher) Close() { k.P.Close() }

type franzProducer struct{ cl *kgo.Client }

func (p franzProducer) ProduceSync(ctx context.Context, recs ...*kgo.Record) error {
	return p.cl.ProduceSync(ctx, recs...).FirstErr()
}
func (p franzProducer) Close() { p.cl.Close() }

// NewKafkaPublisher builds a franz-go publisher (acks=all, idempotent) from the shared
// ASTRYON_KAFKA_* env (same names as the gateway). The topic comes from the caller.
func NewKafkaPublisher(getenv func(string) string, topic string) (KafkaPublisher, error) {
	if topic == "" {
		return KafkaPublisher{}, errors.New("retryclient: topic required")
	}
	var brokers []string
	for _, b := range strings.Split(getenv("ASTRYON_KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return KafkaPublisher{}, errors.New("ASTRYON_KAFKA_BROKERS is required")
	}
	clientID := "astryon-retry-controller"
	if v := getenv("ASTRYON_KAFKA_CLIENT_ID"); v != "" {
		clientID = v
	}
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...), kgo.ClientID(clientID), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(30 * time.Second)}
	useTLS := false
	switch strings.ToLower(strings.TrimSpace(getenv("ASTRYON_KAFKA_TLS"))) {
	case "", "0", "false":
	case "1", "true":
		useTLS = true
	default:
		return KafkaPublisher{}, errors.New("ASTRYON_KAFKA_TLS must be 1/true or 0/false")
	}
	ca := getenv("ASTRYON_KAFKA_TLS_CA_FILE")
	if ca != "" && !useTLS {
		return KafkaPublisher{}, errors.New("ASTRYON_KAFKA_TLS_CA_FILE is set but ASTRYON_KAFKA_TLS is off (refusing plaintext)")
	}
	if useTLS {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if ca != "" {
			pem, err := os.ReadFile(ca) // #nosec G304 -- operator-supplied path from env
			if err != nil {
				return KafkaPublisher{}, err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return KafkaPublisher{}, errors.New("ASTRYON_KAFKA_TLS_CA_FILE has no valid certificates")
			}
			tc.RootCAs = pool
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	user, pass := getenv("ASTRYON_KAFKA_SASL_USERNAME"), getenv("ASTRYON_KAFKA_SASL_PASSWORD")
	var mech sasl.Mechanism
	switch m := strings.ToUpper(strings.TrimSpace(getenv("ASTRYON_KAFKA_SASL_MECHANISM"))); m {
	case "":
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if user == "" || pass == "" {
			return KafkaPublisher{}, errors.New("SASL mechanism requires ASTRYON_KAFKA_SASL_USERNAME and ASTRYON_KAFKA_SASL_PASSWORD")
		}
		switch m {
		case "PLAIN":
			if !useTLS {
				return KafkaPublisher{}, errors.New("SASL PLAIN requires ASTRYON_KAFKA_TLS=1")
			}
			mech = plain.Auth{User: user, Pass: pass}.AsMechanism()
		case "SCRAM-SHA-256":
			mech = scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()
		default:
			mech = scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()
		}
		opts = append(opts, kgo.SASL(mech))
	default:
		return KafkaPublisher{}, fmt.Errorf("unsupported ASTRYON_KAFKA_SASL_MECHANISM %q", m)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return KafkaPublisher{}, fmt.Errorf("kafka client: %w", err)
	}
	return KafkaPublisher{P: franzProducer{cl}, Topic: topic, Timeout: 30 * time.Second}, nil
}
