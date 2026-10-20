package gateway

// Real-broker TLS+SASL exercise for KafkaPublisher. Unlike kafka_publisher_test.go
// (config parsing + a stub producer, no network), this drives a real franz-go
// client against a real Kafka broker with the dev TLS/SASL listener enabled
// (infra/docker/docker-compose.dev.yml service "kafka", listener DEVTLS on
// 127.0.0.1:19093, protocol SASL_SSL, mechanism SCRAM-SHA-512) and an
// independent plain franz-go client to confirm the published message actually
// round-trips over the encrypted, authenticated connection - not just
// "NewKafkaPublisher/Publish returned no error".
//
// Gated by ASTRYON_KAFKA_IT_TLS_BROKER so `go test ./...` is a no-op without the
// dev stack (same convention as services/clickhouse-sink/internal/sink/realinfra_test.go).
//
// Run with (from repo root):
//   1. Generate the dev CA + broker cert, bring up the stack, and provision the
//      SCRAM user (the last two steps also happen automatically as part of
//      `make dev-up`; run them by hand only to regenerate without a full
//      teardown):
//        ./scripts/dev/gen-kafka-tls.sh
//        make dev-up
//        ./scripts/dev/provision-kafka-scram.sh
//   2. Export the env vars and run the test:
//        ASTRYON_KAFKA_IT_TLS_BROKER=127.0.0.1:19093 \
//        ASTRYON_KAFKA_IT_TLS_CA_FILE="$(pwd)/infra/docker/.devcerts/kafka/ca.crt" \
//        ASTRYON_KAFKA_IT_SASL_USER=astryon_dev \
//        ASTRYON_KAFKA_IT_SASL_PASS=astryon_dev_only \
//        go test ./internal/gateway/ -run TestKafkaPublisherRealBrokerTLSSASL -v
//
// Uses a fresh throwaway topic per run, so it is safe to re-run against the dev stack.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaPublisherRealBrokerTLSSASL(t *testing.T) {
	broker := os.Getenv("ASTRYON_KAFKA_IT_TLS_BROKER")
	if broker == "" {
		t.Skip("ASTRYON_KAFKA_IT_TLS_BROKER not set")
	}
	caFile := os.Getenv("ASTRYON_KAFKA_IT_TLS_CA_FILE")
	if caFile == "" {
		t.Fatal("ASTRYON_KAFKA_IT_TLS_CA_FILE must be set (path to the dev CA cert, see scripts/dev/gen-kafka-tls.sh)")
	}
	user := envOr(os.Getenv("ASTRYON_KAFKA_IT_SASL_USER"), "astryon_dev")
	pass := envOr(os.Getenv("ASTRYON_KAFKA_IT_SASL_PASS"), "astryon_dev_only")

	// --- Build the TLS+SASL dial options once (shared helper, same inputs the
	// gateway's own KafkaConfig would use) and an admin client to create a
	// throwaway topic (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false in the dev stack).
	opts, err := realTLSSASLOpts(caFile, "SCRAM-SHA-512", user, pass)
	if err != nil {
		t.Fatalf("build TLS+SASL opts: %v", err)
	}
	admin, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(broker)}, opts...)...)
	if err != nil {
		t.Fatalf("admin client over TLS+SASL: %v", err)
	}
	defer admin.Close()

	topic := fmt.Sprintf("gateway.it.tls.%d", time.Now().UnixNano())
	if err := createITTopic(admin, topic); err != nil {
		t.Fatalf("create topic over TLS+SASL: %v", err)
	}

	// --- The actual code under test: KafkaConfig -> NewKafkaPublisher -> Publish.
	cfg := KafkaConfig{
		Brokers:         []string{broker},
		ClientID:        "astryon-gateway-it",
		TLS:             true,
		TLSCAFile:       caFile,
		SASLMech:        "SCRAM-SHA-512",
		SASLUser:        user,
		SASLPass:        pass,
		DeliveryTimeout: 15 * time.Second,
	}
	pub, err := NewKafkaPublisher(cfg)
	if err != nil {
		t.Fatalf("NewKafkaPublisher over TLS+SASL: %v", err)
	}
	defer pub.Close()

	key := []byte("tls-sasl-key")
	value := []byte(fmt.Sprintf("tls-sasl-roundtrip-%d", time.Now().UnixNano()))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Publish() blocks for a durable broker ack (acks=all). If the TLS
	// handshake or SASL auth is broken, this is where it fails.
	if err := pub.Publish(ctx, topic, key, value); err != nil {
		t.Fatalf("Publish over TLS+SASL: %v", err)
	}

	// --- Independently consume the record back with a SEPARATE plain
	// franz-go client (no KafkaPublisher involved) authenticating with the
	// same TLS+SASL settings, to prove the message actually landed on the
	// broker over the encrypted/authenticated connection, not merely that
	// the local Publish call returned without error.
	consumerOpts := append([]kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, opts...)
	consumer, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		t.Fatalf("build verification consumer: %v", err)
	}
	defer consumer.Close()

	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("consume fetch errors: %v", errs)
	}
	var got, gotKey []byte
	fetches.EachRecord(func(r *kgo.Record) {
		got = r.Value
		gotKey = r.Key
	})
	if string(got) != string(value) {
		t.Fatalf("round-trip value mismatch: got %q want %q", got, value)
	}
	if string(gotKey) != string(key) {
		t.Fatalf("round-trip key mismatch: got %q want %q", gotKey, key)
	}
}

// realTLSSASLOpts builds the franz-go TLS+SASL dial options directly (not via
// NewKafkaPublisher) so the verification admin/consumer clients in this test
// are independent of the KafkaPublisher code path under test.
func realTLSSASLOpts(caFile, mech, user, pass string) ([]kgo.Opt, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA file %s: no valid PEM certificates", caFile)
	}
	tc.RootCAs = pool
	c := KafkaConfig{SASLMech: mech, SASLUser: user, SASLPass: pass}
	return []kgo.Opt{kgo.DialTLSConfig(tc), kgo.SASL(c.saslMechanism())}, nil
}

// createITTopic creates a throwaway single-partition topic via the admin API,
// tolerating "already exists" so the test is safe to re-run.
func createITTopic(cl *kgo.Client, topic string) error {
	req := kmsg.NewCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = 1
	rt.ReplicationFactor = 1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		return err
	}
	for _, t := range resp.Topics {
		if t.ErrorCode != 0 && t.ErrorCode != 36 { // 36 = TOPIC_ALREADY_EXISTS
			return fmt.Errorf("create topic %s: code %d", topic, t.ErrorCode)
		}
	}
	return nil
}

func envOr(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
