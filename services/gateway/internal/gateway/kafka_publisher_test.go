package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

type stubProducer struct {
	err       error
	topic     string
	key       []byte
	closed    bool
	failTopic string
}

func (s *stubProducer) produce(_ context.Context, topic string, key, _ []byte) error {
	s.topic, s.key = topic, key
	return s.err
}
func (s *stubProducer) produceBatch(_ context.Context, msgs []Message) []error {
	errs := make([]error, len(msgs))
	for i, m := range msgs {
		errs[i] = s.err
		if s.failTopic != "" && m.Topic == s.failTopic {
			errs[i] = errors.New("no ack")
		}
	}
	return errs
}
func (s *stubProducer) close() { s.closed = true }

func TestKafkaPublisherAckSemantics(t *testing.T) {
	sp := &stubProducer{}
	k := &KafkaPublisher{p: sp}
	if err := k.Publish(context.Background(), "t", []byte("k"), []byte("v")); err != nil || sp.topic != "t" || string(sp.key) != "k" {
		t.Fatalf("ack path: err=%v topic=%q", err, sp.topic)
	}
	boom := errors.New("no ack")
	sp.err = boom
	if err := k.Publish(context.Background(), "t", nil, nil); !errors.Is(err, boom) {
		t.Fatalf("want wrapped no-ack error, got %v", err)
	}
	k.Close()
	if !sp.closed {
		t.Fatal("Close did not close producer")
	}
}

func TestLoadKafkaConfig(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if _, err := LoadKafkaConfig(env(nil)); err == nil {
		t.Fatal("brokers required")
	}
	c, err := LoadKafkaConfig(env(map[string]string{"ASTRYON_KAFKA_BROKERS": " a:1, b:2 ,", "ASTRYON_KAFKA_DELIVERY_TIMEOUT": "5s"}))
	if err != nil || len(c.Brokers) != 2 || c.Brokers[1] != "b:2" || c.ClientID != "astryon-gateway" || c.DeliveryTimeout != 5*time.Second {
		t.Fatalf("%+v %v", c, err)
	}
	if _, err := LoadKafkaConfig(env(map[string]string{"ASTRYON_KAFKA_BROKERS": "a:1", "ASTRYON_KAFKA_SASL_MECHANISM": "PLAIN"})); err == nil {
		t.Fatal("sasl creds required")
	}
	if _, err := LoadKafkaConfig(env(map[string]string{"ASTRYON_KAFKA_BROKERS": "a:1", "ASTRYON_KAFKA_SASL_MECHANISM": "X"})); err == nil {
		t.Fatal("bad mechanism")
	}
	c, err = LoadKafkaConfig(env(map[string]string{"ASTRYON_KAFKA_BROKERS": "a:1", "ASTRYON_KAFKA_SASL_MECHANISM": "scram-sha-512", "ASTRYON_KAFKA_SASL_USERNAME": "u", "ASTRYON_KAFKA_SASL_PASSWORD": "p"}))
	if err != nil || c.saslMechanism() == nil {
		t.Fatalf("%+v %v", c, err)
	}
}
