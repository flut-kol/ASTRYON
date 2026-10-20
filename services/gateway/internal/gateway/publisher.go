package gateway

import (
	"context"
	"sync"
)

// Publisher publishes one record and returns nil ONLY after durable broker
// acknowledgement (Kafka acks=all, idempotent producer).
//
// Production implementation: KafkaPublisher (kafka_publisher.go, franz-go).
type Publisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
}

// Message is one record for BatchPublisher.
type Message struct {
	Topic      string
	Key, Value []byte
}

// BatchPublisher is an optional Publisher extension: PublishBatch publishes
// all messages concurrently (one pipelined produce) and returns a slice of the
// same length and order; errs[i]==nil ONLY after durable broker ack of msgs[i].
// Per-message failures are independent. Ordering within one partition follows
// the order of msgs.
type BatchPublisher interface {
	PublishBatch(ctx context.Context, msgs []Message) []error
}

// Record is a message captured by FakePublisher.
type Record struct {
	Topic      string
	Key, Value []byte
}

// FakePublisher is an in-memory Publisher for tests and dev.
type FakePublisher struct {
	mu      sync.Mutex
	records []Record
	// Fail, if set, is consulted per publish; non-nil error means no ack.
	Fail func(topic string, value []byte) error
}

func (f *FakePublisher) Publish(_ context.Context, topic string, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Fail != nil {
		if err := f.Fail(topic, value); err != nil {
			return err
		}
	}
	f.records = append(f.records, Record{topic, append([]byte(nil), key...), append([]byte(nil), value...)})
	return nil
}

// Records returns a copy of acknowledged records.
func (f *FakePublisher) Records() []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Record(nil), f.records...)
}

// ByTopic returns acknowledged records for topic.
func (f *FakePublisher) ByTopic(topic string) []Record {
	var out []Record
	for _, r := range f.Records() {
		if r.Topic == topic {
			out = append(out, r)
		}
	}
	return out
}

// PublishBatch implements BatchPublisher by looping Publish.
func (f *FakePublisher) PublishBatch(ctx context.Context, msgs []Message) []error {
	errs := make([]error, len(msgs))
	for i, m := range msgs {
		errs[i] = f.Publish(ctx, m.Topic, m.Key, m.Value)
	}
	return errs
}

var _ BatchPublisher = (*FakePublisher)(nil)
