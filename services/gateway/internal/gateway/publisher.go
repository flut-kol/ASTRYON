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
