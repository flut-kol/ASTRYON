package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A produce call that never returns (hung broker) must not hold Publish past ctx.
func TestPublishBoundedByCtx_HungProducer(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	s := &KafkaSink{conn: &connErrs{}, produce: func(ctx context.Context, _ []*kgo.Record) error {
		<-release // ignores ctx on purpose
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := s.Publish(ctx, []Record{{Topic: "t", Key: []byte("k"), Value: []byte("v")}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Publish took %v, not bounded by ctx", d)
	}
}

// Real client, unreachable broker, long delivery timeout: Publish still returns at ctx.
func TestPublishBoundedByCtx_UnreachableBroker(t *testing.T) {
	s, err := NewKafkaSink(KafkaConfig{Brokers: []string{"127.0.0.1:1"}, ClientID: "t", DeliveryTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = s.Publish(ctx, []Record{{Topic: "t", Key: []byte("k"), Value: []byte("v")}})
	if err == nil {
		t.Fatal("expected an error")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("Publish took %v, not bounded by ctx", d)
	}
}

func TestPublishReturnsProducerError(t *testing.T) {
	want := errors.New("boom")
	s := &KafkaSink{conn: &connErrs{}, produce: func(context.Context, []*kgo.Record) error { return want }}
	if err := s.Publish(context.Background(), []Record{{Topic: "t"}}); !errors.Is(err, want) {
		t.Fatalf("got %v", err)
	}
}
