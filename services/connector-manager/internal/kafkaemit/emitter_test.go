package kafkaemit

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fake struct {
	recs  []*kgo.Record
	err   error
	block bool
}

func (f *fake) ProduceSync(ctx context.Context, r ...*kgo.Record) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	f.recs = append(f.recs, r...)
	return f.err
}
func (f *fake) Close() {}

func ev(t *testing.T, tenant, id, entity string) []byte {
	b, err := json.Marshal(ec.Envelope{EventID: id, TenantID: tenant, SourceID: "s1", EntityKey: entity})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestEmitKeysAndTopic(t *testing.T) {
	f := &fake{}
	e := &Emitter{P: f, Topic: ec.TopicRawEvents, Timeout: time.Second}
	if err := e.Emit(context.Background(), "t1", "s1", [][]byte{ev(t, "t1", "e1", "ent"), ev(t, "t1", "e2", "")}); err != nil {
		t.Fatal(err)
	}
	if len(f.recs) != 2 || f.recs[0].Topic != "raw.events.v1" {
		t.Fatalf("%+v", f.recs)
	}
	if string(f.recs[0].Key) != string(ec.PartitionKey("t1", "ent")) || string(f.recs[1].Key) != string(ec.PartitionKey("t1", "e2")) {
		t.Fatalf("keys %q %q", f.recs[0].Key, f.recs[1].Key)
	}
}

func TestEmitRejectsForeignTenantAndPropagatesErrors(t *testing.T) {
	f := &fake{}
	e := &Emitter{P: f, Topic: "x", Timeout: time.Second}
	if err := e.Emit(context.Background(), "t1", "s1", [][]byte{ev(t, "t2", "e1", "")}); err == nil || len(f.recs) != 0 {
		t.Fatalf("foreign tenant must be refused, err=%v", err)
	}
	if err := e.Emit(context.Background(), "t1", "s1", [][]byte{[]byte("{")}); err == nil {
		t.Fatal("invalid envelope must fail")
	}
	f.err = errors.New("boom")
	if err := e.Emit(context.Background(), "t1", "s1", [][]byte{ev(t, "t1", "e1", "")}); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("got %v", err)
	}
}

func TestEmitBoundedByTimeout(t *testing.T) {
	e := &Emitter{P: &fake{block: true}, Topic: "x", Timeout: 50 * time.Millisecond}
	start := time.Now()
	err := e.Emit(context.Background(), "t1", "s1", [][]byte{ev(t, "t1", "e1", "")})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("err=%v", err)
	}
}

func TestLoadConfig(t *testing.T) {
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if _, err := LoadConfig(get(nil)); err == nil {
		t.Fatal("brokers required")
	}
	c, err := LoadConfig(get(map[string]string{"ASTRYON_KAFKA_BROKERS": "a:9092, b:9092", "ASTRYON_KAFKA_DELIVERY_TIMEOUT": "5s"}))
	if err != nil || len(c.Brokers) != 2 || c.DeliveryTimeout != 5*time.Second {
		t.Fatalf("%+v %v", c, err)
	}
	bad := []map[string]string{
		{"ASTRYON_KAFKA_BROKERS": "a:1", "ASTRYON_KAFKA_TLS": "yes"},
		{"ASTRYON_KAFKA_BROKERS": "a:1", "ASTRYON_KAFKA_TLS_CA_FILE": "/x"},
		{"ASTRYON_KAFKA_BROKERS": "a:1", "ASTRYON_KAFKA_SASL_MECHANISM": "PLAIN", "ASTRYON_KAFKA_SASL_USERNAME": "u", "ASTRYON_KAFKA_SASL_PASSWORD": "p"},
		{"ASTRYON_KAFKA_BROKERS": "a:1", "ASTRYON_KAFKA_DELIVERY_TIMEOUT": "-1s"},
	}
	for i, m := range bad {
		if _, err := LoadConfig(get(m)); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}
