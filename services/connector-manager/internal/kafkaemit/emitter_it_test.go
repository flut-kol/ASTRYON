package kafkaemit

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Live Kafka test; skipped unless ASTRYON_KAFKA_IT_BROKER (e.g. 127.0.0.1:19092) is set.
func TestLiveKafkaEmit(t *testing.T) {
	broker := os.Getenv("ASTRYON_KAFKA_IT_BROKER")
	if broker == "" {
		t.Skip("ASTRYON_KAFKA_IT_BROKER not set")
	}
	topic := fmt.Sprintf("it.connector-manager.raw.%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	creq := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = topic, 1, 1
	creq.Topics = append(creq.Topics, ct)
	cresp, err := creq.RequestWith(ctx, admin)
	if err != nil || (len(cresp.Topics) > 0 && cresp.Topics[0].ErrorCode != 0) {
		t.Fatalf("create topic: %v %+v", err, cresp)
	}
	defer func() {
		dreq := kmsg.NewPtrDeleteTopicsRequest()
		dreq.TopicNames = []string{topic}
		dt := kmsg.NewDeleteTopicsRequestTopic()
		dt.Topic = &topic
		dreq.Topics = append(dreq.Topics, dt)
		_, _ = dreq.RequestWith(context.Background(), admin)
	}()

	em, err := New(Config{Brokers: []string{broker}, ClientID: "it-connector-manager", DeliveryTimeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer em.Close()
	em.Topic = topic

	mk := func(tenant, id, entity string) []byte {
		b, err := json.Marshal(ec.Envelope{EventID: id, TenantID: tenant, SourceID: "s1", EntityKey: entity,
			PayloadFormat: ec.PayloadFormat("json"), Payload: json.RawMessage(`{"n":"` + id + `"}`)})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	events := [][]byte{mk("t1", "e1", "ent-a"), mk("t1", "e2", "ent-b")}
	if err := em.Emit(ctx, "t1", "s1", events); err != nil {
		t.Fatalf("emit: %v", err)
	}
	// foreign tenant is rejected before anything is produced
	if err := em.Emit(ctx, "t1", "s1", [][]byte{mk("t2", "evil", "x")}); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("foreign tenant not rejected: %v", err)
	}

	cons, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	got := map[string]*kgo.Record{}
	deadline := time.Now().Add(30 * time.Second)
	for len(got) < 2 && time.Now().Before(deadline) {
		pctx, pc := context.WithTimeout(ctx, 5*time.Second)
		fetches := cons.PollFetches(pctx)
		pc()
		fetches.EachRecord(func(r *kgo.Record) {
			var env ec.Envelope
			if json.Unmarshal(r.Value, &env) == nil {
				got[env.EventID] = r
			}
		})
	}
	if _, bad := got["evil"]; bad || len(got) != 2 {
		t.Fatalf("consumed %d records (evil present=%v), want exactly e1,e2", len(got), bad)
	}
	for i, id := range []string{"e1", "e2"} {
		r := got[id]
		if r == nil {
			t.Fatalf("missing %s", id)
		}
		var env, want ec.Envelope
		_ = json.Unmarshal(r.Value, &env)
		_ = json.Unmarshal(events[i], &want)
		if env.TenantID != "t1" || string(env.Payload) != string(want.Payload) {
			t.Fatalf("%s: tenant/payload mismatch: %+v", id, env)
		}
		if string(r.Key) != string(want.PartitionKey()) {
			t.Fatalf("%s: key %q want %q", id, r.Key, want.PartitionKey())
		}
	}
}
