package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type collectEmit struct {
	mu  sync.Mutex
	ids []string
	ent []string
}

func (c *collectEmit) Emit(_ context.Context, _, _ string, evs [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range evs {
		var env ec.Envelope
		if err := json.Unmarshal(b, &env); err != nil {
			return err
		}
		c.ids = append(c.ids, env.EventID)
		c.ent = append(c.ent, env.EntityKey)
	}
	return nil
}

func createTopic(t *testing.T, ctx context.Context, cl *kgo.Client, topic string) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = topic, 1, 1
	req.Topics = append(req.Topics, ct)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Topics) > 0 && resp.Topics[0].ErrorCode != 0 && resp.Topics[0].ErrorCode != 36 { // 36 = already exists
		t.Fatalf("create topic %s: code %d", topic, resp.Topics[0].ErrorCode)
	}
}

// Live Kafka test; skipped unless ASTRYON_KAFKA_IT_BROKER (e.g. 127.0.0.1:19092) is set.
func TestLiveKafkaSourceQuarantineRestart(t *testing.T) {
	broker := os.Getenv("ASTRYON_KAFKA_IT_BROKER")
	if broker == "" {
		t.Skip("ASTRYON_KAFKA_IT_BROKER not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	stamp := time.Now().UnixNano()
	topic := fmt.Sprintf("it.cdc.pg.public.people.%d", stamp)
	group := fmt.Sprintf("it-cdc-%d", stamp)
	conn := []kgo.Opt{kgo.SeedBrokers(broker)}

	prod, err := kgo.NewClient(append(conn, kgo.RequiredAcks(kgo.AllISRAcks()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	createTopic(t, ctx, prod, topic)
	createTopic(t, ctx, prod, ec.TopicQuarantine)

	produce := func(i int, v []byte) {
		t.Helper()
		r := &kgo.Record{Topic: topic, Key: []byte(fmt.Sprintf(`{"id":%d}`, i)), Value: v}
		if err := prod.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	good := func(i int) []byte { return dbz("c", 100+i, i, "") }
	// offsets 0..6: offset 3 is malformed
	for i := 0; i < 7; i++ {
		if i == 3 {
			produce(i, []byte(`{"not":"debezium"`))
			continue
		}
		produce(i, good(i))
	}

	cfg := MapConfig{ConnectorID: "c1", Tenant: tenant, Source: source, SchemaID: "cdc.people", SchemaVersion: 1, ProcessingVersion: 1, PrimaryKey: []string{"id"}}
	em := &collectEmit{}
	q, err := NewKafkaQuarantine(conn)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// run drains until `want` records were handled in total by this consumer instance.
	run := func(src *KafkaSource, want int) int {
		t.Helper()
		c := &Consumer{Feed: src, Emitter: em, Quarantine: q, Map: cfg, MaxBatch: 2}
		got := 0
		for deadline := time.Now().Add(60 * time.Second); got < want && time.Now().Before(deadline); {
			n, err := c.RunOnce(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got += n
		}
		if got < want {
			t.Fatalf("handled %d records, want >= %d", got, want)
		}
		return got
	}

	src1, err := NewKafkaSource(conn, group, topic)
	if err != nil {
		t.Fatal(err)
	}
	n1 := run(src1, 4) // MaxBatch 2: at least two committed batches, remainder uncommitted
	src1.Close()       // simulated crash/restart

	for i := 7; i < 9; i++ {
		produce(i, good(i))
	}
	src2, err := NewKafkaSource(conn, group, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer src2.Close()
	n2 := run(src2, 9-n1)
	if n1+n2 != 9 {
		t.Fatalf("handled %d+%d records, want exactly 9 (no duplicate/loss)", n1, n2)
	}
	// nothing further: committed offsets reached the end
	c := &Consumer{Feed: src2, Emitter: em, Quarantine: q, Map: cfg}
	if n, err := c.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("extra records after drain: n=%d err=%v", n, err)
	}

	seen := map[string]bool{}
	for _, id := range em.ids {
		if seen[id] {
			t.Fatalf("duplicate event id %s", id)
		}
		seen[id] = true
	}
	if len(em.ids) != 8 {
		t.Fatalf("emitted %d envelopes, want 8 (entities %v)", len(em.ids), em.ent)
	}
	for i := 0; i < 9; i++ {
		if i == 3 {
			continue
		}
		found := false
		for _, e := range em.ent {
			found = found || e == fmt.Sprint(i)
		}
		if !found {
			t.Fatalf("entity %d missing from emitted events %v", i, em.ent)
		}
	}

	// quarantine record for offset 3 on the quarantine topic
	qc, err := kgo.NewClient(append(conn, kgo.ConsumeTopics(ec.TopicQuarantine), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer qc.Close()
	var found *ec.QuarantineRecordV1
	for deadline := time.Now().Add(30 * time.Second); found == nil && time.Now().Before(deadline); {
		pctx, pc := context.WithTimeout(ctx, 3*time.Second)
		qc.PollFetches(pctx).EachRecord(func(r *kgo.Record) {
			var rec ec.QuarantineRecordV1
			if json.Unmarshal(r.Value, &rec) == nil && rec.Provenance.SourceTopic == topic {
				found = &rec
			}
		})
		pc()
	}
	if found == nil {
		t.Fatal("quarantine record not found")
	}
	if found.TenantID != tenant || found.SourceID != source || found.ReasonCode != "CDC_MALFORMED_EVENT" ||
		found.Provenance.SourceOffset == nil || *found.Provenance.SourceOffset != 3 {
		t.Fatalf("unexpected quarantine record: %+v", found)
	}
}
