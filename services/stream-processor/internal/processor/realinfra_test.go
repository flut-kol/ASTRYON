package processor

// Real-Kafka end-to-end test, gated like the other services' integration tests.
//   STREAMPROC_IT_KAFKA=127.0.0.1:19092 go test ./internal/processor/ -run TestRealKafka -v
// Uses fresh throwaway topics/groups per run (dev stack from infra/docker).

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func createTopics(t *testing.T, cl *kgo.Client, names ...string) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	for _, n := range names {
		tp := kmsg.NewCreateTopicsRequestTopic()
		tp.Topic, tp.NumPartitions, tp.ReplicationFactor = n, 3, 1
		req.Topics = append(req.Topics, tp)
	}
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resp.Topics {
		if r.ErrorCode != 0 {
			t.Fatalf("create %s: %v", r.Topic, r.ErrorMessage)
		}
	}
}

func consumeN(t *testing.T, brokers []string, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var out []*kgo.Record
	deadline := time.Now().Add(20 * time.Second)
	for len(out) < n && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) { out = append(out, r) })
		cancel()
	}
	return out
}

func runFor(t *testing.T, cfg Config, win *Window, d time.Duration) *Pipeline {
	k, err := NewKafka(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(k, k, win, cfg.Options())
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRealKafkaEndToEnd(t *testing.T) {
	broker := os.Getenv("STREAMPROC_IT_KAFKA")
	if broker == "" {
		t.Skip("STREAMPROC_IT_KAFKA not set")
	}
	sfx := fmt.Sprint(time.Now().UnixNano())
	cfg := Config{Brokers: []string{broker}, GroupID: "sp-it-" + sfx, InputTopic: "sp-it-raw-" + sfx,
		OutputTopic: "sp-it-out-" + sfx, QuarantineTopic: "sp-it-quar-" + sfx, DLQTopic: "sp-it-dlq-" + sfx,
		PipelineVersion: 1, BatchSize: 100, BatchInterval: 200 * time.Millisecond, DedupTTL: time.Hour,
		DedupMaxEntries: 1000, MaxRecordBytes: 1 << 20, RetryBackoff: 100 * time.Millisecond, RetryBackoffMax: time.Second}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	admin, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	createTopics(t, admin, cfg.InputTopic, cfg.OutputTopic, cfg.QuarantineTopic, cfg.DLQTopic)

	// Fixtures use the real clock (the processor validates against time.Now()).
	nowSave := now
	now = time.Now().UTC()
	defer func() { now = nowSave }()
	ids := []string{
		"aaaaaaaa-aaaa-4aaa-8aaa-000000000001", "aaaaaaaa-aaaa-4aaa-8aaa-000000000002", "aaaaaaaa-aaaa-4aaa-8aaa-000000000003",
	}
	var recs []*kgo.Record
	for _, id := range ids {
		recs = append(recs, &kgo.Record{Topic: cfg.InputTopic, Key: []byte(id), Value: evJSON(t, id, nil)})
	}
	recs = append(recs,
		&kgo.Record{Topic: cfg.InputTopic, Key: []byte(ids[0]), Value: evJSON(t, ids[0], nil)}, // duplicate
		&kgo.Record{Topic: cfg.InputTopic, Key: []byte("bad"), Value: []byte("{not json")},     // quarantine
	)
	if err := admin.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	win := NewWindow(cfg.DedupTTL, cfg.DedupMaxEntries)
	p := runFor(t, cfg, win, 6*time.Second)
	out := consumeN(t, cfg.Brokers, cfg.OutputTopic, 3)
	if len(out) != 3 {
		t.Fatalf("processed=%d want 3 (dup dropped)", len(out))
	}
	for _, r := range out {
		if w := WarmKey(r.Value); w == "" {
			t.Fatalf("unparseable output %s", r.Value)
		}
		if len(r.Key) == 0 {
			t.Fatal("missing partition key")
		}
	}
	if q := consumeN(t, cfg.Brokers, cfg.QuarantineTopic, 1); len(q) != 1 {
		t.Fatalf("quarantine=%d", len(q))
	}
	if p.M.Duplicates.Load() != 1 || win.Stats().Entries != 3 {
		t.Fatalf("dups=%d entries=%d", p.M.Duplicates.Load(), win.Stats().Entries)
	}

	// Restart with an empty window: warm start from the processed topic restores
	// state, so a re-submitted duplicate is still dropped.
	win2 := NewWindow(cfg.DedupTTL, cfg.DedupMaxEntries)
	n, err := WarmWindow(context.Background(), cfg, win2, 3*time.Second)
	if err != nil || n != 3 || win2.Stats().Entries != 3 {
		t.Fatalf("warm n=%d err=%v entries=%d", n, err, win2.Stats().Entries)
	}
	if err := admin.ProduceSync(context.Background(), &kgo.Record{Topic: cfg.InputTopic, Key: []byte(ids[1]), Value: evJSON(t, ids[1], nil)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	p2 := runFor(t, cfg, win2, 4*time.Second)
	if p2.M.Duplicates.Load() != 1 || p2.M.Out.Load() != 0 {
		t.Fatalf("after warm restart dups=%d out=%d", p2.M.Duplicates.Load(), p2.M.Out.Load())
	}
	// Committed offsets: nothing is re-read by the first group on restart.
	if again := runFor(t, cfg, NewWindow(time.Hour, 10), 2*time.Second); again.M.In.Load() != 0 {
		t.Fatalf("re-read %d records after commit", again.M.In.Load())
	}
}
