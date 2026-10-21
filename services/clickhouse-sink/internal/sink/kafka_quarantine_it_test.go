package sink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// recSource wraps the real KafkaSource and remembers the highest committed offset.
type recSource struct {
	*KafkaSource
	mu   sync.Mutex
	high int64
}

func (r *recSource) Commit(ctx context.Context, m []Message) error {
	if err := r.KafkaSource.Commit(ctx, m); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, x := range m {
		if x.Offset > r.high {
			r.high = x.Offset
		}
	}
	return nil
}

func (r *recSource) highest() int64 { r.mu.Lock(); defer r.mu.Unlock(); return r.high }

// Live quarantine path against a real broker (plaintext). Skipped unless CHSINK_KAFKA_IT_BROKER is set, e.g.
// CHSINK_KAFKA_IT_BROKER=127.0.0.1:19092. Creates uniquely named topics and a consumer group per run.
// 10 messages (offsets 0..9), 3 of them poison (invalid JSON, unknown schema, empty): expect 7 rows to the sink,
// 3 records on the quarantine topic carrying the source offsets, the source offset committed past the last
// poison message, and nothing redelivered to a restarted consumer of the same group.
func TestIT_QuarantineRealBroker(t *testing.T) {
	broker := os.Getenv("CHSINK_KAFKA_IT_BROKER")
	if broker == "" {
		t.Skip("CHSINK_KAFKA_IT_BROKER not set")
	}
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	topic, qtopic, group := "chsink.it.in."+id, "chsink.it.quar."+id, "chsink-it-"+id
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	adm, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer adm.Close()
	req := kmsg.NewCreateTopicsRequest()
	for _, n := range []string{topic, qtopic} {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = n, 1, 1
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, adm)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resp.Topics {
		if r.ErrorCode != 0 {
			t.Fatalf("create topic %s: %v", r.Topic, r.ErrorMessage)
		}
	}
	defer func() {
		d := kmsg.NewDeleteTopicsRequest()
		d.TopicNames = []string{topic, qtopic}
		for _, n := range d.TopicNames {
			dt := kmsg.NewDeleteTopicsRequestTopic()
			dt.Topic = &[]string{n}[0]
			d.Topics = append(d.Topics, dt)
		}
		d.TimeoutMillis = 5000
		_, _ = d.RequestWith(context.Background(), adm)
	}()

	poison := map[int]string{2: `not json at all`, 5: `{"schema_id":"x.v9"}`, 8: ``}
	var recs []*kgo.Record
	for i := 0; i < 10; i++ {
		v := []byte(poison[i])
		if _, bad := poison[i]; !bad {
			v = good(i)
		}
		recs = append(recs, &kgo.Record{Topic: topic, Partition: 0, Value: v})
	}
	if err := adm.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	cfg := Config{Brokers: []string{broker}, Topic: topic, GroupID: group, BatchSize: 10, QuarantineTopic: qtopic}
	k, err := NewKafka(cfg)
	if err != nil {
		t.Fatal(err)
	}
	src := &recSource{KafkaSource: k, high: -1}
	snk := &fakeSink{}
	p := NewPipeline(src, snk, k, Options{BatchSize: 10, BatchInterval: 200 * time.Millisecond, RetryBackoff: 50 * time.Millisecond,
		RetryBackoffMax: 200 * time.Millisecond, MaxRetries: 5, ShutdownTimeout: 5 * time.Second})
	rctx, rcancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- p.Run(rctx) }()
	deadline := time.Now().Add(45 * time.Second)
	for src.highest() < 9 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	rcancel()
	if err := <-done; err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if got := src.highest(); got != 9 {
		t.Fatalf("committed offset %d, want 9 (partition must advance past poison messages)", got)
	}
	snk.mu.Lock()
	rows := snk.rows
	snk.mu.Unlock()
	if rows != 7 {
		t.Fatalf("sink rows = %d, want 7", rows)
	}

	// Independent reader of the quarantine topic.
	qc, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(qtopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer qc.Close()
	var got []string
	qctx, qcancel := context.WithTimeout(ctx, 20*time.Second)
	defer qcancel()
	for len(got) < 3 {
		fs := qc.PollFetches(qctx)
		if qctx.Err() != nil {
			break
		}
		fs.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	if len(got) != 3 {
		t.Fatalf("quarantine topic has %d records, want 3: %v", len(got), got)
	}
	for i, off := range []int{2, 5, 8} {
		want := fmt.Sprintf(`"source_offset":%d`, off)
		if !strings.Contains(got[i], want) || !strings.Contains(got[i], `"stage":"clickhouse-sink"`) {
			t.Errorf("quarantine record %d = %s, want %s", i, got[i], want)
		}
	}

	// Restart: same group must not see the 10 messages again.
	k2, err := NewKafka(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer k2.Close()
	pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
	defer pcancel()
	if m, _ := k2.Poll(pctx); len(m) != 0 {
		t.Fatalf("restarted consumer received %d messages, want 0 (offsets were committed)", len(m))
	}
}
