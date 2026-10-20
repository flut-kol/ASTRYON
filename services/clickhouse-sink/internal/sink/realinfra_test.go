package sink

// Real-broker, real-ClickHouse end-to-end exercise. Unlike integration_test.go
// (CHSINK_IT_URL only, direct CHSink.Insert against events_normalized_local), this
// drives the full Pipeline + KafkaSource against a real Kafka broker and verifies
// dedup through the Distributed table (events_normalized_distributed), which is the
// path main.go/production actually reads/writes. Gated by env vars so `go test ./...`
// is a no-op without a real dev stack (see make dev-up / make topics / db/apply.sh).
// Run with:
//   CHSINK_IT_URL=http://127.0.0.1:18124 CHSINK_IT_DB=astryon CHSINK_IT_USER=astryon \
//   CHSINK_IT_PASSWORD=astryon_dev_only CHSINK_IT_KAFKA=127.0.0.1:19092 \
//   go test ./internal/sink/ -run TestRealInfraEndToEnd -v
//
// Uses a fresh throwaway Kafka topic/group per run and truncates
// events_normalized_local first, so it is safe to re-run against the dev stack.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// chExec runs a statement with no result rows expected (e.g. TRUNCATE).
func chExec(t *testing.T, base, db, user, pass, q string) {
	t.Helper()
	req, _ := http.NewRequest("POST", base+"/?"+url.Values{"database": {db}}.Encode(), strings.NewReader(q))
	req.Header.Set("X-ClickHouse-User", user)
	req.Header.Set("X-ClickHouse-Key", pass)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	if r.StatusCode != 200 {
		t.Fatalf("exec %q: HTTP %d: %s", q, r.StatusCode, b)
	}
}

func chQuery(t *testing.T, base, db, user, pass, q string) string {
	t.Helper()
	qs := url.Values{"query": {q}, "database": {db}}
	req, _ := http.NewRequest("GET", base+"/?"+qs.Encode(), nil)
	req.Header.Set("X-ClickHouse-User", user)
	req.Header.Set("X-ClickHouse-Key", pass)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	if r.StatusCode != 200 {
		t.Fatalf("query %q: HTTP %d: %s", q, r.StatusCode, b)
	}
	return strings.TrimSpace(string(b))
}

func realEnvelope(eventID string, offsetHint int64) []byte {
	now := time.Now().UTC()
	return []byte(fmt.Sprintf(`{"event_id":"%s","tenant_id":"44444444-4444-4444-8444-444444444444",`+
		`"source_id":"55555555-5555-4555-8555-555555555555","schema_id":"realinfra.v1","schema_version":1,`+
		`"event_time":"%s","ingested_at":"%s","trace_id":"t","payload_format":"json",`+
		`"payload":{"offset_hint":%d},"processing_version":1}`,
		eventID, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), offsetHint))
}

// TestRealInfraEndToEnd drives the actual KafkaSource + CHSink + Pipeline against a
// real Kafka broker and a real ClickHouse Distributed table (events_normalized_distributed):
// normal events, in-batch duplicate events (same event_id, different offsets - an
// upstream-retry style duplicate), a run split across several flush/commit boundaries,
// and a restart with the same consumer group to prove offsets resume correctly.
func TestRealInfraEndToEnd(t *testing.T) {
	chURL := os.Getenv("CHSINK_IT_URL")
	brokers := os.Getenv("CHSINK_IT_KAFKA")
	if chURL == "" || brokers == "" {
		t.Skip("CHSINK_IT_URL / CHSINK_IT_KAFKA not set")
	}
	db := envOr(os.Getenv("CHSINK_IT_DB"), "astryon")
	user := os.Getenv("CHSINK_IT_USER")
	pass := os.Getenv("CHSINK_IT_PASSWORD")
	table := "events_normalized_distributed"

	// Isolate this run: the local table backing the Distributed table is shared
	// dev-stack state, so start from empty to make row-count assertions exact.
	chExec(t, chURL, db, user, pass, "TRUNCATE TABLE events_normalized_local")

	topic := fmt.Sprintf("processed.events.v1.it.%d", time.Now().UnixNano())
	group := fmt.Sprintf("chsink-it-%d", time.Now().UnixNano())

	// Create the throwaway topic (single partition, so offset math is easy to reason about).
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := createTopic(admin, topic, 1); err != nil {
		t.Fatal(err)
	}

	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	produce := func(eventID string, offsetHint int64) {
		rec := &kgo.Record{Topic: topic, Partition: 0, Value: realEnvelope(eventID, offsetHint)}
		if err := prod.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	uuid := func(n int) string { return fmt.Sprintf("66666666-6666-4666-8666-%012d", n) }

	// --- batch 1: offsets 0-4, five normal events, plus offset 5 as an in-batch
	// duplicate of event uuid(2) (same event_id, different Kafka offset: a realistic
	// upstream-retry duplicate, NOT caught by the insert-dedup token since it is a
	// distinct row, not a retried identical chunk).
	for i := 0; i < 5; i++ {
		produce(uuid(i), int64(i))
	}
	produce(uuid(2), 5) // duplicate event_id at a new offset

	// --- batch 2: offsets 6-9, four more normal events, to exercise a second
	// flush/commit boundary immediately after the first.
	for i := 5; i < 9; i++ {
		produce(uuid(i), int64(i+1))
	}

	cfg := Config{Brokers: []string{brokers}, Topic: topic, QuarantineTopic: topic + ".dlq", GroupID: group,
		CHURL: chURL, CHDatabase: db, CHUser: user, CHPassword: pass, CHTable: table, BatchSize: 5}
	ch, err := NewCHSink(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ks, err := NewKafka(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(ks, ch, ks, Options{BatchSize: 5, BatchInterval: 300 * time.Millisecond,
		RetryBackoff: 50 * time.Millisecond, RetryBackoffMax: time.Second, ShutdownTimeout: 5 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	go func() {
		time.Sleep(3 * time.Second) // let both batches flush+commit, then stop cleanly
		cancel()
	}()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("pipeline run: %v", err)
	}
	if p.M.Commits.Load() < 2 {
		t.Fatalf("expected at least 2 commits (one per flush boundary), got %d", p.M.Commits.Load())
	}

	// Raw row count: 10 rows produced (9 distinct event_ids + 1 duplicate row for uuid(2)).
	rawCount := chQuery(t, chURL, db, user, pass, fmt.Sprintf("SELECT count() FROM %s WHERE tenant_id='44444444-4444-4444-8444-444444444444'", table))
	if rawCount != "10" {
		t.Fatalf("raw row count = %s, want 10 (9 distinct events + 1 duplicate)", rawCount)
	}
	// Exact/query-time dedup (per migration 0001's documented strategy, item 5): distinct
	// event_ids must be exactly 9 - the duplicate row must not inflate the logical count.
	exactCount := chQuery(t, chURL, db, user, pass,
		fmt.Sprintf("SELECT uniqExact(event_id) FROM %s WHERE tenant_id='44444444-4444-4444-8444-444444444444'", table))
	if exactCount != "9" {
		t.Fatalf("uniqExact(event_id) = %s, want 9", exactCount)
	}

	// --- restart with the SAME consumer group: nothing new to deliver (everything
	// already committed), proving offset commit against the real broker landed correctly
	// and a restart does not reprocess/duplicate already-committed data.
	ks.Close()
	ks2, err := NewKafka(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ks2.Close()
	pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
	msgs, err := ks2.Poll(pctx)
	pcancel()
	if err != nil {
		t.Fatalf("poll after restart: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("restart redelivered %d already-committed messages", len(msgs))
	}

	// --- retry-same-token idempotency directly against the Distributed table (the
	// actual unverified caveat: does insert_deduplication_token survive Distributed
	// table routing, or only work against the local MergeTree table).
	var rows [][]byte
	for i := 100; i < 103; i++ {
		line, code, why := MapMessage(Message{Topic: topic, Partition: 0, Offset: int64(i), Value: realEnvelope(uuid(i), int64(i))}, time.Now())
		if code != "" {
			t.Fatal(code, why)
		}
		rows = append(rows, line)
	}
	tok := Token(topic, 0, 100, 102)
	c := Chunk{Token: tok, Rows: rows}
	for i := 0; i < 2; i++ {
		if err := ch.Insert(context.Background(), c); err != nil {
			t.Fatalf("distributed insert (attempt %d): %v", i, err)
		}
	}
	n := chQuery(t, chURL, db, user, pass, fmt.Sprintf("SELECT count() FROM %s WHERE tenant_id='44444444-4444-4444-8444-444444444444' AND event_id IN ('%s','%s','%s')",
		table, uuid(100), uuid(101), uuid(102)))
	if n != "3" {
		t.Fatalf("same-token retry through Distributed table: count=%s, want 3 (dedup should drop the 2nd insert)", n)
	}
}

func envOr(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func createTopic(cl *kgo.Client, topic string, parts int32) error {
	req := kmsg.NewCreateTopicsRequest()
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic = topic
	t.NumPartitions = parts
	t.ReplicationFactor = 1
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		return err
	}
	for _, rt := range resp.Topics {
		if rt.ErrorCode != 0 && rt.ErrorCode != 36 { // 36 = TOPIC_ALREADY_EXISTS
			return fmt.Errorf("create topic %s: %v", topic, rt.ErrorCode)
		}
	}
	return nil
}
