package sink

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"
)

// TestIntegrationIntentOverhead measures what CHSINK_INTENT_TABLE adds per chunk: the extra Record insert that
// runs before each chunk Insert. Needs CHSINK_IT_URL (+USER/PASSWORD/DB) with migrations 0001 and 0004 applied
// and CHSINK_IT_BENCH=1; it writes a few thousand rows to events_normalized_local, so use a throwaway ClickHouse.
func TestIntegrationIntentOverhead(t *testing.T) {
	base := os.Getenv("CHSINK_IT_URL")
	if base == "" || os.Getenv("CHSINK_IT_BENCH") == "" {
		t.Skip("CHSINK_IT_URL / CHSINK_IT_BENCH not set")
	}
	cfg := Config{CHURL: base, CHDatabase: os.Getenv("CHSINK_IT_DB"), CHUser: os.Getenv("CHSINK_IT_USER"),
		CHPassword: os.Getenv("CHSINK_IT_PASSWORD"), CHTable: "events_normalized_local", IntentTable: "sink_chunk_intents_local"}
	s, err := NewCHSink(cfg)
	if err != nil {
		t.Fatal(err)
	}
	in, err := NewCHIntents(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	part := int32(time.Now().UnixNano() % 1_000_000)
	next := int64(1)
	chunk := func(n int) Chunk {
		first := next
		rows := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			line, code, why := MapMessage(Message{Topic: "processed.events.v1", Partition: part, Offset: next, Value: good(int(next))}, fixedNow)
			if code != "" {
				t.Fatal(code, why)
			}
			rows = append(rows, line)
			next++
		}
		return Chunk{Token: Token("processed.events.v1", part, first, next-1), Topic: "processed.events.v1", Partition: part,
			FirstOffset: first, LastOffset: next - 1, Rows: rows}
	}
	pct := func(d []time.Duration, p float64) time.Duration {
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		return d[int(float64(len(d)-1)*p)]
	}
	const chunks = 40
	for _, size := range []int{100, 1000, 5000} {
		var plain, withIntent, recOnly []time.Duration
		for i := 0; i < chunks; i++ {
			c := chunk(size)
			t0 := time.Now()
			if err := s.Insert(ctx, c); err != nil {
				t.Fatal(err)
			}
			plain = append(plain, time.Since(t0))

			c = chunk(size)
			t0 = time.Now()
			if err := in.Record(ctx, c.Topic, c.Partition, Range{c.FirstOffset, c.LastOffset}); err != nil {
				t.Fatal(err)
			}
			recOnly = append(recOnly, time.Since(t0))
			if err := s.Insert(ctx, c); err != nil {
				t.Fatal(err)
			}
			withIntent = append(withIntent, time.Since(t0))
		}
		t.Logf("chunk=%d rows, %d chunks each: insert p50=%v p95=%v | record+insert p50=%v p95=%v | record alone p50=%v p95=%v",
			size, chunks, pct(plain, .5), pct(plain, .95), pct(withIntent, .5), pct(withIntent, .95), pct(recOnly, .5), pct(recOnly, .95))
	}
}
