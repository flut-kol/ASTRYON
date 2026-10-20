package sink

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
)

// Run with CHSINK_IT_URL=http://127.0.0.1:PORT (+ CHSINK_IT_USER/PASSWORD/DB) against a ClickHouse
// that has migration 0001 applied (single node, plain MergeTree).
func TestIntegrationDedupToken(t *testing.T) {
	base := os.Getenv("CHSINK_IT_URL")
	if base == "" {
		t.Skip("CHSINK_IT_URL not set")
	}
	cfg := Config{CHURL: base, CHDatabase: os.Getenv("CHSINK_IT_DB"), CHUser: os.Getenv("CHSINK_IT_USER"),
		CHPassword: os.Getenv("CHSINK_IT_PASSWORD"), CHTable: "events_normalized_local"}
	s, err := NewCHSink(cfg)
	if err != nil {
		t.Fatal(err)
	}
	count := func() string {
		q := url.Values{"query": {"SELECT count() FROM events_normalized_local"}, "database": {cfg.CHDatabase}}
		req, _ := http.NewRequest("GET", base+"/?"+q.Encode(), nil)
		req.Header.Set("X-ClickHouse-User", cfg.CHUser)
		req.Header.Set("X-ClickHouse-Key", cfg.CHPassword)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return strings.TrimSpace(string(b))
	}
	var rows [][]byte
	for i := 1; i <= 3; i++ {
		line, code, why := MapMessage(Message{Topic: "processed.events.v1", Partition: 0, Offset: int64(i), Value: good(i)}, fixedNow)
		if code != "" {
			t.Fatal(code, why)
		}
		rows = append(rows, line)
	}
	c := Chunk{Token: Token("processed.events.v1", 0, 1, 3), Rows: rows}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := s.Insert(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if n := count(); n != "3" {
		t.Fatalf("same token twice: count=%s, want 3", n)
	}
	c.Token = Token("processed.events.v1", 0, 4, 6)
	if err := s.Insert(ctx, c); err != nil {
		t.Fatal(err)
	}
	if n := count(); n != "6" {
		t.Fatalf("new token: count=%s, want 6", n)
	}
}

// TestIntegrationDedupTokenDistributed checks whether insert_deduplication_token still deduplicates when the
// sink writes through events_normalized_distributed (single-node 'default' cluster). Distributed inserts are
// asynchronous, so the test flushes the queue before counting the local table.
// Uses a time-derived Kafka partition number so reruns against the same database stay independent.
func TestIntegrationDedupTokenDistributed(t *testing.T) {
	base := os.Getenv("CHSINK_IT_URL")
	if base == "" {
		t.Skip("CHSINK_IT_URL not set")
	}
	cfg := Config{CHURL: base, CHDatabase: os.Getenv("CHSINK_IT_DB"), CHUser: os.Getenv("CHSINK_IT_USER"),
		CHPassword: os.Getenv("CHSINK_IT_PASSWORD"), CHTable: "events_normalized_distributed"}
	s, err := NewCHSink(cfg)
	if err != nil {
		t.Fatal(err)
	}
	run := func(query string) string {
		q := url.Values{"database": {cfg.CHDatabase}}
		req, _ := http.NewRequest("POST", base+"/?"+q.Encode(), strings.NewReader(query))
		req.Header.Set("X-ClickHouse-User", cfg.CHUser)
		req.Header.Set("X-ClickHouse-Key", cfg.CHPassword)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		if r.StatusCode/100 != 2 {
			t.Fatalf("%s: %s", query, b)
		}
		return strings.TrimSpace(string(b))
	}
	part := int32(time.Now().UnixNano() % 1_000_000)
	like := fmt.Sprintf("SELECT count() FROM events_normalized_local WHERE raw_ref LIKE 'kafka:processed.events.v1/%d/%%'", part)
	before := run(like)
	var rows [][]byte
	for i := 101; i <= 103; i++ {
		line, code, why := MapMessage(Message{Topic: "processed.events.v1", Partition: part, Offset: int64(i), Value: good(i)}, fixedNow)
		if code != "" {
			t.Fatal(code, why)
		}
		rows = append(rows, line)
	}
	c := Chunk{Token: Token("processed.events.v1", part, 101, 103), Rows: rows}
	for i := 0; i < 2; i++ {
		if err := s.Insert(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		run("SYSTEM FLUSH DISTRIBUTED events_normalized_distributed")
	}
	after := run(like)
	t.Logf("distributed same token twice: before=%s after=%s (3 rows sent twice)", before, after)
	if before != "0" || after != "3" {
		t.Fatalf("distributed dedup failed: before=%s after=%s, want 0 -> 3", before, after)
	}
}

// TestIntegrationIntentStore checks CHIntents against a ClickHouse with migration 0004 applied
// (table sink_chunk_intents_local). Uses a time-derived partition so reruns stay independent.
func TestIntegrationIntentStore(t *testing.T) {
	base := os.Getenv("CHSINK_IT_URL")
	if base == "" {
		t.Skip("CHSINK_IT_URL not set")
	}
	in, err := NewCHIntents(Config{CHURL: base, CHDatabase: os.Getenv("CHSINK_IT_DB"), CHUser: os.Getenv("CHSINK_IT_USER"),
		CHPassword: os.Getenv("CHSINK_IT_PASSWORD"), IntentTable: "sink_chunk_intents_local"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	part := int32(time.Now().UnixNano() % 1_000_000)
	if _, ok, err := in.Latest(ctx, "processed.events.v1", part); err != nil || ok {
		t.Fatalf("empty: ok=%v err=%v", ok, err)
	}
	for _, r := range []Range{{0, 5}, {6, 9_000_000_000}} {
		if err := in.Record(ctx, "processed.events.v1", part, r); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := in.Latest(ctx, "processed.events.v1", part)
	if err != nil || !ok || got != (Range{6, 9_000_000_000}) {
		t.Fatalf("latest=%v ok=%v err=%v", got, ok, err)
	}
	if _, ok, _ := in.Latest(ctx, "processed.events.v1", part+1); ok {
		t.Fatal("intent leaked to another partition")
	}
}
