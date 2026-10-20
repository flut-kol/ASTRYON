package sink

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
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
