package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Range is the inclusive offset range of one chunk of a topic partition.
type Range struct{ First, Last int64 }

// IntentStore durably records the offset range of a chunk before it is inserted, so that a restarted sink can re-cut
// the same chunk (same dedup token) after a crash between insert and offset commit.
type IntentStore interface {
	// Record must be durable when it returns nil.
	Record(ctx context.Context, topic string, partition int32, r Range) error
	// Latest returns the most recently recorded range for the partition, if any.
	Latest(ctx context.Context, topic string, partition int32) (Range, bool, error)
}

// CHIntents stores intents in a ClickHouse table over the HTTP interface (migration 0004).
type CHIntents struct {
	URL, Database, User, Password, Table string
	Client                               *http.Client
}

func NewCHIntents(c Config) (*CHIntents, error) {
	if !identRe.MatchString(c.IntentTable) || (c.CHDatabase != "" && !identRe.MatchString(c.CHDatabase)) {
		return nil, fmt.Errorf("invalid database/intent table identifier")
	}
	return &CHIntents{URL: c.CHURL, Database: c.CHDatabase, User: c.CHUser, Password: c.CHPassword, Table: c.IntentTable,
		Client: &http.Client{Timeout: 30 * time.Second}}, nil
}

func (s *CHIntents) do(ctx context.Context, q url.Values, body string) ([]byte, error) {
	if s.Database != "" {
		q.Set("database", s.Database)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.URL, "/")+"/?"+q.Encode(), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	if s.User != "" {
		req.Header.Set("X-ClickHouse-User", s.User)
		if s.Password != "" {
			req.Header.Set("X-ClickHouse-Key", s.Password)
		}
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse intents: HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
	return b, nil
}

func (s *CHIntents) Record(ctx context.Context, topic string, partition int32, r Range) error {
	row, _ := json.Marshal(map[string]any{"topic": topic, "partition": partition, "first_offset": r.First, "last_offset": r.Last})
	q := url.Values{"query": {fmt.Sprintf("INSERT INTO %s (topic, partition, first_offset, last_offset) FORMAT JSONEachRow", s.Table)}}
	_, err := s.do(ctx, q, string(row)+"\n")
	return err
}

func (s *CHIntents) Latest(ctx context.Context, topic string, partition int32) (Range, bool, error) {
	q := url.Values{
		"query": {fmt.Sprintf("SELECT first_offset, last_offset FROM %s WHERE topic = {t:String} AND partition = {p:Int32} "+
			"ORDER BY recorded_at DESC LIMIT 1 FORMAT JSONEachRow", s.Table)},
		"param_t": {topic},
		"param_p": {fmt.Sprint(partition)},
	}
	b, err := s.do(ctx, q, "")
	if err != nil {
		return Range{}, false, err
	}
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return Range{}, false, nil
	}
	var row struct {
		First int64 `json:"first_offset,string"`
		Last  int64 `json:"last_offset,string"`
	}
	if err := json.Unmarshal(b, &row); err != nil {
		return Range{}, false, fmt.Errorf("clickhouse intents: %w", err)
	}
	return Range{row.First, row.Last}, true, nil
}
