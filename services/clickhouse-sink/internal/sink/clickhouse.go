package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// CHSink inserts chunks over the ClickHouse HTTP interface (FORMAT JSONEachRow).
type CHSink struct {
	URL, Database, User, Password, Table string
	Client                               *http.Client
}

func NewCHSink(c Config) (*CHSink, error) {
	if !identRe.MatchString(c.CHTable) || (c.CHDatabase != "" && !identRe.MatchString(c.CHDatabase)) {
		return nil, fmt.Errorf("invalid database/table identifier")
	}
	return &CHSink{URL: c.CHURL, Database: c.CHDatabase, User: c.CHUser, Password: c.CHPassword, Table: c.CHTable,
		Client: &http.Client{Timeout: 60 * time.Second}}, nil
}

// Insert posts the rows with insert_deduplicate=1 and the chunk's deterministic token.
func (s *CHSink) Insert(ctx context.Context, c Chunk) error {
	q := url.Values{}
	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", s.Table))
	if s.Database != "" {
		q.Set("database", s.Database)
	}
	q.Set("insert_deduplicate", "1")
	q.Set("insert_deduplication_token", c.Token)
	q.Set("date_time_input_format", "best_effort")
	q.Set("input_format_skip_unknown_fields", "0")
	var body bytes.Buffer
	for _, r := range c.Rows {
		body.Write(r)
		body.WriteByte('\n')
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/?"+q.Encode(), &body)
	if err != nil {
		return err
	}
	if s.User != "" {
		req.Header.Set("X-ClickHouse-User", s.User)
		if s.Password != "" {
			req.Header.Set("X-ClickHouse-Key", s.Password)
		}
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("clickhouse insert: HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
	return nil
}
