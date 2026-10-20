package api

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

// Row is one decoded ClickHouse JSON row (numbers are json.Number).
type Row map[string]any

// Runner executes a parameterized query. Fakes implement it in tests.
type Runner interface {
	Run(ctx context.Context, q Query) ([]Row, error)
	Ping(ctx context.Context) error
}

// CHClient is a ClickHouse HTTP Runner. Queries run with readonly=2 (cannot
// write or change anything but the listed per-query limits) and bounded
// execution time / result size.
type CHClient struct {
	URL, Database, User, Password string
	HTTP                          *http.Client
	MaxExecSeconds                int
	MaxResultRows                 int
}

func NewCHClient(url, db, user, pass string) *CHClient {
	return &CHClient{URL: strings.TrimRight(url, "/"), Database: db, User: user, Password: pass,
		HTTP: &http.Client{Timeout: 30 * time.Second}, MaxExecSeconds: 20, MaxResultRows: 10000}
}

func (c *CHClient) Run(ctx context.Context, q Query) ([]Row, error) {
	v := url.Values{}
	v.Set("database", c.Database)
	v.Set("readonly", "2")
	v.Set("max_execution_time", fmt.Sprint(c.MaxExecSeconds))
	v.Set("max_result_rows", fmt.Sprint(c.MaxResultRows))
	v.Set("result_overflow_mode", "throw")
	v.Set("output_format_json_quote_64bit_integers", "0")
	for k, val := range q.Params {
		v.Set("param_"+k, val)
	}
	body := q.SQL + " FORMAT JSON"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/?"+v.Encode(), bytes.NewBufferString(body))
	if err != nil {
		return nil, err
	}
	if c.User != "" {
		req.Header.Set("X-ClickHouse-User", c.User)
		req.Header.Set("X-ClickHouse-Key", c.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		msg := string(b)
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return nil, fmt.Errorf("clickhouse: HTTP %d: %s", resp.StatusCode, msg)
	}
	var out struct {
		Data []Row `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("clickhouse: decode: %w", err)
	}
	return out.Data, nil
}

func (c *CHClient) Ping(ctx context.Context) error {
	_, err := c.Run(ctx, Query{SQL: "SELECT 1", Params: map[string]string{}})
	return err
}
