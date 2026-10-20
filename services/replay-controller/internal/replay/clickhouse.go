package replay

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CH talks to ClickHouse over the HTTP interface: readonly=1, bound parameters only (param_*).
type CH struct {
	URL, Database, User, Password string
	Table                         string // default events_normalized_distributed
	HTTP                          *http.Client

	mu       sync.Mutex
	reserved map[[2]string]uint32
}

func (c *CH) query(ctx context.Context, q string, params map[string]string) (string, error) {
	v := url.Values{"query": {q}, "readonly": {"1"}}
	if c.Database != "" {
		v.Set("database", c.Database)
	}
	for k, val := range params {
		v.Set("param_"+k, val)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/?"+v.Encode(), nil)
	if err != nil {
		return "", err
	}
	if c.User != "" {
		req.Header.Set("X-ClickHouse-User", c.User)
		req.Header.Set("X-ClickHouse-Key", c.Password)
	}
	cl := c.HTTP
	if cl == nil {
		cl = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := cl.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("clickhouse: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return strings.TrimSpace(string(b)), nil
}

func (c *CH) table() string {
	if c.Table != "" {
		return c.Table
	}
	return "events_normalized_distributed"
}

// Peek returns max(processing_version) for the tenant+source (0 when none).
func (c *CH) Peek(ctx context.Context, tenantID, sourceID string) (uint32, error) {
	out, err := c.query(ctx, "SELECT max(processing_version) FROM "+c.table()+
		" WHERE tenant_id = {tenant:UUID} AND source_id = {source:UUID}",
		map[string]string{"tenant": tenantID, "source": sourceID})
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(out, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: bad max(processing_version) %q", out)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r := c.reserved[[2]string{tenantID, sourceID}]; r > uint32(n) {
		return r, nil
	}
	return uint32(n), nil
}

// Allocate returns max(ClickHouse, versions already handed out by this process)+1. The in-process
// reservation closes the window before the replayed data lands in ClickHouse; the lock makes
// concurrent calls in one process distinct. Multi-replica safety needs a durable reservation
// (TODO: PostgreSQL row with SELECT ... FOR UPDATE / sequence per tenant+source).
func (c *CH) Allocate(ctx context.Context, tenantID, sourceID string) (uint32, error) {
	key := [2]string{tenantID, sourceID}
	for {
		cur, err := c.Peek(ctx, tenantID, sourceID)
		if err != nil {
			return 0, err
		}
		c.mu.Lock()
		if c.reserved == nil {
			c.reserved = map[[2]string]uint32{}
		}
		if c.reserved[key] <= cur { // nobody raced us past cur
			c.reserved[key] = cur + 1
			c.mu.Unlock()
			return cur + 1, nil
		}
		c.mu.Unlock() // another goroutine reserved after our Peek: re-read
	}
}

func chTime(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000") }

// Count is a bounded count() over de-duplicated events of one version in [from,to).
func (c *CH) Count(ctx context.Context, tenantID, sourceID string, version uint32, from, to time.Time, limit uint64) (uint64, error) {
	q := "SELECT count() FROM (SELECT 1 FROM " + c.table() + " WHERE tenant_id = {tenant:UUID} AND source_id = {source:UUID}" +
		" AND processing_version = {pv:UInt32}" +
		" AND event_time >= {from:DateTime64(3,'UTC')} AND event_time < {to:DateTime64(3,'UTC')}" +
		" LIMIT 1 BY (tenant_id, source_id, event_id, processing_version) LIMIT {lim:UInt64})"
	out, err := c.query(ctx, q, map[string]string{"tenant": tenantID, "source": sourceID,
		"pv": strconv.FormatUint(uint64(version), 10), "from": chTime(from), "to": chTime(to),
		"lim": strconv.FormatUint(limit, 10)})
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(out, 10, 64)
}
