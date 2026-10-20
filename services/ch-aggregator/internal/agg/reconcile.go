package agg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Drift is one (tenant, source, batch, processing_version) whose stored rows disagree with the unique event count.
// Rows > Unique means duplicates were stored (insert dedup missed); the exact readers still hide them via LIMIT 1 BY.
type Drift struct {
	TenantID          string `json:"tenant_id"`
	SourceID          string `json:"source_id"`
	BatchID           string `json:"batch_id"`
	ProcessingVersion uint32 `json:"processing_version"`
	Rows              uint64 `json:"rows,string"`
	Unique            uint64 `json:"unique_events,string"`
}

// ReconcileSQL compares count() with uniqExact(event_id) per batch inside the window (§5.3 reconciliation).
func (c *Client) ReconcileSQL(w Window, limit int) string {
	f, t := w.lit()
	return fmt.Sprintf(`SELECT toString(tenant_id) AS tenant_id, toString(source_id) AS source_id, toString(batch_id) AS batch_id, processing_version,
 count() AS rows, uniqExact(event_id) AS unique_events
FROM %s WHERE event_time >= toDateTime64('%s', 3, 'UTC') AND event_time < toDateTime64('%s', 3, 'UTC')
GROUP BY tenant_id, source_id, batch_id, processing_version HAVING rows != unique_events
ORDER BY rows - unique_events DESC LIMIT %d FORMAT JSONEachRow`, c.Events, f, t, limit)
}

// query runs a read-only statement (FORMAT JSONEachRow) and decodes each row into a T.
func query[T any](ctx context.Context, c *Client, sql string) ([]T, error) {
	v := url.Values{}
	if c.Database != "" {
		v.Set("database", c.Database)
	}
	v.Set("readonly", "1")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/?"+v.Encode(), strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	if c.User != "" {
		req.Header.Set("X-ClickHouse-User", c.User)
		req.Header.Set("X-ClickHouse-Key", c.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("clickhouse: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var out []T
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var d T
		if err := dec.Decode(&d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// Reconcile returns up to limit batches with duplicate rows in the window.
func (c *Client) Reconcile(ctx context.Context, w Window, limit int) ([]Drift, error) {
	return query[Drift](ctx, c, c.ReconcileSQL(w, limit))
}

// AggDrift is an hourly bucket whose latest stored aggregate disagrees with the primary events.
type AggDrift struct {
	TenantID          string `json:"tenant_id"`
	SourceID          string `json:"source_id"`
	ProcessingVersion uint32 `json:"processing_version"`
	Hour              string `json:"hour"`
	Stored            uint64 `json:"stored,string"`
	Actual            uint64 `json:"actual,string"`
}

// AggDriftSQL compares source_metrics_hourly.events_count (latest revision per bucket) with a fresh count of
// de-duplicated events. A bucket present on only one side counts as drift (missing aggregate or orphan aggregate).
func (c *Client) AggDriftSQL(w Window, limit int) string {
	f, t := w.lit()
	return fmt.Sprintf(`SELECT toString(tenant_id) AS tenant_id, toString(source_id) AS source_id, processing_version, toString(hour) AS hour, sum(s_) AS stored, sum(a_) AS actual
FROM (
  SELECT tenant_id, source_id, processing_version, hour, argMax(events_count, revision) AS s_, toUInt64(0) AS a_
  FROM source_metrics_hourly WHERE hour >= toDateTime('%[1]s', 'UTC') AND hour < toDateTime('%[2]s', 'UTC')
  GROUP BY tenant_id, source_id, processing_version, hour
  UNION ALL
  SELECT tenant_id, source_id, processing_version, toStartOfHour(event_time, 'UTC') AS hour, toUInt64(0), count()
  FROM (%[3]s) GROUP BY tenant_id, source_id, processing_version, hour
)
GROUP BY tenant_id, source_id, processing_version, hour
HAVING stored != actual
ORDER BY abs(toInt64(stored) - toInt64(actual)) DESC LIMIT %[4]d FORMAT JSONEachRow`, f, t, c.dedupEvents(w), limit)
}

// AggregateDrift returns up to limit hourly buckets where stored aggregates differ from the primary events.
func (c *Client) AggregateDrift(ctx context.Context, w Window, limit int) ([]AggDrift, error) {
	return query[AggDrift](ctx, c, c.AggDriftSQL(w, limit))
}

// Gap is an hour without any event between two hours that have events, for one (tenant, source, version).
type Gap struct {
	TenantID          string `json:"tenant_id"`
	SourceID          string `json:"source_id"`
	ProcessingVersion uint32 `json:"processing_version"`
	Hour              string `json:"hour"`
}

// GapsSQL lists empty hours strictly between the first and last hour with events per source (WITH FILL).
// Informational: a quiet source legitimately has gaps.
func (c *Client) GapsSQL(w Window, limit int) string {
	return fmt.Sprintf(`SELECT toString(tenant_id) AS tenant_id, toString(source_id) AS source_id, processing_version, toString(hour) AS hour
FROM (
  SELECT tenant_id, source_id, processing_version, toStartOfHour(event_time, 'UTC') AS hour, count() AS n
  FROM (%s) GROUP BY tenant_id, source_id, processing_version, hour
  ORDER BY tenant_id, source_id, processing_version, hour WITH FILL STEP 3600
) WHERE n = 0 LIMIT %d FORMAT JSONEachRow`, c.dedupEvents(w), limit)
}

// Gaps returns up to limit empty hours inside each source's active range.
func (c *Client) Gaps(ctx context.Context, w Window, limit int) ([]Gap, error) {
	return query[Gap](ctx, c, c.GapsSQL(w, limit))
}
