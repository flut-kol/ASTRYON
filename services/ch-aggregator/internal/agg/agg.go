// Package agg recomputes ClickHouse aggregate tables (0003_aggregates) from events_normalized_* and
// processing_errors_local. Every run writes a new `revision`; readers take argMax(.., revision) per key (§5.3),
// so recomputes and replays (new processing_version) never double metrics.
package agg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

const tsLayout = "2006-01-02 15:04:05"

// Client executes statements over the ClickHouse HTTP interface.
type Client struct {
	URL, Database, User, Password string
	Events, Errors                string // source tables (default events_normalized_distributed / processing_errors_local)
	HTTP                          *http.Client
}

func New(chURL, db, user, pass string) (*Client, error) {
	if db != "" && !identRe.MatchString(db) {
		return nil, fmt.Errorf("invalid database identifier")
	}
	return &Client{URL: chURL, Database: db, User: user, Password: pass,
		Events: "events_normalized_distributed", Errors: "processing_errors_local",
		HTTP: &http.Client{Timeout: 10 * time.Minute}}, nil
}

// Window is a half-open [From, To) UTC interval aligned to whole hours.
type Window struct{ From, To time.Time }

// HourWindow aligns [from, to) outward to hour boundaries.
func HourWindow(from, to time.Time) (Window, error) {
	from, to = from.UTC().Truncate(time.Hour), to.UTC()
	if t := to.Truncate(time.Hour); t.Before(to) {
		to = t.Add(time.Hour)
	}
	if !to.After(from) {
		return Window{}, fmt.Errorf("empty window")
	}
	return Window{from, to}, nil
}

func (w Window) lit() (string, string) { return w.From.Format(tsLayout), w.To.Format(tsLayout) }

// dedupEvents yields one row per (tenant, source, event, processing_version) inside the window; it does not rely on
// FINAL or merge-time dedup (see 0001).
func (c *Client) dedupEvents(w Window) string {
	f, t := w.lit()
	return fmt.Sprintf(`SELECT * FROM %s WHERE event_time >= toDateTime64('%s', 3, 'UTC') AND event_time < toDateTime64('%s', 3, 'UTC')
 LIMIT 1 BY tenant_id, source_id, event_id, processing_version`, c.Events, f, t)
}

func (c *Client) dedupErrors(w Window) string {
	f, t := w.lit()
	return fmt.Sprintf(`SELECT * FROM %s WHERE occurred_at >= toDateTime64('%s', 3, 'UTC') AND occurred_at < toDateTime64('%s', 3, 'UTC')
 LIMIT 1 BY tenant_id, source_id, event_id, processing_version, stage, error_code`, c.Errors, f, t)
}

// Statements returns the INSERT ... SELECT statements for one recompute at the given revision.
// Rejected events carry no pipeline_id in processing_errors, so they are attributed to the nil UUID pipeline.
func (c *Client) Statements(w Window, rev uint64) []string {
	nilUUID := "toUUID('00000000-0000-0000-0000-000000000000')"
	ev, er := c.dedupEvents(w), c.dedupErrors(w)
	return []string{
		fmt.Sprintf(`INSERT INTO source_metrics_hourly (tenant_id, source_id, processing_version, hour, revision, events_count, bytes_count, batches_count, errors_count, max_ingest_lag_ms)
SELECT tenant_id, source_id, processing_version, hour, %d,
  countIf(is_event), sumIf(bytes, is_event), toUInt32(uniqExactIf(batch_id, is_event)), countIf(NOT is_event), maxIf(lag_ms, is_event)
FROM (
  SELECT tenant_id, source_id, processing_version, toStartOfHour(event_time, 'UTC') AS hour, batch_id, 1 AS is_event,
         toUInt64(length(payload)) AS bytes, toUInt64(greatest(0, dateDiff('millisecond', event_time, ingested_at))) AS lag_ms
  FROM (%s)
  UNION ALL
  SELECT tenant_id, source_id, processing_version, toStartOfHour(occurred_at, 'UTC'), batch_id, 0, toUInt64(0), toUInt64(0)
  FROM (%s)
) GROUP BY tenant_id, source_id, processing_version, hour`, rev, ev, er),

		fmt.Sprintf(`INSERT INTO pipeline_metrics_hourly (tenant_id, pipeline_id, pipeline_version, processing_version, hour, revision, events_in, events_out, events_failed, p50_latency_ms, p95_latency_ms)
SELECT tenant_id, pipeline_id, pipeline_version, processing_version, hour, %d,
  countIf(is_event) + countIf(NOT is_event), countIf(is_event), countIf(NOT is_event),
  ifNotFinite(toFloat32(quantileIf(0.5)(lag_ms, is_event)), 0), ifNotFinite(toFloat32(quantileIf(0.95)(lag_ms, is_event)), 0)
FROM (
  SELECT tenant_id, pipeline_id, pipeline_version, processing_version, toStartOfHour(event_time, 'UTC') AS hour, 1 AS is_event,
         toUInt64(greatest(0, dateDiff('millisecond', event_time, ingested_at))) AS lag_ms
  FROM (%s)
  UNION ALL
  SELECT tenant_id, %s, pipeline_version, processing_version, toStartOfHour(occurred_at, 'UTC'), 0, toUInt64(0)
  FROM (%s)
) GROUP BY tenant_id, pipeline_id, pipeline_version, processing_version, hour`, rev, ev, nilUUID, er),

		// totals row (flag = '')
		fmt.Sprintf(`INSERT INTO processing_quality_daily (tenant_id, source_id, pipeline_id, processing_version, day, revision, events_total, events_valid, events_rejected, events_with_flags, flag, flag_count)
SELECT tenant_id, source_id, pipeline_id, processing_version, day, %d,
  countIf(is_event) + countIf(NOT is_event), countIf(is_event), countIf(NOT is_event), countIf(flagged), '', 0
FROM (
  SELECT tenant_id, source_id, pipeline_id, processing_version, toDate(event_time, 'UTC') AS day, 1 AS is_event, notEmpty(quality_flags) AS flagged
  FROM (%s)
  UNION ALL
  SELECT tenant_id, source_id, %s, processing_version, toDate(occurred_at, 'UTC'), 0, 0
  FROM (%s)
) GROUP BY tenant_id, source_id, pipeline_id, processing_version, day`, rev, ev, nilUUID, er),

		// per-flag rows
		fmt.Sprintf(`INSERT INTO processing_quality_daily (tenant_id, source_id, pipeline_id, processing_version, day, revision, events_total, events_valid, events_rejected, events_with_flags, flag, flag_count)
SELECT tenant_id, source_id, pipeline_id, processing_version, toDate(event_time, 'UTC'), %d, 0, 0, 0, 0, flag, count()
FROM (%s) ARRAY JOIN quality_flags AS flag
GROUP BY tenant_id, source_id, pipeline_id, processing_version, toDate(event_time, 'UTC'), flag`, rev, ev),
	}
}

// Recompute runs all statements for the window under one revision (unix millis by default; strictly increasing
// across runs as long as the caller's clock is).
func (c *Client) Recompute(ctx context.Context, w Window, rev uint64) error {
	for i, s := range c.Statements(w, rev) {
		if err := c.exec(ctx, s); err != nil {
			return fmt.Errorf("statement %d: %w", i, err)
		}
	}
	return nil
}

// LatestSourceMetricsSQL is the canonical reader for revisioned data: latest revision per key.
const LatestSourceMetricsSQL = `SELECT tenant_id, source_id, processing_version, hour,
  argMax(events_count, revision) AS events_count, argMax(bytes_count, revision) AS bytes_count,
  argMax(batches_count, revision) AS batches_count, argMax(errors_count, revision) AS errors_count
FROM source_metrics_hourly WHERE tenant_id = {tenant:UUID} GROUP BY tenant_id, source_id, processing_version, hour`

func (c *Client) exec(ctx context.Context, q string) error {
	v := url.Values{}
	if c.Database != "" {
		v.Set("database", c.Database)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/?"+v.Encode(), bytes.NewReader([]byte(q)))
	if err != nil {
		return err
	}
	if c.User != "" {
		req.Header.Set("X-ClickHouse-User", c.User)
		req.Header.Set("X-ClickHouse-Key", c.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("clickhouse: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}
