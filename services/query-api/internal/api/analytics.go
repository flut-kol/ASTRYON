package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Event is one search result row.
type Event struct {
	EventID           string            `json:"event_id"`
	SourceID          string            `json:"source_id"`
	BatchID           string            `json:"batch_id"`
	ProcessingVersion uint32            `json:"processing_version"`
	EventType         string            `json:"event_type"`
	EventTime         time.Time         `json:"event_time"`
	IngestedAt        time.Time         `json:"ingested_at"`
	EntityID          string            `json:"entity_id"`
	Attributes        map[string]string `json:"attributes"`
	QualityFlags      []string          `json:"quality_flags"`
	Payload           *string           `json:"payload,omitempty"`
}

type SourceMetrics struct {
	SourceID       string `json:"source_id"`
	Events         int64  `json:"events"`
	Bytes          int64  `json:"bytes"`
	Batches        int64  `json:"batches"`
	Errors         int64  `json:"errors"`
	MaxIngestLagMs int64  `json:"max_ingest_lag_ms"`
}

type SeriesPoint struct {
	Hour   time.Time `json:"hour"`
	Events int64     `json:"events"`
	Errors int64     `json:"errors"`
}

type MetricsSummary struct {
	From     time.Time       `json:"from"`
	To       time.Time       `json:"to"`
	Totals   SourceMetrics   `json:"totals"`
	BySource []SourceMetrics `json:"by_source"`
	Series   []SeriesPoint   `json:"series"`
}

type FlagCount struct {
	Flag  string `json:"flag"`
	Count int64  `json:"count"`
}

type ErrorCount struct {
	ErrorCode string `json:"error_code"`
	Stage     string `json:"stage"`
	Count     int64  `json:"count"`
}

type QualitySummary struct {
	From            time.Time    `json:"from"`
	To              time.Time    `json:"to"`
	EventsTotal     int64        `json:"events_total"`
	EventsValid     int64        `json:"events_valid"`
	EventsRejected  int64        `json:"events_rejected"`
	EventsWithFlags int64        `json:"events_with_flags"`
	Flags           []FlagCount  `json:"flags"`
	TopErrors       []ErrorCount `json:"top_errors"`
}

type AggregateResult struct {
	Rows      []map[string]any `json:"rows"`
	Truncated bool             `json:"truncated"`
}

// Analytics is the ClickHouse-backed read model. Every method takes the
// tenant explicitly; it comes from the validated principal.
type Analytics interface {
	QueryEvents(ctx context.Context, tenant string, r EventsRequest, after *Keyset) ([]Event, error)
	Aggregate(ctx context.Context, tenant string, r AggregateRequest) (AggregateResult, error)
	MetricsSummary(ctx context.Context, tenant string, tr TimeRange, sourceID string) (MetricsSummary, error)
	QualitySummary(ctx context.Context, tenant string, tr TimeRange, sourceID string) (QualitySummary, error)
	Ping(ctx context.Context) error
}

// CHAnalytics implements Analytics over a Runner.
type CHAnalytics struct{ R Runner }

func (a CHAnalytics) Ping(ctx context.Context) error { return a.R.Ping(ctx) }

func (a CHAnalytics) QueryEvents(ctx context.Context, tenant string, r EventsRequest, after *Keyset) ([]Event, error) {
	rows, err := a.R.Run(ctx, BuildEventsQuery(tenant, r, after))
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(rows))
	for _, row := range rows {
		e := Event{
			EventID: str(row["event_id"]), SourceID: str(row["source_id"]), BatchID: str(row["batch_id"]),
			ProcessingVersion: uint32(num(row["processing_version"])), EventType: str(row["event_type"]),
			EventTime: chTime(row["event_time"]), IngestedAt: chTime(row["ingested_at"]),
			EntityID: str(row["entity_id"]), Attributes: map[string]string{}, QualityFlags: []string{},
		}
		if m, ok := row["attributes"].(map[string]any); ok {
			for k, v := range m {
				e.Attributes[k] = str(v)
			}
		}
		if l, ok := row["quality_flags"].([]any); ok {
			for _, v := range l {
				e.QualityFlags = append(e.QualityFlags, str(v))
			}
		}
		if p, ok := row["payload"]; ok {
			s := str(p)
			e.Payload = &s
		}
		out = append(out, e)
	}
	return out, nil
}

func (a CHAnalytics) Aggregate(ctx context.Context, tenant string, r AggregateRequest) (AggregateResult, error) {
	rows, err := a.R.Run(ctx, BuildAggregateQuery(tenant, r))
	if err != nil {
		return AggregateResult{}, err
	}
	res := AggregateResult{Rows: []map[string]any{}}
	if len(rows) > r.Limit {
		res.Truncated = true
		rows = rows[:r.Limit]
	}
	for _, row := range rows {
		o := map[string]any{}
		for k, v := range row {
			switch {
			case strings.HasPrefix(k, "g_"):
				o[k[2:]] = v
			case strings.HasPrefix(k, "m_"):
				name := k[2:]
				if strings.HasSuffix(name, "_time") {
					o[name] = chTime(v)
				} else {
					o[name] = num(v)
				}
			}
		}
		res.Rows = append(res.Rows, o)
	}
	return res, nil
}

func (a CHAnalytics) MetricsSummary(ctx context.Context, tenant string, tr TimeRange, sourceID string) (MetricsSummary, error) {
	// Latest revision of the active (highest) processing_version per
	// (source, hour): replays and recomputes never double-count.
	mk := func(outer string) Query {
		b := newQB(tenant)
		b.timeRange("hour", tr)
		if sourceID != "" {
			b.add("source_id = {source_id:UUID}")
			b.params["source_id"] = sourceID
		}
		inner := "SELECT source_id, hour," +
			" argMax(events_count, (processing_version, revision)) AS a_events," +
			" argMax(bytes_count, (processing_version, revision)) AS a_bytes," +
			" argMax(batches_count, (processing_version, revision)) AS a_batches," +
			" argMax(errors_count, (processing_version, revision)) AS a_errors," +
			" argMax(max_ingest_lag_ms, (processing_version, revision)) AS a_lag" +
			" FROM " + tblSourceHr + " WHERE " + b.whereSQL() + " GROUP BY tenant_id, source_id, hour"
		return Query{SQL: strings.Replace(outer, "%INNER%", "("+inner+")", 1), Params: b.params}
	}
	res := MetricsSummary{From: tr.From, To: tr.To, BySource: []SourceMetrics{}, Series: []SeriesPoint{}}
	rows, err := a.R.Run(ctx, mk("SELECT source_id, sum(a_events) AS events, sum(a_bytes) AS bytes,"+
		" sum(a_batches) AS batches, sum(a_errors) AS errors, max(a_lag) AS lag"+
		" FROM %INNER% GROUP BY source_id ORDER BY events DESC LIMIT 200"))
	if err != nil {
		return res, err
	}
	for _, r := range rows {
		m := SourceMetrics{SourceID: str(r["source_id"]), Events: num(r["events"]), Bytes: num(r["bytes"]),
			Batches: num(r["batches"]), Errors: num(r["errors"]), MaxIngestLagMs: num(r["lag"])}
		res.BySource = append(res.BySource, m)
		res.Totals.Events += m.Events
		res.Totals.Bytes += m.Bytes
		res.Totals.Batches += m.Batches
		res.Totals.Errors += m.Errors
		if m.MaxIngestLagMs > res.Totals.MaxIngestLagMs {
			res.Totals.MaxIngestLagMs = m.MaxIngestLagMs
		}
	}
	rows, err = a.R.Run(ctx, mk("SELECT hour, sum(a_events) AS events, sum(a_errors) AS errors"+
		" FROM %INNER% GROUP BY hour ORDER BY hour LIMIT 2200"))
	if err != nil {
		return res, err
	}
	for _, r := range rows {
		res.Series = append(res.Series, SeriesPoint{Hour: chTime(r["hour"]), Events: num(r["events"]), Errors: num(r["errors"])})
	}
	return res, nil
}

func (a CHAnalytics) QualitySummary(ctx context.Context, tenant string, tr TimeRange, sourceID string) (QualitySummary, error) {
	res := QualitySummary{From: tr.From, To: tr.To, Flags: []FlagCount{}, TopErrors: []ErrorCount{}}
	day := func() *qb {
		b := newQB(tenant)
		b.add("day >= toDate({t_from:" + chTimeType + "}, 'UTC') AND day < toDate({t_to:" + chTimeType + "}, 'UTC') + 1")
		b.params["t_from"] = tr.From.UTC().Format(chTimeLayout)
		b.params["t_to"] = tr.To.UTC().Format(chTimeLayout)
		if sourceID != "" {
			b.add("source_id = {source_id:UUID}")
			b.params["source_id"] = sourceID
		}
		return b
	}
	latest := func(b *qb, extra string) string {
		return "(SELECT argMax(events_total,(processing_version,revision)) AS q_total," +
			" argMax(events_valid,(processing_version,revision)) AS q_valid," +
			" argMax(events_rejected,(processing_version,revision)) AS q_rejected," +
			" argMax(events_with_flags,(processing_version,revision)) AS q_flagged," +
			" argMax(flag_count,(processing_version,revision)) AS q_flagcount, flag" +
			" FROM " + tblQuality + " WHERE " + b.whereSQL() + extra +
			" GROUP BY tenant_id, source_id, pipeline_id, day, flag)"
	}
	b := day()
	rows, err := a.R.Run(ctx, Query{Params: b.params, SQL: "SELECT sum(q_total) AS t, sum(q_valid) AS v," +
		" sum(q_rejected) AS r, sum(q_flagged) AS f FROM " + latest(b, " AND flag = ''")})
	if err != nil {
		return res, err
	}
	if len(rows) == 1 {
		res.EventsTotal, res.EventsValid = num(rows[0]["t"]), num(rows[0]["v"])
		res.EventsRejected, res.EventsWithFlags = num(rows[0]["r"]), num(rows[0]["f"])
	}
	b = day()
	if rows, err = a.R.Run(ctx, Query{Params: b.params, SQL: "SELECT flag, sum(q_flagcount) AS c FROM " +
		latest(b, " AND flag != ''") + " GROUP BY flag ORDER BY c DESC LIMIT 50"}); err != nil {
		return res, err
	}
	for _, r := range rows {
		res.Flags = append(res.Flags, FlagCount{Flag: str(r["flag"]), Count: num(r["c"])})
	}
	eb := newQB(tenant)
	eb.timeRange("occurred_at", tr)
	if sourceID != "" {
		eb.add("source_id = {source_id:UUID}")
		eb.params["source_id"] = sourceID
	}
	if rows, err = a.R.Run(ctx, Query{Params: eb.params, SQL: "SELECT error_code, stage, uniqExact(event_id, processing_version) AS c FROM " +
		tblProcErrors + " WHERE " + eb.whereSQL() + " GROUP BY error_code, stage ORDER BY c DESC LIMIT 20"}); err != nil {
		return res, err
	}
	for _, r := range rows {
		res.TopErrors = append(res.TopErrors, ErrorCount{ErrorCode: str(r["error_code"]), Stage: str(r["stage"]), Count: num(r["c"])})
	}
	return res, nil
}

func str(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return x.String()
	default:
		return fmt.Sprint(x)
	}
}

func num(v any) int64 {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return int64(f)
	case float64:
		return int64(x)
	case string:
		var i int64
		fmt.Sscan(x, &i)
		return i
	}
	return 0
}

// chTime parses ClickHouse JSON date/datetime strings (UTC).
func chTime(v any) time.Time {
	s, _ := v.(string)
	for _, l := range []string{"2006-01-02 15:04:05.000", "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.ParseInLocation(l, s, time.UTC); err == nil {
			return t
		}
	}
	return time.Time{}
}
