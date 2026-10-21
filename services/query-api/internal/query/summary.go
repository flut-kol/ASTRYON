package query

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Summaries read the revisioned aggregate tables (0003_aggregates). Readers take argMax(.., revision) per key,
// never FINAL or merge-time dedup, so recomputes and replays do not double metrics (§5.3).

func (s *Server) summaryHandlers(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/metrics/summary", s.wrapGET("metrics_summary", s.metricsSummary))
	mux.HandleFunc("GET /v1/quality/summary", s.wrapGET("quality_summary", s.qualitySummary))
}

// wrapGET adapts query-string parameters (from, to, processing_version, source_id) to the common pipeline.
func (s *Server) wrapGET(group string, h func(context.Context, Principal, Filter) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.serve(group, w, r, func(ctx context.Context, p Principal, _ []byte) (any, error) {
			f, err := parseFilterQuery(r.URL.Query())
			if err != nil {
				return nil, err
			}
			return h(ctx, p, f)
		})
	}
}

func parseFilterQuery(q url.Values) (Filter, error) {
	var f Filter
	for k := range q {
		switch k {
		case "from", "to", "processing_version", "source_id":
		default:
			return f, bad("invalid_request", "unknown parameter: "+k)
		}
	}
	var err error
	if f.From, err = time.Parse(time.RFC3339, q.Get("from")); err != nil {
		return f, bad("invalid_range", "from must be RFC3339")
	}
	if f.To, err = time.Parse(time.RFC3339, q.Get("to")); err != nil {
		return f, bad("invalid_range", "to must be RFC3339")
	}
	if raw := q.Get("processing_version"); raw != "" || q.Has("processing_version") {
		pv, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return f, bad("invalid_request", "processing_version must be an unsigned integer")
		}
		v := uint32(pv)
		f.ProcessingVersion = &v
	}
	f.SourceID = q.Get("source_id")
	return f, nil
}

// summaryWhere builds the shared predicate on an aggregate table. col is the bucket column; lo/hi are bound literals.
func summaryWhere(f Filter, p Principal, col, typ, lo, hi string) (*where, error) {
	if err := f.validateRange(); err != nil {
		return nil, err
	}
	if f.ProcessingVersion == nil {
		return nil, bad("invalid_request", "processing_version is required")
	}
	if f.SourceID != "" && !uuidRe.MatchString(f.SourceID) {
		return nil, bad("invalid_request", "source_id must be a UUID")
	}
	w := &where{params: url.Values{}}
	w.add("tenant_id = {p_tenant:UUID}", "p_tenant", p.TenantID)
	w.add(fmt.Sprintf("%s >= {p_lo:%s}", col, typ), "p_lo", lo)
	w.add(fmt.Sprintf("%s <= {p_hi:%s}", col, typ), "p_hi", hi)
	w.add("processing_version = {p_pv:UInt32}", "p_pv", fmt.Sprint(*f.ProcessingVersion))
	if f.SourceID != "" {
		w.add("source_id = {p_source:UUID}", "p_source", f.SourceID)
	}
	return w, nil
}

func (s *Server) metricsSummary(ctx context.Context, p Principal, f Filter) (any, error) {
	if err := s.resolveVersion(ctx, p, &f); err != nil {
		return nil, err
	}
	w, err := summaryWhere(f, p, "hour", "DateTime('UTC')",
		f.From.UTC().Truncate(time.Hour).Format(time.DateTime), f.To.UTC().Add(-time.Nanosecond).Truncate(time.Hour).Format(time.DateTime))
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf(`SELECT source_id, sum(events_count) AS events_count, sum(bytes_count) AS bytes_count, sum(batches_count) AS batches_count,
 sum(errors_count) AS errors_count, max(max_ingest_lag_ms) AS max_ingest_lag_ms
FROM (SELECT tenant_id, source_id, processing_version, hour,
  argMax(events_count, revision) AS events_count, argMax(bytes_count, revision) AS bytes_count,
  argMax(batches_count, revision) AS batches_count, argMax(errors_count, revision) AS errors_count,
  argMax(max_ingest_lag_ms, revision) AS max_ingest_lag_ms
  FROM source_metrics_hourly WHERE %s GROUP BY tenant_id, source_id, processing_version, hour)
GROUP BY source_id ORDER BY source_id LIMIT %d FORMAT JSONEachRow`, strings.Join(w.sql, " AND "), MaxLimit)
	rows, err := s.run(ctx, q, w.params)
	if err != nil {
		return nil, err
	}
	return map[string]any{"sources": rows}, nil
}

func (s *Server) qualitySummary(ctx context.Context, p Principal, f Filter) (any, error) {
	if err := s.resolveVersion(ctx, p, &f); err != nil {
		return nil, err
	}
	w, err := summaryWhere(f, p, "day", "Date",
		f.From.UTC().Format(time.DateOnly), f.To.UTC().Add(-time.Nanosecond).Format(time.DateOnly))
	if err != nil {
		return nil, err
	}
	pred := strings.Join(w.sql, " AND ")
	latest := fmt.Sprintf(`SELECT tenant_id, source_id, pipeline_id, processing_version, day, flag,
  argMax(events_total, revision) AS events_total, argMax(events_valid, revision) AS events_valid,
  argMax(events_rejected, revision) AS events_rejected, argMax(events_with_flags, revision) AS events_with_flags,
  argMax(flag_count, revision) AS flag_count
  FROM processing_quality_daily WHERE %s GROUP BY tenant_id, source_id, pipeline_id, processing_version, day, flag`, pred)
	totals, err := s.run(ctx, fmt.Sprintf(`SELECT source_id, sum(events_total) AS events_total, sum(events_valid) AS events_valid,
 sum(events_rejected) AS events_rejected, sum(events_with_flags) AS events_with_flags
FROM (%s) WHERE flag = '' GROUP BY source_id ORDER BY source_id LIMIT %d FORMAT JSONEachRow`, latest, MaxLimit), w.params)
	if err != nil {
		return nil, err
	}
	flags, err := s.run(ctx, fmt.Sprintf(`SELECT source_id, flag, sum(flag_count) AS count
FROM (%s) WHERE flag != '' GROUP BY source_id, flag ORDER BY source_id, flag LIMIT %d FORMAT JSONEachRow`, latest, MaxLimit), w.params)
	if err != nil {
		return nil, err
	}
	return map[string]any{"sources": totals, "flags": flags}, nil
}
