package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const chTimeLayout = "2006-01-02 15:04:05.000"

// CH reads events_normalized over the ClickHouse HTTP interface: readonly=1, bound param_* parameters
// only (no value is ever concatenated into SQL; the table name comes from trusted config).
type CH struct {
	URL, Database, User, Password string
	Table                         string // default events_normalized_distributed
	HTTP                          *http.Client
	MaxPageBytes                  int64 // response cap per query, default 128 MiB
}

var _ EventReader = (*CH)(nil)

func (c *CH) table() string {
	if c.Table != "" {
		return c.Table
	}
	return "events_normalized_distributed"
}

// do runs one query and returns the (size-capped) response body reader.
func (c *CH) do(ctx context.Context, q string, params map[string]string) (io.ReadCloser, error) {
	v := url.Values{"query": {q}, "readonly": {"1"}}
	if c.Database != "" {
		v.Set("database", c.Database)
	}
	for k, val := range params {
		v.Set("param_"+k, val)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/?"+v.Encode(), nil)
	if err != nil {
		return nil, err
	}
	if c.User != "" {
		req.Header.Set("X-ClickHouse-User", c.User)
		req.Header.Set("X-ClickHouse-Key", c.Password)
	}
	cl := c.HTTP
	if cl == nil {
		cl = &http.Client{Timeout: 60 * time.Second}
	}
	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("clickhouse: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	max := c.MaxPageBytes
	if max <= 0 {
		max = 128 << 20
	}
	return struct {
		io.Reader
		io.Closer
	}{io.LimitReader(resp.Body, max), resp.Body}, nil
}

func (c *CH) BaseVersion(ctx context.Context, tenantID, sourceID string, below uint32) (uint32, error) {
	body, err := c.do(ctx, "SELECT max(processing_version) FROM "+c.table()+
		" WHERE tenant_id = {tenant:UUID} AND source_id = {source:UUID} AND processing_version < {below:UInt32}",
		map[string]string{"tenant": tenantID, "source": sourceID, "below": strconv.FormatUint(uint64(below), 10)})
	if err != nil {
		return 0, err
	}
	defer body.Close()
	b, _ := io.ReadAll(io.LimitReader(body, 64))
	n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: bad max(processing_version) %q", b)
	}
	return uint32(n), nil
}

type chRow struct {
	EventID         string            `json:"event_id"`
	TenantID        string            `json:"tenant_id"`
	BatchID         string            `json:"batch_id"`
	PipelineID      string            `json:"pipeline_id"`
	PipelineVersion uint32            `json:"pipeline_version"`
	EventType       string            `json:"event_type"`
	EventTime       string            `json:"event_time"`
	IngestedAt      string            `json:"ingested_at"`
	EntityID        string            `json:"entity_id"`
	Attributes      map[string]string `json:"attributes"`
	Payload         string            `json:"payload"`
	RawRef          string            `json:"raw_ref"`
	QualityFlags    []string          `json:"quality_flags"`
}

// Page: LIMIT 1 BY dedups on (tenant, source, event_id, processing_version) as everywhere else; keyset
// paging on (event_time, event_id) keeps memory at O(Limit) rows regardless of the range size.
func (c *CH) Page(ctx context.Context, q PageQuery) ([]SourceEvent, error) {
	params := map[string]string{"tenant": q.TenantID, "source": q.SourceID,
		"pv": strconv.FormatUint(uint64(q.Version), 10), "from": chTime(q.From), "to": chTime(q.To),
		"lim": strconv.Itoa(q.Limit)}
	cursor := ""
	if q.After != nil {
		cursor = " AND (event_time, event_id) > ({ct:DateTime64(3,'UTC')}, {cid:UUID})"
		params["ct"], params["cid"] = chTime(q.After.EventTime), q.After.EventID
	}
	sql := "SELECT tenant_id, event_id, batch_id, pipeline_id, pipeline_version, event_type, event_time, ingested_at," +
		" entity_id, attributes, payload, raw_ref, quality_flags FROM " + c.table() +
		" WHERE tenant_id = {tenant:UUID} AND source_id = {source:UUID} AND processing_version = {pv:UInt32}" +
		" AND event_time >= {from:DateTime64(3,'UTC')} AND event_time < {to:DateTime64(3,'UTC')}" + cursor +
		" ORDER BY event_time, event_id LIMIT 1 BY (tenant_id, source_id, event_id, processing_version)" +
		" LIMIT {lim:UInt32} FORMAT JSONEachRow"
	body, err := c.do(ctx, sql, params)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	dec := json.NewDecoder(body)
	var out []SourceEvent
	for {
		var r chRow
		if err := dec.Decode(&r); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("clickhouse page decode (page larger than MaxPageBytes? lower the page size): %w", err)
		}
		et, err := time.ParseInLocation(chTimeLayout, r.EventTime, time.UTC)
		if err != nil {
			return nil, fmt.Errorf("clickhouse event_time %q: %w", r.EventTime, err)
		}
		ia, err := time.ParseInLocation(chTimeLayout, r.IngestedAt, time.UTC)
		if err != nil {
			return nil, fmt.Errorf("clickhouse ingested_at %q: %w", r.IngestedAt, err)
		}
		out = append(out, SourceEvent{EventID: r.EventID, TenantID: r.TenantID, BatchID: r.BatchID, PipelineID: r.PipelineID,
			PipelineVersion: r.PipelineVersion, EventType: r.EventType, EventTime: et, IngestedAt: ia, EntityID: r.EntityID,
			Attributes: r.Attributes, Payload: r.Payload, RawRef: r.RawRef, QualityFlags: r.QualityFlags})
	}
	return out, nil
}

func (c *CH) Existing(ctx context.Context, tenantID, sourceID string, version uint32, from, to time.Time, ids []string) (map[string]bool, error) {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = "'" + id + "'" // ids come from ClickHouse UUID columns; bound as an Array(UUID) parameter
	}
	body, err := c.do(ctx, "SELECT event_id FROM "+c.table()+
		" WHERE tenant_id = {tenant:UUID} AND source_id = {source:UUID} AND processing_version = {pv:UInt32}"+
		" AND event_time >= {from:DateTime64(3,'UTC')} AND event_time <= {to:DateTime64(3,'UTC')}"+
		" AND event_id IN {ids:Array(UUID)} LIMIT 1 BY event_id FORMAT JSONEachRow",
		map[string]string{"tenant": tenantID, "source": sourceID, "pv": strconv.FormatUint(uint64(version), 10),
			"from": chTime(from), "to": chTime(to), "ids": "[" + strings.Join(quoted, ",") + "]"})
	if err != nil {
		return nil, err
	}
	defer body.Close()
	dec := json.NewDecoder(body)
	have := map[string]bool{}
	for {
		var r struct {
			EventID string `json:"event_id"`
		}
		if err := dec.Decode(&r); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		have[strings.ToLower(r.EventID)] = true
	}
	return have, nil
}

func chTime(t time.Time) string { return t.UTC().Format(chTimeLayout) }
