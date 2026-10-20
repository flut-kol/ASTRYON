package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidCursor = errors.New("invalid quarantine cursor")

var clickHouseIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

const clickHouseTime = "2006-01-02 15:04:05.000"

const (
	maxErrorMessageBytes = 8 << 10
	maxSamplePayloadBytes = 64 << 10
	maxClickHouseResponse = 12 << 20
)

// ClickHouseStore reads the bounded deviation journal already defined by
// db/clickhouse/migrations/0002_processing_errors.up.sql. It never reads raw_ref:
// payload-authorized callers can request only the table's bounded/masked sample.
type ClickHouseStore struct {
	URL, Database, User, Password string
	Table                         string
	HTTP                          *http.Client
	MaxExecutionSeconds           int
}

type quarantineCursor struct {
	TenantHash string `json:"h"`
	Time       string `json:"t"`
	SourceID   string `json:"s"`
	BatchID    string `json:"b"`
	JobID      string `json:"j"`
	EventID    string `json:"e"`
	Processing uint32 `json:"p"`
	Pipeline   uint32 `json:"v"`
	Code       string `json:"c"`
	Stage      string `json:"g"`
}

type clickHouseRow struct {
	TenantID      string `json:"tenant_id"`
	SourceID      string `json:"source_id"`
	BatchID       string `json:"batch_id"`
	JobID         string `json:"job_id"`
	EventID       string `json:"event_id"`
	Processing    uint32 `json:"processing_version"`
	Pipeline      uint32 `json:"pipeline_version"`
	OccurredAt    string `json:"occurred_at"`
	Stage         string `json:"stage"`
	ErrorCode     string `json:"error_code"`
	ErrorMessage  string `json:"error_message"`
	SamplePayload string `json:"sample_payload"`
}

func (s *ClickHouseStore) ListQuarantine(ctx context.Context, tenantID, rawCursor string, limit int, includePayload bool) (BrowserPage, error) {
	if !tenantUUID.MatchString(tenantID) || limit < 1 || limit > maxPageSize {
		return BrowserPage{}, ErrInvalidPage
	}
	table := s.Table
	if table == "" {
		table = "processing_errors_distributed"
	}
	if !clickHouseIdentifier.MatchString(table) {
		return BrowserPage{}, fmt.Errorf("invalid ClickHouse table %q", table)
	}

	params := url.Values{"param_p_tenant": {tenantID}}
	where := "tenant_id = {p_tenant:UUID}"
	if rawCursor != "" {
		cur, err := decodeQuarantineCursor(rawCursor, tenantID)
		if err != nil {
			return BrowserPage{}, err
		}
		where += ` AND (occurred_at, source_id, batch_id, job_id, event_id, processing_version, pipeline_version, error_code, stage) <
({p_time:DateTime64(3, 'UTC')}, {p_source:UUID}, {p_batch:UUID}, {p_job:UUID}, {p_event:UUID}, {p_processing:UInt32}, {p_pipeline:UInt32}, {p_code:String}, {p_stage:String})`
		params.Set("param_p_time", cur.Time)
		params.Set("param_p_source", cur.SourceID)
		params.Set("param_p_batch", cur.BatchID)
		params.Set("param_p_job", cur.JobID)
		params.Set("param_p_event", cur.EventID)
		params.Set("param_p_processing", strconv.FormatUint(uint64(cur.Processing), 10))
		params.Set("param_p_pipeline", strconv.FormatUint(uint64(cur.Pipeline), 10))
		params.Set("param_p_code", cur.Code)
		params.Set("param_p_stage", cur.Stage)
	}
	payloadColumn := "'' AS sample_payload"
	if includePayload {
		payloadColumn = fmt.Sprintf("substring(sample_payload, 1, %d) AS sample_payload", maxSamplePayloadBytes)
	}
	query := fmt.Sprintf(`SELECT toString(tenant_id) AS tenant_id, toString(source_id) AS source_id,
toString(batch_id) AS batch_id, toString(job_id) AS job_id, toString(event_id) AS event_id,
processing_version, pipeline_version, toString(occurred_at) AS occurred_at, stage, error_code,
	substring(error_message, 1, %d) AS error_message, %s
FROM %s WHERE %s
ORDER BY occurred_at DESC, source_id DESC, batch_id DESC, job_id DESC, event_id DESC,
processing_version DESC, pipeline_version DESC, error_code DESC, stage DESC
LIMIT 1 BY tenant_id, occurred_at, source_id, batch_id, job_id, event_id,
processing_version, pipeline_version, error_code, stage
LIMIT %d FORMAT JSONEachRow`, maxErrorMessageBytes, payloadColumn, table, where, limit+1)

	rows, err := s.run(ctx, query, params)
	if err != nil {
		return BrowserPage{}, err
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		next = encodeQuarantineCursor(cursorFromRow(last, tenantID))
	}
	records := make([]QuarantineRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, recordFromClickHouse(row, includePayload))
	}
	return BrowserPage{Records: records, NextCursor: next}, nil
}

func (s *ClickHouseStore) run(ctx context.Context, query string, params url.Values) ([]clickHouseRow, error) {
	if s.URL == "" {
		return nil, errors.New("ClickHouse URL is required")
	}
	v := url.Values{}
	for key, values := range params {
		v[key] = values
	}
	if s.Database != "" {
		v.Set("database", s.Database)
	}
	seconds := s.MaxExecutionSeconds
	if seconds <= 0 {
		seconds = 10
	}
	v.Set("readonly", "1")
	v.Set("max_execution_time", strconv.Itoa(seconds))
	v.Set("max_result_rows", strconv.Itoa(maxPageSize+1))
	v.Set("result_overflow_mode", "throw")
	v.Set("max_memory_usage", strconv.FormatInt(64<<20, 10))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.URL, "/")+"/?"+v.Encode(), strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	if s.User != "" {
		req.Header.Set("X-ClickHouse-User", s.User)
		req.Header.Set("X-ClickHouse-Key", s.Password)
	}
	client := s.HTTP
	if client == nil {
		client = &http.Client{Timeout: time.Duration(seconds)*time.Second + 5*time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("clickhouse %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxClickHouseResponse+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxClickHouseResponse {
		return nil, errors.New("ClickHouse response exceeds quarantine byte limit")
	}
	rows := make([]clickHouseRow, 0)
	dec := json.NewDecoder(bytes.NewReader(body))
	for dec.More() {
		var row clickHouseRow
		if err := dec.Decode(&row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func recordFromClickHouse(row clickHouseRow, includePayload bool) QuarantineRecord {
	key, _ := json.Marshal(rowCursor(row, row.TenantID))
	id := sha256.Sum256(key)
	record := QuarantineRecord{
		ID: fmt.Sprintf("%x", id[:16]), TenantID: row.TenantID, SourceID: row.SourceID,
		BatchID: row.BatchID, JobID: row.JobID, EventID: row.EventID,
		ProcessingVersion: row.Processing, PipelineVersion: row.Pipeline,
		ReasonCode: row.ErrorCode, Reason: boundedUTF8(row.ErrorMessage, maxErrorMessageBytes), Stage: row.Stage,
		QuarantinedAt: clickHouseTimeToRFC3339(row.OccurredAt),
	}
	row.SamplePayload = boundedUTF8(row.SamplePayload, maxSamplePayloadBytes)
	if includePayload && row.SamplePayload != "" {
		record.PayloadKind = "masked_sample"
		if json.Valid([]byte(row.SamplePayload)) {
			record.Original = json.RawMessage(row.SamplePayload)
		} else {
			encoded, _ := json.Marshal(row.SamplePayload)
			record.Original = encoded
		}
	}
	return record
}

func boundedUTF8(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return strings.ToValidUTF8(value[:limit], "�")
}

func clickHouseTimeToRFC3339(value string) string {
	parsed, err := time.ParseInLocation(clickHouseTime, value, time.UTC)
	if err != nil {
		return value
	}
	return parsed.Format(time.RFC3339Nano)
}

func tenantCursorHash(tenantID string) string {
	sum := sha256.Sum256([]byte(tenantID))
	return base64.RawURLEncoding.EncodeToString(sum[:12])
}

func rowCursor(row clickHouseRow, tenantID string) quarantineCursor {
	return quarantineCursor{TenantHash: tenantCursorHash(tenantID), Time: row.OccurredAt, SourceID: row.SourceID,
		BatchID: row.BatchID, JobID: row.JobID, EventID: row.EventID, Processing: row.Processing,
		Pipeline: row.Pipeline, Code: row.ErrorCode, Stage: row.Stage}
}

func cursorFromRow(row clickHouseRow, tenantID string) quarantineCursor {
	return rowCursor(row, tenantID)
}

func encodeQuarantineCursor(cur quarantineCursor) string {
	body, _ := json.Marshal(cur)
	return base64.RawURLEncoding.EncodeToString(body)
}

func decodeQuarantineCursor(raw, tenantID string) (quarantineCursor, error) {
	var cur quarantineCursor
	body, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(body) > 2048 || json.Unmarshal(body, &cur) != nil ||
		cur.TenantHash != tenantCursorHash(tenantID) || !tenantUUID.MatchString(cur.SourceID) ||
		!tenantUUID.MatchString(cur.BatchID) || !tenantUUID.MatchString(cur.JobID) || !tenantUUID.MatchString(cur.EventID) ||
		len(cur.Code) > 256 || len(cur.Stage) > 256 {
		return quarantineCursor{}, ErrInvalidCursor
	}
	if _, err := time.ParseInLocation(clickHouseTime, cur.Time, time.UTC); err != nil {
		return quarantineCursor{}, ErrInvalidCursor
	}
	return cur, nil
}
