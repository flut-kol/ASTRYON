package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	maxReplayRowsRead     = 1_000_000
	maxReplayPlanResponse = 4 << 10
)

type replayCountRow struct {
	CandidateCount uint64 `json:"candidate_count"`
}

// PlanReplay performs a bounded, read-only estimate. It never publishes a
// replay job and deliberately returns only the number of matching deviations.
func (s *ClickHouseStore) PlanReplay(ctx context.Context, tenantID string, request ReplayDryRunRequest) (ReplayDryRunPlan, error) {
	if !tenantUUID.MatchString(tenantID) || request.Validate() != nil {
		return ReplayDryRunPlan{}, ErrInvalidReplayRequest
	}
	table := s.Table
	if table == "" {
		table = "processing_errors_distributed"
	}
	if !clickHouseIdentifier.MatchString(table) {
		return ReplayDryRunPlan{}, fmt.Errorf("invalid ClickHouse table %q", table)
	}
	params := url.Values{
		"param_p_tenant":   {strings.ToLower(tenantID)},
		"param_p_source":   {strings.ToLower(request.SourceID)},
		"param_p_from":     {request.OccurredFrom.UTC().Format(clickHouseTime)},
		"param_p_to":       {request.OccurredTo.UTC().Format(clickHouseTime)},
		"param_p_pipeline": {strconv.FormatUint(uint64(request.PipelineVersion), 10)},
	}
	query := fmt.Sprintf(`SELECT count() AS candidate_count
FROM
(
    SELECT 1
    FROM %s
    WHERE tenant_id = {p_tenant:UUID}
      AND source_id = {p_source:UUID}
      AND occurred_at >= {p_from:DateTime64(3, 'UTC')}
      AND occurred_at < {p_to:DateTime64(3, 'UTC')}
      AND pipeline_version = {p_pipeline:UInt32}
    LIMIT %d
)
FORMAT JSONEachRow`, table, maxReplayCandidates+1)
	count, err := s.runReplayCount(ctx, query, params)
	if err != nil {
		return ReplayDryRunPlan{}, err
	}
	if count > maxReplayCandidates {
		return ReplayDryRunPlan{CandidateCount: maxReplayCandidates, CandidateCountIsLowerBound: true}, nil
	}
	return ReplayDryRunPlan{CandidateCount: count}, nil
}

func (s *ClickHouseStore) runReplayCount(ctx context.Context, query string, params url.Values) (uint64, error) {
	if s.URL == "" {
		return 0, errors.New("ClickHouse URL is required")
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
	v.Set("max_result_rows", "1")
	v.Set("result_overflow_mode", "throw")
	v.Set("max_memory_usage", strconv.FormatInt(64<<20, 10))
	v.Set("max_rows_to_read", strconv.Itoa(maxReplayRowsRead))
	v.Set("max_threads", "2")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.URL, "/")+"/?"+v.Encode(), strings.NewReader(query)) // #nosec G704 -- URL comes from operator configuration, not request input
	if err != nil {
		return 0, err
	}
	if s.User != "" {
		req.Header.Set("X-ClickHouse-User", s.User)
		req.Header.Set("X-ClickHouse-Key", s.Password)
	}
	client := s.HTTP
	if client == nil {
		client = &http.Client{Timeout: time.Duration(seconds)*time.Second + 5*time.Second}
	}
	resp, err := client.Do(req) // #nosec G704 -- URL comes from operator configuration, not request input
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("clickhouse %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxReplayPlanResponse+1))
	if err != nil {
		return 0, err
	}
	if len(body) > maxReplayPlanResponse {
		return 0, errors.New("ClickHouse replay plan response exceeds byte limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var row replayCountRow
	if err := decoder.Decode(&row); err != nil {
		return 0, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return 0, errors.New("ClickHouse replay plan returned multiple rows")
	}
	return row.CandidateCount, nil
}
