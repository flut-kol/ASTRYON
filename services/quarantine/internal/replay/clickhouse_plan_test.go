package replay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func validReplayRequest() ReplayDryRunRequest {
	return ReplayDryRunRequest{
		SourceID:        "00000000-0000-4000-8000-000000000001",
		OccurredFrom:    time.Date(2026, 10, 1, 7, 0, 0, 0, time.UTC),
		OccurredTo:      time.Date(2026, 10, 2, 7, 0, 0, 0, time.UTC),
		PipelineVersion: 2,
	}
}

func TestClickHouseReplayPlanIsTenantBoundAndReadOnly(t *testing.T) {
	var query string
	var params url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query, params = string(body), r.URL.Query()
		_, _ = io.WriteString(w, `{"candidate_count":42}`+"\n")
	}))
	defer server.Close()
	store := &ClickHouseStore{URL: server.URL, Database: "astryon", User: "reader", Password: "secret"}
	plan, err := store.PlanReplay(context.Background(), tenantA, validReplayRequest())
	if err != nil {
		t.Fatal(err)
	}
	if plan.CandidateCount != 42 || plan.CandidateCountIsLowerBound {
		t.Fatalf("plan=%+v", plan)
	}
	for key, want := range map[string]string{
		"param_p_tenant": tenantA, "param_p_source": validReplayRequest().SourceID,
		"param_p_from": "2026-10-01 07:00:00.000", "param_p_to": "2026-10-02 07:00:00.000",
		"param_p_pipeline": "2", "readonly": "1", "max_result_rows": "1",
		"max_rows_to_read": "1000000", "max_threads": "2", "database": "astryon",
	} {
		if got := params.Get(key); got != want {
			t.Fatalf("%s=%q, want %q; params=%v", key, got, want, params)
		}
	}
	for _, fragment := range []string{
		"FROM processing_errors_distributed", "tenant_id = {p_tenant:UUID}", "source_id = {p_source:UUID}",
		"occurred_at >= {p_from:DateTime64", "occurred_at < {p_to:DateTime64", "pipeline_version = {p_pipeline:UInt32}",
		"LIMIT 10001", "FORMAT JSONEachRow",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query missing %q: %s", fragment, query)
		}
	}
}

func TestClickHouseReplayPlanClampsEstimateAndRejectsUnsafeInputs(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = io.WriteString(w, `{"candidate_count":10001}`+"\n")
	}))
	defer server.Close()
	store := &ClickHouseStore{URL: server.URL}
	plan, err := store.PlanReplay(context.Background(), tenantA, validReplayRequest())
	if err != nil {
		t.Fatal(err)
	}
	if plan.CandidateCount != maxReplayCandidates || !plan.CandidateCountIsLowerBound {
		t.Fatalf("plan=%+v", plan)
	}
	store.Table = "errors; DROP TABLE errors"
	if _, err := store.PlanReplay(context.Background(), tenantA, validReplayRequest()); err == nil {
		t.Fatal("unsafe table identifier accepted")
	}
	store.Table = ""
	if _, err := store.PlanReplay(context.Background(), tenantB, ReplayDryRunRequest{}); err == nil {
		t.Fatal("invalid request accepted")
	}
	if calls != 1 {
		t.Fatalf("backend calls=%d", calls)
	}
}

func TestClickHouseReplayPlanFailsClosedOnBackendErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal storage detail", http.StatusInternalServerError)
	}))
	defer server.Close()
	store := &ClickHouseStore{URL: server.URL}
	if _, err := store.PlanReplay(context.Background(), tenantA, validReplayRequest()); err == nil {
		t.Fatal("backend failure accepted")
	}
}
