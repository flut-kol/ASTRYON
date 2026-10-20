package replay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const chRows = `{"tenant_id":"00000000-0000-4000-8000-0000000000a1","source_id":"00000000-0000-4000-8000-000000000001","batch_id":"00000000-0000-4000-8000-000000000002","job_id":"00000000-0000-4000-8000-000000000003","event_id":"00000000-0000-4000-8000-000000000004","processing_version":2,"pipeline_version":3,"occurred_at":"2026-10-20 11:00:00.123","stage":"normalize","error_code":"BAD_VALUE","error_message":"masked failure","sample_payload":"{\"safe\":true}"}
{"tenant_id":"00000000-0000-4000-8000-0000000000a1","source_id":"00000000-0000-4000-8000-000000000011","batch_id":"00000000-0000-4000-8000-000000000012","job_id":"00000000-0000-4000-8000-000000000013","event_id":"00000000-0000-4000-8000-000000000014","processing_version":1,"pipeline_version":1,"occurred_at":"2026-10-20 10:00:00.000","stage":"validate","error_code":"SCHEMA","error_message":"masked","sample_payload":""}
`

func TestClickHouseStoreTenantBoundPaginationAndPayloadMinimization(t *testing.T) {
	var query string
	var params url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query, params = string(body), r.URL.Query()
		_, _ = io.WriteString(w, chRows)
	}))
	defer server.Close()
	store := &ClickHouseStore{URL: server.URL, Database: "astryon"}

	page, err := store.ListQuarantine(context.Background(), tenantA, "", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.NextCursor == "" || page.Records[0].Original != nil {
		t.Fatalf("page=%+v", page)
	}
	if params.Get("param_p_tenant") != tenantA || params.Get("readonly") != "1" ||
		!strings.Contains(query, "tenant_id = {p_tenant:UUID}") || !strings.Contains(query, "'' AS sample_payload") ||
		!strings.Contains(query, "FROM processing_errors_distributed") || !strings.Contains(query, "LIMIT 2 FORMAT") {
		t.Fatalf("params=%v query=%s", params, query)
	}
	if got := page.Records[0].QuarantinedAt; got != "2026-10-20T11:00:00.123Z" {
		t.Fatalf("time=%q", got)
	}

	if _, err := store.ListQuarantine(context.Background(), tenantA, page.NextCursor, 1, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "sample_payload") || strings.Contains(query, "'' AS sample_payload") ||
		!strings.Contains(query, "(occurred_at, source_id") || params.Get("param_p_event") == "" {
		t.Fatalf("params=%v query=%s", params, query)
	}
	if _, err := store.ListQuarantine(context.Background(), tenantB, page.NextCursor, 1, false); err != ErrInvalidCursor {
		t.Fatalf("cross-tenant cursor: %v", err)
	}
}

func TestClickHouseStorePayloadAndConfigurationSafety(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, chRows[:strings.Index(chRows, "\n")+1])
	}))
	defer server.Close()
	store := &ClickHouseStore{URL: server.URL, Table: "processing_errors_distributed"}
	page, err := store.ListQuarantine(context.Background(), tenantA, "", 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(page.Records[0].Original); got != `{"safe":true}` || page.Records[0].PayloadKind != "masked_sample" {
		t.Fatalf("payload=%s kind=%q", got, page.Records[0].PayloadKind)
	}
	store.Table = "errors; DROP TABLE errors"
	if _, err := store.ListQuarantine(context.Background(), tenantA, "", 1, false); err == nil {
		t.Fatal("unsafe table identifier accepted")
	}
}
