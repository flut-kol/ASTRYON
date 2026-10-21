package connectormanagerclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openapi_types "github.com/oapi-codegen/runtime/types"
)

// The tenant comes only from the access token: the generated client must expose no tenant input and must never send one.
func TestClientSendsNoTenantAndExposesStructuredError(t *testing.T) {
	var gotAuth, gotQuery string
	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotQuery, gotHeaders = r.Header.Get("Authorization"), r.URL.RawQuery, r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"not_found","message":"no","trace_id":"t-3"}`))
	}))
	defer srv.Close()

	c, err := NewClientWithResponses(srv.URL, WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
		r.Header.Set("Authorization", "Bearer test-token")
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.GetConnectorWithResponse(context.Background(), openapi_types.UUID{1})
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("authorization header: %q", gotAuth)
	}
	for k := range gotHeaders {
		if strings.Contains(strings.ToLower(k), "tenant") {
			t.Fatalf("client sent tenant header %q", k)
		}
	}
	if strings.Contains(strings.ToLower(gotQuery), "tenant") {
		t.Fatalf("client sent tenant in query: %s", gotQuery)
	}
	if resp.JSON404 == nil || !(resp.JSON404.Code == "not_found" && resp.JSON404.TraceId == "t-3") {
		t.Fatalf("structured error not exposed: %+v", resp.JSON404)
	}
}
