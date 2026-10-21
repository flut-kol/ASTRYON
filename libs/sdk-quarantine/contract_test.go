package quarantineclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The tenant comes only from the access token: the generated client must expose no tenant input and must never send one.
func TestClientSendsNoTenantAndExposesStructuredError(t *testing.T) {
	var gotAuth, gotQuery string
	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotQuery, gotHeaders = r.Header.Get("Authorization"), r.URL.RawQuery, r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"forbidden","message":"no scope","trace_id":"t-2","details":null}}`))
	}))
	defer srv.Close()

	c, err := NewClientWithResponses(srv.URL, WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
		r.Header.Set("Authorization", "Bearer test-token")
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	limit := 10
	resp, err := c.ListQuarantineWithResponse(context.Background(), &ListQuarantineParams{Limit: &limit})
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
	if resp.JSONDefault == nil || resp.JSONDefault.Error.Code != "forbidden" || resp.JSONDefault.Error.TraceId != "t-2" {
		t.Fatalf("structured error not exposed: %+v", resp.JSONDefault)
	}
}
