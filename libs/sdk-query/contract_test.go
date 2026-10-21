package queryclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The tenant comes only from the access token: the generated client must expose no tenant input and must never send one.
func TestClientSendsNoTenantAndExposesStructuredError(t *testing.T) {
	var gotAuth, gotBody string
	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotAuth, gotBody, gotHeaders = r.Header.Get("Authorization"), string(b), r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"slow down","trace_id":"t-1"}}`))
	}))
	defer srv.Close()

	c, err := NewClientWithResponses(srv.URL, WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
		r.Header.Set("Authorization", "Bearer test-token")
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	resp, err := c.QueryEventsWithResponse(context.Background(), &QueryEventsParams{}, EventsRequest{From: now.Add(-time.Hour), To: now})
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
	if strings.Contains(strings.ToLower(gotBody), "tenant") {
		t.Fatalf("client sent tenant in body: %s", gotBody)
	}
	if resp.JSONDefault == nil || resp.JSONDefault.Error.Code != "rate_limited" || resp.JSONDefault.Error.TraceId != "t-1" {
		t.Fatalf("structured error not exposed: %+v", resp.JSONDefault)
	}
}
