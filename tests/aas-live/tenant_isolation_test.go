package aaslive

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	queryclient "github.com/astryon/sdk-query"
)

const tenantB = "0b2c3d4e-2222-4333-8444-555566667777"

// TestLiveQueryAPITenantIsolation drives query-api with the generated client (libs/sdk-query) and real AAS tokens of two
// tenants. ClickHouse is a recording stub; the assertion is that each token's own tenant, and nothing else, reaches it,
// even when the caller smuggles a tenant through headers or the body.
func TestLiveQueryAPITenantIsolation(t *testing.T) {
	l := newLive(t)
	l.newScope(scopeQuery)
	a := l.newClient(suffix(), tenantA, 0, scopeQuery)
	b := l.newClient(suffix(), tenantB, 0, scopeQuery)

	var mu sync.Mutex
	var tenants []string
	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tenants = append(tenants, r.URL.Query().Get("param_p_tenant"))
		mu.Unlock()
	}))
	defer ch.Close()
	addr, logs := startQueryAPI(t, l, a.ID+","+b.ID, ch.URL)

	call := func(c *client, smuggle string) {
		t.Helper()
		cl, err := queryclient.NewClientWithResponses("http://"+addr, queryclient.WithRequestEditorFn(
			func(_ context.Context, r *http.Request) error {
				r.Header.Set("Authorization", "Bearer "+l.token(c, scopeQuery))
				if smuggle != "" {
					r.Header.Set("X-Tenant-ID", smuggle)
				}
				return nil
			}))
		if err != nil {
			t.Fatal(err)
		}
		v := 1
		from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		resp, err := cl.QueryEventsWithResponse(context.Background(), &queryclient.QueryEventsParams{},
			queryclient.EventsRequest{From: from, To: from.Add(24 * time.Hour), ProcessingVersion: &v})
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode() != http.StatusOK {
			t.Fatalf("status %d: %s\n%s", resp.StatusCode(), resp.Body, logs.String())
		}
	}
	last := func() string {
		mu.Lock()
		defer mu.Unlock()
		if len(tenants) == 0 {
			t.Fatal("no request reached the ClickHouse stub")
		}
		return tenants[len(tenants)-1]
	}

	call(a, "")
	if got := last(); got != tenantA {
		t.Fatalf("token A reached ClickHouse as %s, want %s", got, tenantA)
	}
	call(b, tenantA) // B smuggles A's tenant in a header
	if got := last(); got != tenantB {
		t.Fatalf("token B with X-Tenant-ID=A reached ClickHouse as %s, want %s", got, tenantB)
	}
	call(a, tenantB)
	if got := last(); got != tenantA {
		t.Fatalf("token A with X-Tenant-ID=B reached ClickHouse as %s, want %s", got, tenantA)
	}
}
