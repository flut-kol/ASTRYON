package replay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type browserAuth BrowserPrincipal

func (a browserAuth) Authenticate(*http.Request) (BrowserPrincipal, error) {
	return BrowserPrincipal(a), nil
}

type browserStore struct {
	tenant         string
	includePayload bool
	page           BrowserPage
	err            error
}

func (s *browserStore) ListQuarantine(_ context.Context, tenant, _ string, _ int, includePayload bool) (BrowserPage, error) {
	s.tenant = tenant
	s.includePayload = includePayload
	return s.page, s.err
}

const (
	tenantA = "00000000-0000-4000-8000-0000000000a1"
	tenantB = "00000000-0000-4000-8000-0000000000b2"
)

func TestBrowserUsesClaimTenantAndRedactsOriginal(t *testing.T) {
	store := &browserStore{page: BrowserPage{Records: []QuarantineRecord{
		{ID: "a", TenantID: tenantA, ReasonCode: "BAD", Original: []byte(`{"secret":true}`)},
		{ID: "b", TenantID: tenantB, ReasonCode: "BAD", Original: []byte(`{"leak":true}`)},
	}}}
	h := BrowserHandler{Auth: browserAuth{TenantID: tenantA, Scopes: map[string]bool{ScopeQuarantineRead: true}}, Store: store}.Routes()
	r := httptest.NewRequest(http.MethodGet, "/v1/quarantine?tenant_id="+tenantB+"&limit=1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || store.tenant != tenantA || store.includePayload {
		t.Fatalf("status=%d tenant=%q", w.Code, store.tenant)
	}
	body := w.Body.String()
	if strings.Contains(body, "secret") || strings.Contains(body, "leak") || strings.Contains(body, `"id":"b"`) {
		t.Fatalf("tenant data or original payload leaked: %s", body)
	}
}

func TestBrowserRequiresScopeAndValidPage(t *testing.T) {
	store := &browserStore{}
	h := BrowserHandler{Auth: browserAuth{TenantID: tenantA, Scopes: map[string]bool{}}, Store: store}.Routes()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/quarantine", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d", w.Code)
	}

	h = BrowserHandler{Auth: browserAuth{TenantID: tenantA, Scopes: map[string]bool{ScopeQuarantineRead: true}}, Store: store}.Routes()
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/quarantine?limit=101", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestBrowserAllowsOriginalOnlyWithSeparateScope(t *testing.T) {
	store := &browserStore{page: BrowserPage{Records: []QuarantineRecord{{ID: "a", TenantID: tenantA, Original: []byte(`{"ok":true}`)}}}}
	h := BrowserHandler{Auth: browserAuth{TenantID: tenantA, Scopes: map[string]bool{ScopeQuarantineRead: true, ScopeQuarantineReadPayload: true}}, Store: store}.Routes()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/quarantine?include_original=true", nil))
	if w.Code != http.StatusOK || !store.includePayload || !strings.Contains(w.Body.String(), `"original":{"ok":true}`) {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestBrowserRejectsOriginalWithoutPayloadScope(t *testing.T) {
	store := &browserStore{page: BrowserPage{Records: []QuarantineRecord{{ID: "a", TenantID: tenantA}}}}
	h := BrowserHandler{Auth: browserAuth{TenantID: tenantA, Scopes: map[string]bool{ScopeQuarantineRead: true}}, Store: store}.Routes()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/quarantine?include_original=true", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d", w.Code)
	}
	if store.tenant != "" {
		t.Fatal("store reached before payload scope check")
	}
}

func TestBrowserErrorEnvelopeAndRequestID(t *testing.T) {
	store := &browserStore{err: ErrInvalidCursor}
	h := BrowserHandler{Auth: browserAuth{TenantID: tenantA, Scopes: map[string]bool{ScopeQuarantineRead: true}}, Store: store}.Routes()
	r := httptest.NewRequest(http.MethodGet, "/v1/quarantine?cursor=bad", nil)
	r.Header.Set("X-Request-ID", "request-42")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || w.Header().Get("X-Request-ID") != "request-42" {
		t.Fatalf("status=%d request-id=%q", w.Code, w.Header().Get("X-Request-ID"))
	}
	body := w.Body.String()
	if !strings.Contains(body, `"error":{"code":"INVALID_CURSOR"`) || !strings.Contains(body, `"trace_id":"request-42"`) {
		t.Fatalf("body=%s", body)
	}

	badTenant := BrowserHandler{Auth: browserAuth{TenantID: "not-a-uuid", Scopes: map[string]bool{ScopeQuarantineRead: true}}, Store: store}.Routes()
	w = httptest.NewRecorder()
	badTenant.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/quarantine", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("bad tenant status=%d", w.Code)
	}
}

func TestHealthzIsPublicWhileBrowserFailsClosed(t *testing.T) {
	handler := (BrowserHandler{}).Routes()
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"ok"`) || w.Header().Get("X-Request-ID") == "" {
		t.Fatalf("health status=%d headers=%v body=%s", w.Code, w.Header(), w.Body.String())
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/quarantine", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("browser status=%d", w.Code)
	}
}
