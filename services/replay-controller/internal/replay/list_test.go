package replay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeReader struct {
	gotTenant string
	gotFilter ListFilter
	items     []Info
	next      *Cursor
	err       error
}

func (f *fakeReader) List(_ context.Context, tenantID string, flt ListFilter) ([]Info, *Cursor, error) {
	f.gotTenant, f.gotFilter = tenantID, flt
	return f.items, f.next, f.err
}
func (f *fakeReader) Get(_ context.Context, tenantID, id string) (Info, error) {
	f.gotTenant = tenantID
	for _, i := range f.items {
		if i.ID == id {
			return i, nil
		}
	}
	return Info{}, ErrNotFound
}

const (
	lTenant = "11111111-1111-4111-8111-111111111111"
	lSource = "22222222-2222-4222-8222-222222222222"
	lID     = "33333333-3333-4333-8333-333333333333"
)

func listSrv(scopes []string, r Reader) http.Handler {
	auth := DevStaticAuthenticator{Token: "t", Principal: Principal{TenantID: lTenant, Subject: "u", Scopes: scopes}}
	return (&Handler{Auth: auth, Reader: r}).Routes()
}

func lget(h http.Handler, path string, tok string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", path, nil)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestListAuthAndScope(t *testing.T) {
	fr := &fakeReader{}
	if w := lget(listSrv([]string{ScopeReplayRead}, fr), "/v1/replays", ""); w.Code != 401 {
		t.Fatalf("no token = %d", w.Code)
	}
	// the write scope alone must not read
	if w := lget(listSrv([]string{ScopeReplayWrite}, fr), "/v1/replays", "t"); w.Code != 403 {
		t.Fatalf("write scope reads = %d", w.Code)
	}
	if w := lget(listSrv([]string{ScopeReplayWrite}, fr), "/v1/replays/"+lID, "t"); w.Code != 403 {
		t.Fatalf("write scope gets = %d", w.Code)
	}
	if w := lget(listSrv([]string{ScopeReplayRead}, nil), "/v1/replays", "t"); w.Code != 501 {
		t.Fatalf("no reader = %d", w.Code)
	}
}

func TestListTenantFromPrincipalAndFilters(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fr := &fakeReader{items: []Info{{ID: lID, SourceID: lSource, ProcessingVersion: 2, Status: "COMPLETED", CreatedAt: now, UpdatedAt: now}}, next: &Cursor{T: now, ID: lID}}
	h := listSrv([]string{ScopeReplayRead}, fr)
	w := lget(h, "/v1/replays?limit=10&source_id="+lSource+"&status=COMPLETED&tenant_id=evil", "t")
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if fr.gotTenant != lTenant || fr.gotFilter.SourceID != lSource || fr.gotFilter.Status != "COMPLETED" || fr.gotFilter.Limit != 10 {
		t.Fatalf("reader got %q %+v", fr.gotTenant, fr.gotFilter)
	}
	var out struct {
		Items      []Info `json:"items"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || len(out.Items) != 1 || out.NextCursor == "" {
		t.Fatalf("body %s", w.Body)
	}
	// the cursor round-trips
	w = lget(h, "/v1/replays?cursor="+out.NextCursor, "t")
	if w.Code != 200 || fr.gotFilter.After == nil || fr.gotFilter.After.ID != lID {
		t.Fatalf("cursor not decoded: %d %+v", w.Code, fr.gotFilter)
	}
}

func TestListValidation(t *testing.T) {
	h := listSrv([]string{ScopeReplayRead}, &fakeReader{})
	for _, q := range []string{"limit=0", "limit=101", "limit=x", "source_id=nope", "status=DONE", "cursor=@@@", "cursor=" + encodeCursor(&Cursor{T: time.Now(), ID: "x"})} {
		if w := lget(h, "/v1/replays?"+q, "t"); w.Code != 400 {
			t.Errorf("%s = %d, want 400", q, w.Code)
		}
	}
	if w := lget(h, "/v1/replays", "t"); w.Code != 200 || !strings.Contains(w.Body.String(), `"items":[]`) {
		t.Fatalf("empty list = %d %s", w.Code, w.Body)
	}
}

func TestGetReplay(t *testing.T) {
	fr := &fakeReader{items: []Info{{ID: lID, SourceID: lSource, Status: "RUNNING"}}}
	h := listSrv([]string{ScopeReplayRead}, fr)
	if w := lget(h, "/v1/replays/"+lID, "t"); w.Code != 200 || !strings.Contains(w.Body.String(), `"RUNNING"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w := lget(h, "/v1/replays/44444444-4444-4444-8444-444444444444", "t"); w.Code != 404 {
		t.Fatalf("unknown = %d", w.Code)
	}
}
