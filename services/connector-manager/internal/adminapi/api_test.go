package adminapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/astryon/connector-manager/internal/registry"
)

const (
	id1 = "11111111-1111-1111-1111-111111111111"
	src = "22222222-2222-2222-2222-222222222222"
)

type fakeAuth struct {
	p   registry.Principal
	err error
}

func (f fakeAuth) Authenticate(*http.Request) (registry.Principal, error) { return f.p, f.err }

type fakeStore struct {
	tenants []string
	items   []Connector
	err     error
	created CreateInput
}

func (f *fakeStore) List(_ context.Context, t, _ string, limit int) ([]Connector, error) {
	f.tenants = append(f.tenants, t)
	if len(f.items) > limit {
		return f.items[:limit], f.err
	}
	return f.items, f.err
}
func (f *fakeStore) Get(_ context.Context, t, id string) (Connector, error) {
	f.tenants = append(f.tenants, t)
	return Connector{ID: id, SecretRef: "env://X"}, f.err
}
func (f *fakeStore) Create(_ context.Context, t string, in CreateInput) (Connector, error) {
	f.tenants = append(f.tenants, t)
	f.created = in
	return Connector{ID: id1, Name: in.Name, Enabled: true}, f.err
}
func (f *fakeStore) SetEnabled(_ context.Context, t, id string, en bool) (Connector, error) {
	f.tenants = append(f.tenants, t)
	return Connector{ID: id, Enabled: en}, f.err
}

func do(s *Server, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func srv(st *fakeStore) *Server {
	return &Server{Store: st, Auth: fakeAuth{p: registry.Principal{TenantID: "t1", Scopes: map[string]bool{registry.ManageScope: true}}}}
}

func TestAuthAndScope(t *testing.T) {
	st := &fakeStore{}
	s := &Server{Store: st, Auth: fakeAuth{err: ErrUnauthenticated}}
	if w := do(s, "GET", "/v1/connectors", ""); w.Code != 401 {
		t.Fatalf("got %d", w.Code)
	}
	s.Auth = fakeAuth{p: registry.Principal{TenantID: "t1"}}
	if w := do(s, "GET", "/v1/connectors", ""); w.Code != 403 {
		t.Fatalf("got %d", w.Code)
	}
	s.Auth = fakeAuth{p: registry.Principal{Scopes: map[string]bool{registry.ManageScope: true}}}
	if w := do(s, "GET", "/v1/connectors", ""); w.Code != 401 {
		t.Fatalf("no tenant must be 401, got %d", w.Code)
	}
	if len(st.tenants) != 0 {
		t.Fatal("store reached without authorization")
	}
}

func TestTenantFromPrincipalOnly(t *testing.T) {
	st := &fakeStore{}
	s := srv(st)
	if w := do(s, "POST", "/v1/connectors", `{"tenant_id":"evil","source_id":"`+src+`","name":"a","kind":"REST_PULL","secret_ref":"env://K"}`); w.Code != 400 {
		t.Fatalf("tenant_id in body must be rejected, got %d", w.Code)
	}
	w := do(s, "POST", "/v1/connectors", `{"source_id":"`+src+`","name":"a","kind":"REST_PULL","secret_ref":"env://K","config":{"url":"https://x"}}`)
	if w.Code != 201 || w.Header().Get("Location") != "/v1/connectors/"+id1 {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	do(s, "GET", "/v1/connectors/"+id1, "")
	do(s, "POST", "/v1/connectors/"+id1+"/pause", "")
	for _, tn := range st.tenants {
		if tn != "t1" {
			t.Fatalf("tenant %q", tn)
		}
	}
	if len(st.tenants) != 3 {
		t.Fatalf("calls %d", len(st.tenants))
	}
}

func TestCreateValidation(t *testing.T) {
	s := srv(&fakeStore{})
	bad := map[string]string{
		"bad kind":      `{"source_id":"` + src + `","name":"a","kind":"X","secret_ref":"env://K"}`,
		"plain secret":  `{"source_id":"` + src + `","name":"a","kind":"REST_PULL","secret_ref":"hunter2"}`,
		"secret config": `{"source_id":"` + src + `","name":"a","kind":"REST_PULL","secret_ref":"env://K","config":{"api_token":"v"}}`,
		"bad source":    `{"source_id":"x","name":"a","kind":"REST_PULL","secret_ref":"env://K"}`,
		"no name":       `{"source_id":"` + src + `","kind":"REST_PULL","secret_ref":"env://K"}`,
	}
	for n, b := range bad {
		if w := do(s, "POST", "/v1/connectors", b); w.Code != 422 {
			t.Errorf("%s: got %d %s", n, w.Code, w.Body)
		}
	}
	if w := do(s, "POST", "/v1/connectors", "{} {}"); w.Code != 400 {
		t.Errorf("trailing: %d", w.Code)
	}
	r := httptest.NewRequest("POST", "/v1/connectors", strings.NewReader("{}"))
	r.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 415 {
		t.Errorf("ct: %d", w.Code)
	}
}

func TestBodyBounded(t *testing.T) {
	s := srv(&fakeStore{})
	body := `{"name":"` + strings.Repeat("a", MaxBodyBytes+10) + `"}`
	if w := do(s, "POST", "/v1/connectors", body); w.Code != 413 {
		t.Fatalf("got %d", w.Code)
	}
}

func TestListPaginationAndErrors(t *testing.T) {
	st := &fakeStore{items: []Connector{{ID: id1}, {ID: src}, {ID: "33333333-3333-3333-3333-333333333333"}}}
	s := srv(st)
	w := do(s, "GET", "/v1/connectors?limit=2", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"next_cursor":"`+src+`"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	for _, q := range []string{"limit=0", "limit=999", "limit=x", "cursor=nope"} {
		if w := do(s, "GET", "/v1/connectors?"+q, ""); w.Code != 400 {
			t.Errorf("%s: %d", q, w.Code)
		}
	}
	if w := do(s, "GET", "/v1/connectors/not-a-uuid", ""); w.Code != 400 {
		t.Errorf("bad id: %d", w.Code)
	}
	for err, code := range map[error]int{ErrNotFound: 404, ErrConflict: 409, ErrBadReference: 422, context.Canceled: 500} {
		st.err = err
		w := do(s, "GET", "/v1/connectors/"+id1, "")
		if w.Code != code || !strings.Contains(w.Body.String(), `"trace_id"`) {
			t.Errorf("%v: %d %s", err, w.Code, w.Body)
		}
	}
}

func TestPauseResume(t *testing.T) {
	s := srv(&fakeStore{})
	if w := do(s, "POST", "/v1/connectors/"+id1+"/pause", ""); !strings.Contains(w.Body.String(), `"enabled":false`) {
		t.Fatal(w.Body)
	}
	if w := do(s, "POST", "/v1/connectors/"+id1+"/resume", ""); !strings.Contains(w.Body.String(), `"enabled":true`) {
		t.Fatal(w.Body)
	}
}
