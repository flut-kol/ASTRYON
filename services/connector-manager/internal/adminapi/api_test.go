package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/astryon/audit"
	"github.com/astryon/connector-manager/internal/registry"
	goauth "github.com/astryon/go-common/auth"
)

const (
	id1      = "11111111-1111-1111-1111-111111111111"
	src      = "22222222-2222-2222-2222-222222222222"
	tenantID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
)

type fakeAuth struct {
	p   goauth.Principal
	err error
}

func (f fakeAuth) Authenticate(*http.Request) (goauth.Principal, error) { return f.p, f.err }

type fakeAudit struct{ events []audit.Event }

func (f *fakeAudit) Submit(e audit.Event) error {
	f.events = append(f.events, e)
	return nil
}

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
	return &Server{Store: st, Auth: fakeAuth{p: goauth.Principal{TenantID: tenantID, Subject: "actor-1", ACR: "mfa", Scopes: []string{registry.ManageScope}}}}
}

func TestAuthAndScope(t *testing.T) {
	st := &fakeStore{}
	s := &Server{Store: st, Auth: fakeAuth{err: ErrUnauthenticated}}
	if w := do(s, "GET", "/v1/connectors", ""); w.Code != 401 {
		t.Fatalf("got %d", w.Code)
	}
	s.Auth = fakeAuth{p: goauth.Principal{TenantID: tenantID, Subject: "actor-1"}}
	if w := do(s, "GET", "/v1/connectors", ""); w.Code != 403 {
		t.Fatalf("got %d", w.Code)
	}
	s.Auth = fakeAuth{p: goauth.Principal{Scopes: []string{registry.ManageScope}}}
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
		if tn != tenantID {
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
	a := &fakeAudit{}
	s := srv(&fakeStore{})
	s.Audit = a
	if w := do(s, "POST", "/v1/connectors/"+id1+"/pause", ""); !strings.Contains(w.Body.String(), `"enabled":false`) {
		t.Fatal(w.Body)
	}
	if w := do(s, "POST", "/v1/connectors/"+id1+"/resume", ""); !strings.Contains(w.Body.String(), `"enabled":true`) {
		t.Fatal(w.Body)
	}
	if len(a.events) != 2 || a.events[0].Action != "connector.pause" || a.events[1].Action != "connector.resume" {
		t.Fatalf("audit events: %+v", a.events)
	}
	for _, e := range a.events {
		if e.TenantID != tenantID || e.Subject != "actor-1" || e.ACR != "mfa" || e.Outcome != audit.OutcomeSuccess || e.Resource.ID != id1 {
			t.Fatalf("bad audit identity/outcome: %+v", e)
		}
		if _, err := e.Normalize(time.Now()); err != nil {
			t.Fatalf("event rejected by audit library: %v", err)
		}
		if len(e.Details) != 0 {
			t.Fatalf("unexpected audit details: %+v", e.Details)
		}
	}
}

func TestCreateAuditExcludesConfigAndCredentialReference(t *testing.T) {
	a := &fakeAudit{}
	s := srv(&fakeStore{})
	s.Audit = a
	w := do(s, "POST", "/v1/connectors", `{"source_id":"`+src+`","name":"a","kind":"REST_PULL","secret_ref":"env://VERY_SECRET_NAME","config":{"url":"https://internal.example/path"}}`)
	if w.Code != http.StatusCreated || len(a.events) != 1 {
		t.Fatalf("response=%d audit=%+v", w.Code, a.events)
	}
	e := a.events[0]
	if e.Action != "connector.create" || e.Outcome != audit.OutcomeSuccess || e.Resource.ID != id1 ||
		e.Details["source_id"] != src || e.Details["connector_kind"] != "REST_PULL" {
		t.Fatalf("event: %+v", e)
	}
	if _, err := e.Normalize(time.Now()); err != nil {
		t.Fatalf("event rejected by audit library: %v", err)
	}
	b, _ := json.Marshal(e)
	for _, forbidden := range []string{"VERY_SECRET_NAME", "internal.example", "secret_ref", `"config"`, "pwd,otp"} {
		if strings.Contains(string(b), forbidden) {
			t.Fatalf("audit leaked %q: %s", forbidden, b)
		}
	}
}

func TestMutationFailureAuditedWithBoundedErrorCode(t *testing.T) {
	a := &fakeAudit{}
	s := srv(&fakeStore{err: ErrConflict})
	s.Audit = a
	w := do(s, "POST", "/v1/connectors", `{"source_id":"`+src+`","name":"a","kind":"REST_PULL","secret_ref":"env://K"}`)
	if w.Code != http.StatusConflict || len(a.events) != 1 {
		t.Fatalf("response=%d audit=%+v", w.Code, a.events)
	}
	e := a.events[0]
	if e.Outcome != audit.OutcomeFailure || e.Details["error_code"] != "conflict" || e.Resource.ID != "" {
		t.Fatalf("event: %+v", e)
	}
	if _, err := e.Normalize(time.Now()); err != nil {
		t.Fatalf("event rejected by audit library: %v", err)
	}
}

func TestDeniedAuditNeedsValidatedPrincipal(t *testing.T) {
	a := &fakeAudit{}
	s := &Server{Store: &fakeStore{}, Audit: a, Auth: fakeAuth{err: ErrUnauthenticated}}
	if w := do(s, "GET", "/v1/connectors", ""); w.Code != http.StatusUnauthorized || len(a.events) != 0 {
		t.Fatalf("invalid token must not be audited with a synthetic tenant: %d %+v", w.Code, a.events)
	}
	s.Auth = fakeAuth{p: goauth.Principal{TenantID: "11111111-2222-3333-4444-555555555555", ClientID: "client-1", ACR: "mfa", AMR: []string{"pwd", "otp"}}}
	w := do(s, "GET", "/v1/connectors", "")
	if w.Code != http.StatusForbidden || len(a.events) != 1 {
		t.Fatalf("response=%d audit=%+v", w.Code, a.events)
	}
	e := a.events[0]
	if e.Action != "auth.denied" || e.Category != audit.CategorySecurity || e.Outcome != audit.OutcomeDenied ||
		e.TenantID != "11111111-2222-3333-4444-555555555555" || e.ClientID != "client-1" || e.ACR != "mfa" {
		t.Fatalf("event: %+v", e)
	}
	if _, ok := e.Details["amr"]; ok || strings.Contains(strings.Join(mapValues(e.Details), ","), "otp") {
		t.Fatalf("AMR must not be stored: %+v", e.Details)
	}
	if _, err := e.Normalize(time.Now()); err != nil {
		t.Fatalf("event rejected by audit library: %v", err)
	}
}

func mapValues(m map[string]string) []string {
	v := make([]string, 0, len(m))
	for _, x := range m {
		v = append(v, x)
	}
	return v
}
