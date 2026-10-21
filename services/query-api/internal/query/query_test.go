package query

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const tenant = "00000000-0000-0000-0000-0000000000a1"

type fake struct {
	query  string
	params url.Values
	reply  string
}

func setup(t *testing.T, reply string) (*httptest.Server, *fake) {
	f := &fake{reply: reply}
	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.query, f.params = string(b), r.URL.Query()
		_, _ = w.Write([]byte(f.reply))
	}))
	t.Cleanup(ch.Close)
	s := &Server{CHURL: ch.URL, Auth: DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenant, Scopes: []string{ScopeQueryRead}}}}
	api := httptest.NewServer(s.Handler())
	t.Cleanup(api.Close)
	return api, f
}

func post(t *testing.T, api *httptest.Server, path, tok, body string) (int, string) {
	req, _ := http.NewRequest("POST", api.URL+path, strings.NewReader(body))
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

const base = `"from":"2026-10-20T00:00:00Z","to":"2026-10-21T00:00:00Z","processing_version":1`

func TestAuthRequired(t *testing.T) {
	api, f := setup(t, "")
	if c, _ := post(t, api, "/v1/query/events", "", `{`+base+`}`); c != 401 {
		t.Fatalf("no token: %d", c)
	}
	if c, _ := post(t, api, "/v1/query/events", "bad", `{`+base+`}`); c != 401 {
		t.Fatalf("bad token: %d", c)
	}
	if f.query != "" {
		t.Fatal("backend reached without auth")
	}
}

func TestTenantFromPrincipalOnly(t *testing.T) {
	api, f := setup(t, "")
	// body tenant_id is an unknown field and must be rejected, never used
	if c, _ := post(t, api, "/v1/query/events", "tok", `{"tenant_id":"00000000-0000-0000-0000-0000000000ff",`+base+`}`); c != 400 {
		t.Fatalf("tenant in body accepted: %d", c)
	}
	if c, b := post(t, api, "/v1/query/events", "tok", `{`+base+`}`); c != 200 {
		t.Fatalf("%d %s", c, b)
	}
	if f.params.Get("param_p_tenant") != tenant || !strings.Contains(f.query, "tenant_id = {p_tenant:UUID}") || f.params.Get("readonly") != "1" {
		t.Fatalf("%v %s", f.params, f.query)
	}
}

func TestInjectionIsBound(t *testing.T) {
	api, f := setup(t, "")
	post(t, api, "/v1/query/events", "tok", `{`+base+`,"event_type":"x' OR 1=1 --"}`)
	if strings.Contains(f.query, "OR 1=1") || f.params.Get("param_p_type") != "x' OR 1=1 --" {
		t.Fatalf("value not bound: %s", f.query)
	}
	if c, _ := post(t, api, "/v1/query/events", "tok", `{`+base+`,"source_id":"x"}`); c != 400 {
		t.Fatal("bad source_id accepted")
	}
}

func TestValidation(t *testing.T) {
	api, _ := setup(t, "")
	for name, b := range map[string]string{
		"no version": `{"from":"2026-10-20T00:00:00Z","to":"2026-10-21T00:00:00Z"}`,
		"range":      `{"from":"2026-01-01T00:00:00Z","to":"2026-10-21T00:00:00Z","processing_version":1}`,
		"limit":      `{` + base + `,"limit":5000}`,
		"cursor":     `{` + base + `,"cursor":"!!"}`,
	} {
		if c, _ := post(t, api, "/v1/query/events", "tok", b); c != 400 {
			t.Fatalf("%s: %d", name, c)
		}
	}
}

func TestCursorPagination(t *testing.T) {
	rows := `{"event_id":"00000000-0000-0000-0000-0000000000e1","event_time":"2026-10-20 10:15:00.000"}
{"event_id":"00000000-0000-0000-0000-0000000000e2","event_time":"2026-10-20 10:45:00.000"}
`
	api, f := setup(t, rows)
	c, body := post(t, api, "/v1/query/events", "tok", `{`+base+`,"limit":1}`)
	if c != 200 || !strings.Contains(body, `"next_cursor":"ey`) || strings.Contains(body, "0000e2") {
		t.Fatalf("%d %s", c, body)
	}
	if !strings.Contains(f.query, "LIMIT 2 FORMAT") || !strings.Contains(f.query, "LIMIT 1 BY") {
		t.Fatal(f.query)
	}
	cur := body[strings.Index(body, `"next_cursor":"`)+15:]
	cur = cur[:strings.Index(cur, `"`)]
	post(t, api, "/v1/query/events", "tok", `{`+base+`,"limit":1,"cursor":"`+cur+`"}`)
	if f.params.Get("param_p_ct") != "2026-10-20 10:15:00.000" || f.params.Get("param_p_ce") != "00000000-0000-0000-0000-0000000000e1" ||
		!strings.Contains(f.query, "(event_time, event_id) > (") {
		t.Fatalf("%v %s", f.params, f.query)
	}
}

func TestAggregateWhitelist(t *testing.T) {
	api, f := setup(t, `{"event_type":"t","count":2}`+"\n")
	if c, b := post(t, api, "/v1/query/aggregate", "tok", `{`+base+`,"metrics":["count"],"group_by":["event_type"]}`); c != 200 || !strings.Contains(b, `"count":2`) {
		t.Fatalf("%d %s", c, b)
	}
	if !strings.Contains(f.query, "GROUP BY event_type") || !strings.Contains(f.query, "uniqExact(source_id, event_id, processing_version)") || strings.Contains(f.query, "LIMIT 1 BY") {
		t.Fatal(f.query)
	}
	if c, b := post(t, api, "/v1/query/aggregate", "tok", `{`+base+`,"metrics":["count","bytes"]}`); c != 200 {
		t.Fatalf("%d %s", c, b)
	}
	if !strings.Contains(f.query, "LIMIT 1 BY") {
		t.Fatal(f.query)
	}
	for _, b := range []string{
		`{` + base + `,"metrics":["count(); DROP TABLE x"]}`,
		`{` + base + `,"metrics":["count"],"group_by":["payload"]}`,
		`{` + base + `,"metrics":[]}`,
	} {
		if c, _ := post(t, api, "/v1/query/aggregate", "tok", b); c != 400 {
			t.Fatalf("accepted %s", b)
		}
	}
}

func TestScopeAndBackendError(t *testing.T) {
	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", 500) }))
	defer ch.Close()
	noScope := &Server{CHURL: ch.URL, Auth: DevStaticAuthenticator{Token: "t", Principal: Principal{TenantID: tenant}}}
	a := httptest.NewServer(noScope.Handler())
	defer a.Close()
	if c, _ := post(t, a, "/v1/query/events", "t", `{`+base+`}`); c != 403 {
		t.Fatalf("scope: %d", c)
	}
	api, _ := setup(t, "")
	api.Config.Handler = (&Server{CHURL: ch.URL, Auth: DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenant, Scopes: []string{ScopeQueryRead}}}}).Handler()
	if c, b := post(t, api, "/v1/query/events", "tok", `{`+base+`}`); c != 502 || strings.Contains(b, "boom") {
		t.Fatalf("%d %s", c, b)
	}
}

func get(t *testing.T, api *httptest.Server, path, tok string) (int, string) {
	req, _ := http.NewRequest("GET", api.URL+path, nil)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

const sq = "?from=2026-10-20T00:00:00Z&to=2026-10-21T00:00:00Z&processing_version=1"

func TestSummaries(t *testing.T) {
	api, f := setup(t, `{"source_id":"s","events_count":2}`+"\n")
	if c, _ := get(t, api, "/v1/metrics/summary"+sq, ""); c != 401 {
		t.Fatalf("unauth: %d", c)
	}
	c, b := get(t, api, "/v1/metrics/summary"+sq, "tok")
	if c != 200 || !strings.Contains(b, `"sources"`) {
		t.Fatalf("%d %s", c, b)
	}
	if f.params.Get("param_p_tenant") != tenant || !strings.Contains(f.query, "argMax(events_count, revision)") ||
		strings.Contains(f.query, "FINAL") || f.params.Get("param_p_hi") != "2026-10-20 23:00:00" {
		t.Fatalf("%v %s", f.params, f.query)
	}
	c, b = get(t, api, "/v1/quality/summary"+sq+"&source_id=00000000-0000-0000-0000-0000000000b1", "tok")
	if c != 200 || !strings.Contains(b, `"flags"`) || f.params.Get("param_p_hi") != "2026-10-20" || f.params.Get("param_p_source") == "" {
		t.Fatalf("%d %s %v", c, b, f.params)
	}
	for _, q := range []string{"", sq + "&tenant_id=x", "?from=2026-10-20T00:00:00Z&to=2026-10-21T00:00:00Z", sq + "&source_id=x"} {
		if c, _ := get(t, api, "/v1/quality/summary"+q, "tok"); c != 400 {
			t.Fatalf("accepted %q: %d", q, c)
		}
	}
}
