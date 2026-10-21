package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// Needs a real ClickHouse: QUERY_CH_IT_URL=http://127.0.0.1:8123 (default user, no password). Creates and drops a throw-away
// database. Proves that stored rows of one tenant are invisible to another through events, aggregate and cursors.
func TestIntegrationTenantIsolationRealClickHouse(t *testing.T) {
	chURL := os.Getenv("QUERY_CH_IT_URL")
	if chURL == "" {
		t.Skip("QUERY_CH_IT_URL not set")
	}
	db := fmt.Sprintf("qit_%d", time.Now().UnixNano())
	exec := func(database, q string) {
		t.Helper()
		u := strings.TrimRight(chURL, "/") + "/"
		if database != "" {
			u += "?database=" + database
		}
		resp, err := http.Post(u, "text/plain", strings.NewReader(q))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if b, _ := io.ReadAll(resp.Body); resp.StatusCode/100 != 2 {
			t.Fatalf("clickhouse %s: %s\n%s", resp.Status, b, q)
		}
	}
	exec("", "CREATE DATABASE "+db)
	t.Cleanup(func() { exec("", "DROP DATABASE IF EXISTS "+db) })

	ddl, err := os.ReadFile("../../../../db/clickhouse/migrations/0001_events_normalized.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Same template substitution as db/apply.sh on a single node; only the local table is needed.
	sql := strings.NewReplacer("__ON_CLUSTER__", "", "__RMT__", "MergeTree()", "__CLUSTER__", "default").Replace(string(ddl))
	local := sql[:strings.Index(sql, "-- Single node:")]
	exec(db, local)

	const (
		tenA   = "00000000-0000-0000-0000-0000000000a1"
		tenB   = "00000000-0000-0000-0000-0000000000b2"
		source = "00000000-0000-0000-0000-0000000000c3"
	)
	// Same source_id and the SAME event_ids in both tenants: only tenant_id tells the rows apart.
	var vals []string
	for _, ten := range []struct{ id, tag string }{{tenA, "rowA"}, {tenB, "rowB"}} {
		for i := 1; i <= 5; i++ {
			vals = append(vals, fmt.Sprintf("('%s','%s','00000000-0000-0000-0000-00000000000%d','%s',1,'%s',1,'t','2026-10-20 10:00:0%d.000','e%d',{},'%s','',[])",
				ten.id, source, i, source, source, i, i, ten.tag))
		}
	}
	exec(db, "INSERT INTO events_normalized_local (tenant_id,source_id,event_id,batch_id,processing_version,pipeline_id,pipeline_version,event_type,event_time,entity_id,attributes,payload,raw_ref,quality_flags) VALUES "+strings.Join(vals, ","))

	server := func(tenant, tok string) *httptest.Server {
		s := &Server{CHURL: chURL, CHDatabase: db, Table: "events_normalized_local",
			Auth: DevStaticAuthenticator{Token: tok, Principal: Principal{TenantID: tenant, Scopes: []string{ScopeQueryRead}}}}
		ts := httptest.NewServer(s.Handler())
		t.Cleanup(ts.Close)
		return ts
	}
	apiA, apiB := server(tenA, "tokA"), server(tenB, "tokB")
	post := func(api *httptest.Server, tok, path, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest("POST", api.URL+path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("X-Tenant-ID", tenB) // smuggled, must be ignored
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("%s -> %d %s", path, resp.StatusCode, b)
		}
		var out map[string]any
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	payloads := func(out map[string]any) []string {
		var ps []string
		for _, it := range out["items"].([]any) {
			ps = append(ps, it.(map[string]any)["payload"].(string))
		}
		return ps
	}
	const rng = `"from":"2026-10-20T00:00:00Z","to":"2026-10-21T00:00:00Z","processing_version":1`

	for _, c := range []struct {
		api      *httptest.Server
		tok, tag string
	}{{apiA, "tokA", "rowA"}, {apiB, "tokB", "rowB"}} {
		// Page through with limit 2 so cursors are exercised too.
		var got []string
		cur := ""
		for page := 0; page < 10; page++ {
			body := `{` + rng + `,"limit":2`
			if cur != "" {
				body += `,"cursor":"` + cur + `"`
			}
			out := post(c.api, c.tok, "/v1/query/events", body+`}`)
			got = append(got, payloads(out)...)
			cur, _ = out["next_cursor"].(string)
			if cur == "" {
				break
			}
		}
		if len(got) != 5 {
			t.Fatalf("%s: %d rows %v, want 5", c.tag, len(got), got)
		}
		for _, p := range got {
			if p != c.tag {
				t.Fatalf("%s saw a foreign row %q in %v", c.tag, p, got)
			}
		}
		agg := post(c.api, c.tok, "/v1/query/aggregate", `{`+rng+`,"metrics":["count"]}`)
		if b, _ := json.Marshal(agg); !strings.Contains(string(b), `"count":5`) && !strings.Contains(string(b), `"count":"5"`) {
			t.Fatalf("%s aggregate counted foreign rows: %s", c.tag, b)
		}
	}

	// A cursor minted for tenant A, replayed by tenant B, still returns only B's rows.
	first := post(apiA, "tokA", "/v1/query/events", `{`+rng+`,"limit":2}`)
	cur, _ := first["next_cursor"].(string)
	if cur == "" {
		t.Fatal("expected a cursor from tenant A")
	}
	for _, p := range payloads(post(apiB, "tokB", "/v1/query/events", `{`+rng+`,"limit":10,"cursor":"`+cur+`"}`)) {
		if p != "rowB" {
			t.Fatalf("tenant B with A's cursor saw %q", p)
		}
	}
}
