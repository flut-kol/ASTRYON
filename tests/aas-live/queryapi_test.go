package aaslive

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestLiveQueryAPIAdapter builds services/query-api and calls it with real AAS tokens. ClickHouse is
// replaced by a recording stub: the only thing asserted is which tenant reaches the query layer.
func TestLiveQueryAPIAdapter(t *testing.T) {
	l := newLive(t)
	l.newScope(scopeQuery)
	good := l.newClient(suffix(), tenantA, 0, scopeQuery)
	noScope := l.newClient(suffix(), tenantA, 0)

	var mu sync.Mutex
	var tenants []string
	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tenants = append(tenants, r.URL.Query().Get("param_p_tenant"))
		mu.Unlock()
	}))
	defer ch.Close()

	bin := filepath.Join(t.TempDir(), "query-api")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = "../../services/query-api/cmd/query-api"
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build query-api: %v\n%s", err, out)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "LISTEN_ADDR="+addr, "AAS_BASE_URL="+l.base, "AAS_AUDIENCE="+l.audience, "AAS_ENV="+l.env,
		"AAS_ALLOWED_AZP="+good.ID+","+noScope.ID, "CH_URL="+ch.URL, "QUERY_DEV_TOKEN=", "QUERY_REDIS_URL=")
	if ca := os.Getenv("AAS_LIVE_CA_FILE"); ca != "" {
		cmd.Env = append(cmd.Env, "SSL_CERT_FILE="+ca)
	}
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	for i := 0; ; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			break
		}
		if i > 100 {
			t.Fatalf("query-api did not start: %s", logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	post := func(token string, body string) int {
		req, _ := http.NewRequest("POST", "http://"+addr+"/v1/query/events", bytes.NewBufferString(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		req.Header.Set("X-Tenant-ID", "ffffffff-1111-4222-8333-444455556666") // must be ignored
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Logf("query-api -> %d %s", resp.StatusCode, b)
		return resp.StatusCode
	}
	body := `{"limit":1,"processing_version":1,"from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z"}`
	if c := post("", body); c != 401 {
		t.Fatalf("no token: %d\n%s", c, logs.String())
	}
	if c := post(l.token(noScope, ""), body); c != 403 {
		t.Fatalf("token without %s: %d (want 403)\n%s", scopeQuery, c, logs.String())
	}
	if c := post(l.token(good, scopeQuery), body); c != 200 {
		t.Fatalf("valid token: %d\n%s", c, logs.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(tenants) == 0 || tenants[len(tenants)-1] != tenantA {
		t.Fatalf("tenant reaching ClickHouse = %v, want %s (from claim, not X-Tenant-ID header)", tenants, tenantA)
	}
}
