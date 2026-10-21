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
	"strings"
	"testing"
	"time"
)

// startService builds services/<name>/cmd/<name> and runs it with the live AAS config plus extra env.
func startService(t *testing.T, l *live, name, allowedAZP string, extra ...string) (addr string, logs *bytes.Buffer) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), name)
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = "../../services/" + name + "/cmd/" + name
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, out)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr = ln.Addr().String()
	ln.Close()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "AAS_BASE_URL="+l.base, "AAS_AUDIENCE="+l.audience, "AAS_ENV="+l.env, "AAS_ALLOWED_AZP="+allowedAZP)
	cmd.Env = append(cmd.Env, extra...)
	cmd.Env = append(cmd.Env, "LISTEN_ADDR="+addr, "ASTRYON_GATEWAY_ADDR="+addr, "REPLAY_ADDR="+addr)
	if ca := os.Getenv("AAS_LIVE_CA_FILE"); ca != "" {
		cmd.Env = append(cmd.Env, "SSL_CERT_FILE="+ca)
	}
	logs = &bytes.Buffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	for i := 0; ; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return addr, logs
		}
		if i > 100 {
			t.Fatalf("%s did not start: %s", name, logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func call(t *testing.T, method, url, token, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, bytes.NewBufferString(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "ffffffff-1111-4222-8333-444455556666") // must be ignored
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	t.Logf("%s %s -> %d %.200s", method, url, resp.StatusCode, b)
	return resp.StatusCode, string(b)
}

const (
	scopeExportWrite = "astryon:export:write"
	scopeExportRead  = "astryon:export:read"
	scopeReplayWrite = "astryon:replay:write"
	scopeReplayRead  = "astryon:replay:read"
)

// TestLiveGatewayAdapter: services/gateway with a real AAS and the in-memory publisher.
func TestLiveGatewayAdapter(t *testing.T) {
	l := newLive(t)
	l.newScope(scopeWrite)
	good := l.newClient(suffix(), tenantA, 0, scopeWrite)
	noScope := l.newClient(suffix(), tenantA, 0)
	addr, logs := startService(t, l, "gateway", good.ID+","+noScope.ID, "ASTRYON_DEV_FAKE_PUBLISHER=1")
	url := "http://" + addr + "/v1/events"
	evt := func(extra string) string {
		ts := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
		return `{"source_id":"33333333-3333-4333-8333-333333333333","schema_id":"s","schema_version":1,"event_time":"` + ts + `","payload_format":"json","payload":{"a":1}` + extra + `}`
	}
	if c, _ := call(t, "POST", url, "", evt("")); c != 401 {
		t.Fatalf("no token: %d\n%s", c, logs)
	}
	if c, _ := call(t, "POST", url, l.token(noScope, ""), evt("")); c != 403 {
		t.Fatalf("no scope: %d (want 403)\n%s", c, logs)
	}
	tok := l.token(good, scopeWrite)
	if c, _ := call(t, "POST", url, tok, evt("")); c/100 != 2 {
		t.Fatalf("valid token: %d\n%s", c, logs)
	}
	// a tenant_id in the body that differs from the token claim must not be honoured
	if c, b := call(t, "POST", url, tok, evt(`,"tenant_id":"ffffffff-1111-4222-8333-444455556666"`)); c/100 == 2 && strings.Contains(b, "ffffffff-1111") {
		t.Fatalf("foreign tenant accepted: %d %s", c, b)
	}
}

// TestLiveExportWorkerAdapter: services/export-worker, scopes write/read, ClickHouse stubbed.
func TestLiveExportWorkerAdapter(t *testing.T) {
	l := newLive(t)
	l.newScope(scopeExportWrite)
	l.newScope(scopeExportRead)
	w := l.newClient(suffix(), tenantA, 0, scopeExportWrite)
	r := l.newClient(suffix(), tenantA, 0, scopeExportRead)
	ch := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer ch.Close()
	addr, logs := startService(t, l, "export-worker", w.ID+","+r.ID, "CH_URL="+ch.URL, "EXPORT_DEV_TOKEN=")
	post := "http://" + addr + "/v1/exports"
	body := `{"format":"ndjson","from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z","processing_version":1}`
	if c, _ := call(t, "POST", post, "", body); c != 401 {
		t.Fatalf("no token: %d\n%s", c, logs)
	}
	if c, _ := call(t, "POST", post, l.token(r, scopeExportRead), body); c != 403 {
		t.Fatalf("read-only token on POST: %d (want 403)\n%s", c, logs)
	}
	if c, _ := call(t, "POST", post, l.token(w, scopeExportWrite), body); c == 401 || c == 403 {
		t.Fatalf("write token refused: %d\n%s", c, logs)
	}
	get := "http://" + addr + "/v1/exports/00000000-0000-4000-8000-000000000000"
	if c, _ := call(t, "GET", get, l.token(w, scopeExportWrite), ""); c != 403 {
		t.Fatalf("write-only token on GET: %d (want 403)\n%s", c, logs)
	}
	if c, _ := call(t, "GET", get, l.token(r, scopeExportRead), ""); c == 401 || c == 403 {
		t.Fatalf("read token refused: %d\n%s", c, logs)
	}
}

// TestLiveReplayControllerAdapter: services/replay-controller, ClickHouse stubbed, dry-run plan.
func TestLiveReplayControllerAdapter(t *testing.T) {
	l := newLive(t)
	l.newScope(scopeReplayWrite)
	w := l.newClient(suffix(), tenantA, 0, scopeReplayWrite)
	noScope := l.newClient(suffix(), tenantA, 0)
	ch := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { _, _ = rw.Write([]byte("0\n")) }))
	defer ch.Close()
	addr, logs := startService(t, l, "replay-controller", w.ID+","+noScope.ID, "CH_URL="+ch.URL, "REPLAY_DEV_TOKEN=")
	url := "http://" + addr + "/v1/replays"
	body := `{"source_id":"33333333-3333-4333-8333-333333333333","from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z","pipeline_version":1,"dry_run":true}`
	if c, _ := call(t, "POST", url, "", body); c != 401 {
		t.Fatalf("no token: %d\n%s", c, logs)
	}
	if c, _ := call(t, "POST", url, l.token(noScope, ""), body); c != 403 {
		t.Fatalf("no scope: %d (want 403)\n%s", c, logs)
	}
	c, b := call(t, "POST", url, l.token(w, scopeReplayWrite), body)
	if c/100 != 2 {
		t.Fatalf("valid token: %d %s\n%s", c, b, logs)
	}
}
