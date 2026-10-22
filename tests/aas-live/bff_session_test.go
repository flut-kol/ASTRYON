package aaslive

import (
	"github.com/astryon/go-common/auth"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingUpstream records the Authorization header (jti) of every proxied call.
type recordingUpstream struct {
	*httptest.Server
	mu   sync.Mutex
	toks []string
}

func newRecordingUpstream(t *testing.T) *recordingUpstream {
	r := &recordingUpstream{}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.toks = append(r.toks, strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "))
		r.mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *recordingUpstream) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.toks) == 0 {
		return ""
	}
	return r.toks[len(r.toks)-1]
}

var csrfHdr = map[string]string{"X-Requested-With": "astryon-admin", "Content-Type": "application/json"}

// TestLiveAdminBFFRefresh: the access token (40 s lifespan on the client) is refreshed against the real AAS when
// under 30 s remain; when the AAS session is gone the refresh fails and the BFF session ends with 401 session_expired.
func TestLiveAdminBFFRefresh(t *testing.T) {
	u := needUser(t)
	l := newLive(t)
	l.newScope(scopeQuery)
	up := newRecordingUpstream(t)
	rig := newBFFRig(t, l, "127.0.0.1", map[string]string{"access.token.lifespan": "40"}, func(string) string { return "echo=" + up.URL })
	rig.login(l, u)

	if c, b, _ := rig.do("POST", "/api/echo/x", csrfHdr, "{}"); c != 200 {
		t.Fatalf("first call: %d %s\n%s", c, b, rig.logs)
	}
	first := up.last()
	if first == "" {
		t.Fatal("upstream saw no bearer")
	}
	if c, _, _ := rig.do("POST", "/api/echo/x", csrfHdr, "{}"); c != 200 || up.last() != first {
		t.Fatalf("token changed before it was close to expiry (call %d)", c)
	}

	time.Sleep(13 * time.Second) // < 30 s of the 40 s left
	if c, b, _ := rig.do("POST", "/api/echo/x", csrfHdr, "{}"); c != 200 {
		t.Fatalf("call after expiry window: %d %s\n%s", c, b, rig.logs)
	}
	second := up.last()
	if second == first {
		t.Fatal("access token was not refreshed")
	}
	cl1, cl2 := decodePart(t, first, 1), decodePart(t, second, 1)
	if cl2["jti"] == cl1["jti"] || cl2["sub"] != cl1["sub"] || cl2["tenant_id"] != tenantA || cl2["exp"].(float64) <= cl1["exp"].(float64) {
		t.Fatalf("refreshed token not a fresh token of the same user: %v / %v", cl1, cl2)
	}
	if _, err := l.verifier(func(c *auth.Config) { c.AllowedAZP = []string{rig.client.ID} }).Verify(ctx(t), second); err != nil {
		t.Fatalf("refreshed token rejected by the resource-server verifier: %v", err)
	}

	// end the user's sessions in the AAS: the next refresh must fail and drop the BFF session
	l.mustAdmin("POST", "/users/"+l.userID(t, u.user)+"/logout", nil, 204)
	time.Sleep(13 * time.Second)
	c, b, _ := rig.do("POST", "/api/echo/x", csrfHdr, "{}")
	if c != 401 || !strings.Contains(b, "session_expired") {
		t.Fatalf("after AAS session end: %d %s (want 401 session_expired)\n%s", c, b, rig.logs)
	}
	if c, b, _ := rig.do("GET", "/auth/me", nil, ""); !strings.Contains(b, `"authenticated":false`) {
		t.Fatalf("me after expiry: %d %s", c, b)
	}
}

// startLogoutReceiver runs a throw-away container on the AAS network that answers 200 to any request and appends it to a
// file. Nothing is published on the host: the test reads the captured request with `docker exec`.
func startLogoutReceiver(t *testing.T) (urlForKeycloak string, captured func() string) {
	t.Helper()
	ctr := os.Getenv("AAS_LIVE_KEYCLOAK_CONTAINER")
	if ctr == "" {
		t.Skip("AAS_LIVE_KEYCLOAK_CONTAINER not set")
	}
	out, err := exec.Command("docker", "inspect", "-f", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}", ctr).Output()
	nets := strings.Fields(string(out))
	if err != nil || len(nets) == 0 {
		t.Skipf("cannot find the network of %s: %v", ctr, err)
	}
	name := "aas-live-bcl-" + suffix()
	script := `while true; do (printf 'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'; sleep 1) | nc -l -p 8080 >> /tmp/req; done`
	if o, err := exec.Command("docker", "run", "-d", "--rm", "--name", name, "--network", nets[0], "--entrypoint", "sh", "curlimages/curl", "-c", script).CombinedOutput(); err != nil {
		t.Skipf("cannot start the receiver container: %v %s", err, o)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	time.Sleep(time.Second)
	return "http://" + name + ":8080/auth/backchannel-logout", func() string {
		b, _ := exec.Command("docker", "exec", name, "cat", "/tmp/req").Output()
		return string(b)
	}
}

// TestLiveAdminBFFBackchannelLogout: ending the AAS session makes Keycloak POST a signed logout_token to the client's
// backchannel URL. A receiver on the AAS docker network captures that real token; the test then delivers it to the BFF
// (verified against the real JWKS) and expects exactly that user's BFF session to end. A replay must be refused.
func TestLiveAdminBFFBackchannelLogout(t *testing.T) {
	u := needUser(t)
	l := newLive(t)
	l.newScope(scopeQuery)
	up := newRecordingUpstream(t)
	bclURL, captured := startLogoutReceiver(t)

	rig := newBFFRig(t, l, "127.0.0.1", nil, func(string) string { return "echo=" + up.URL })
	l.setClientAttrs(rig.client, map[string]string{"backchannel.logout.url": bclURL, "backchannel.logout.session.required": "true"})
	rig.login(l, u)
	if c, _, _ := rig.do("POST", "/api/echo/x", csrfHdr, "{}"); c != 200 {
		t.Fatalf("proxy before logout: %d\n%s", c, rig.logs)
	}

	l.mustAdmin("POST", "/users/"+l.userID(t, u.user)+"/logout", nil, 204)

	var tok string
	for deadline := time.Now().Add(15 * time.Second); tok == "" && time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		if _, body, ok := strings.Cut(captured(), "\r\n\r\n"); ok {
			if v, err := url.ParseQuery(strings.TrimSpace(body)); err == nil {
				tok = v.Get("logout_token")
			}
		}
	}
	if tok == "" {
		t.Fatalf("Keycloak sent no logout_token to %s within 15 s; captured: %q", bclURL, captured())
	}
	cl := decodePart(t, tok, 1)
	t.Logf("real logout_token header=%v claims: iss=%v aud=%v sid=%v sub=%v events=%v nonce=%v exp=%v", decodePart(t, tok, 0), cl["iss"], cl["aud"], cl["sid"], cl["sub"], cl["events"], cl["nonce"], cl["exp"])

	post := func(tok string) (int, string) {
		c, b, _ := rig.do("POST", "/auth/backchannel-logout", map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, url.Values{"logout_token": {tok}}.Encode())
		return c, b
	}
	if c, b := post(tok[:len(tok)-4] + "AAAA"); c != 400 {
		t.Fatalf("tampered real token: %d %s (want 400)", c, b)
	}
	if _, body, _ := rig.do("GET", "/auth/me", nil, ""); !strings.Contains(body, `"authenticated":true`) {
		t.Fatalf("session ended by a tampered token: %s", body)
	}
	if c, b := post(tok); c != 200 {
		t.Fatalf("real logout_token refused: %d %s\n%s", c, b, rig.logs)
	}
	if _, body, _ := rig.do("GET", "/auth/me", nil, ""); !strings.Contains(body, `"authenticated":false`) {
		t.Fatalf("session survived the back-channel logout: %s", body)
	}
	if c, _ := post(tok); c != 400 {
		t.Fatalf("replay of the logout_token: %d (want 400)", c)
	}
}
