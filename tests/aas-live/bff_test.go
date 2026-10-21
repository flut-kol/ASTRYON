package aaslive

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestLiveAdminBFF: services/admin-bff against the real AAS. The BFF logs a user in (code + PKCE, password+TOTP),
// keeps the tokens server side and proxies /api/query/ to a live query-api with the access token.
// The registered client carries the tenant_id mapper and the query scope, as an AAS registration for the Admin UI must.
func TestLiveAdminBFF(t *testing.T) {
	u := needUser(t)
	l := newLive(t)
	l.newScope(scopeQuery)

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	bffAddr := ln.Addr().String()
	ln.Close()
	origin := "http://" + bffAddr
	c, _ := l.newPortalClient(false, true, origin+"/auth/callback")

	keyDER, _ := x509.MarshalPKCS8PrivateKey(c.key)
	keyFile := filepath.Join(t.TempDir(), "bff.pem")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var tenants []string
	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tenants = append(tenants, r.URL.Query().Get("param_p_tenant"))
		mu.Unlock()
	}))
	defer ch.Close()
	qaddr, qlogs := startQueryAPI(t, l, c.ID, ch.URL)

	_, bfflogs := startService(t, l, "admin-bff", "", "BFF_ADDR="+bffAddr, "BFF_PUBLIC_ORIGIN="+origin, "BFF_COOKIE_INSECURE=1",
		"BFF_CLIENT_ID="+c.ID, "BFF_CLIENT_KEY_FILE="+keyFile, "BFF_CLIENT_KEY_ID="+c.kid(), "BFF_SCOPES=openid "+scopeQuery,
		"BFF_UPSTREAMS=query=http://"+qaddr)

	jar, _ := cookiejar.New(nil)
	browser := &http.Client{Jar: jar, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	do := func(method, path string, hdr map[string]string, body string) (int, string, http.Header) {
		req, _ := http.NewRequest(method, origin+path, strings.NewReader(body))
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := browser.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			sb.Write(buf[:n])
			if err != nil {
				break
			}
		}
		return resp.StatusCode, sb.String(), resp.Header
	}
	csrf := map[string]string{"X-Requested-With": "astryon-admin", "Content-Type": "application/json", "Origin": origin}
	query := `{"limit":1,"processing_version":1,"from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z"}`

	if c, _, _ := do("GET", "/auth/me", nil, ""); c != 200 && c != 401 {
		t.Fatalf("me before login: %d\n%s", c, bfflogs)
	}
	if c, _, _ := do("POST", "/api/query/v1/query/events", csrf, query); c != 401 {
		t.Fatalf("proxy without a session: %d (want 401)", c)
	}

	// login: BFF -> AAS (password, TOTP) -> BFF callback
	code, _, h := do("GET", "/auth/login", nil, "")
	if code != 302 {
		t.Fatalf("/auth/login: %d\n%s", code, bfflogs)
	}
	back := l.userLogin(t, u, h.Get("Location"), origin+"/auth/callback")
	// the browser carries the BFF's login cookie (its jar), so hand it the callback URL
	cb, _ := url.Parse(back)
	if code, body, _ := do("GET", cb.RequestURI(), nil, ""); code != 302 && code != 200 {
		t.Fatalf("callback: %d %.200s\n%s", code, body, bfflogs)
	}

	t.Run("session is opaque and me works", func(t *testing.T) {
		code, body, _ := do("GET", "/auth/me", nil, "")
		var me struct {
			Authenticated bool   `json:"authenticated"`
			Subject       string `json:"subject"`
		}
		_ = json.Unmarshal([]byte(body), &me)
		if code != 200 || !me.Authenticated || me.Subject == "" || strings.Contains(body, "eyJ") {
			t.Fatalf("me: %d %s", code, body)
		}
		for _, ck := range jar.Cookies(&url.URL{Scheme: "http", Host: bffAddr}) {
			if strings.Count(ck.Value, ".") == 2 {
				t.Fatalf("cookie %s looks like a JWT", ck.Name)
			}
		}
	})
	t.Run("CSRF header is required for unsafe methods", func(t *testing.T) {
		if c, _, _ := do("POST", "/api/query/v1/query/events", map[string]string{"Content-Type": "application/json"}, query); c != 403 {
			t.Fatalf("POST without X-Requested-With: %d (want 403)", c)
		}
	})
	t.Run("proxy adds the access token; tenant comes from the token, not the browser", func(t *testing.T) {
		hdr := map[string]string{"X-Tenant-ID": "ffffffff-1111-4222-8333-444455556666", "Authorization": "Bearer forged"}
		for k, v := range csrf {
			hdr[k] = v
		}
		code, body, _ := do("POST", "/api/query/v1/query/events", hdr, query)
		if code != 200 {
			t.Fatalf("proxied query: %d %s\nbff:\n%s\nquery-api:\n%s", code, body, bfflogs, qlogs)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(tenants) == 0 || tenants[len(tenants)-1] != tenantA {
			t.Fatalf("tenant reaching ClickHouse = %v, want %s", tenants, tenantA)
		}
	})
	t.Run("logout ends the session and returns the AAS end_session URL", func(t *testing.T) {
		code, body, _ := do("POST", "/auth/logout", csrf, "{}")
		if code != 200 || !strings.Contains(body, "logout_url") {
			t.Fatalf("logout: %d %s", code, body)
		}
		if c, _, _ := do("POST", "/api/query/v1/query/events", csrf, query); c != 401 {
			t.Fatalf("proxy after logout: %d (want 401)", c)
		}
	})
}
