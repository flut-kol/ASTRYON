package bff

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeAAS implements discovery, authorize (auto-approves) and token with PKCE verification.
type fakeAAS struct {
	*httptest.Server
	mu         sync.Mutex
	codes      map[string]struct{ challenge, nonce string }
	expiresIn  int
	refreshes  atomic.Int32
	lastAssert string
	lastBasic  string
	badNonce   bool
	tokenType  string
}

func newFakeAAS(t *testing.T) *fakeAAS {
	f := &fakeAAS{codes: map[string]struct{ challenge, nonce string }{}, expiresIn: 300, tokenType: "Bearer"}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/realm/.well-known/openid-configuration":
			json.NewEncoder(w).Encode(map[string]string{"issuer": f.URL + "/realm", "authorization_endpoint": f.URL + "/realm/auth", "token_endpoint": f.URL + "/realm/token", "end_session_endpoint": f.URL + "/realm/logout"})
		case "/realm/auth":
			q := r.URL.Query()
			if q.Get("code_challenge_method") != "S256" || q.Get("response_type") != "code" || q.Get("client_id") != "astryon-portal" {
				http.Error(w, "bad request", 400)
				return
			}
			code := randString(8)
			f.mu.Lock()
			f.codes[code] = struct{ challenge, nonce string }{q.Get("code_challenge"), q.Get("nonce")}
			f.mu.Unlock()
			http.Redirect(w, r, q.Get("redirect_uri")+"?code="+code+"&state="+url.QueryEscape(q.Get("state")), 302)
		case "/realm/token":
			r.ParseForm()
			f.mu.Lock()
			f.lastAssert = r.Form.Get("client_assertion")
			f.lastBasic = r.Header.Get("Authorization")
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			switch r.Form.Get("grant_type") {
			case "authorization_code":
				f.mu.Lock()
				c, ok := f.codes[r.Form.Get("code")]
				delete(f.codes, r.Form.Get("code"))
				f.mu.Unlock()
				h := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
				if !ok || b64u(h[:]) != c.challenge {
					w.WriteHeader(400)
					io.WriteString(w, `{"error":"invalid_grant"}`)
					return
				}
				nonce := c.nonce
				if f.badNonce {
					nonce = "other"
				}
				json.NewEncoder(w).Encode(map[string]any{"access_token": "AT1", "refresh_token": "RT1", "id_token": f.idToken(nonce), "expires_in": f.expiresIn, "token_type": f.tokenType})
			case "refresh_token":
				if r.Form.Get("refresh_token") != "RT1" {
					w.WriteHeader(400)
					io.WriteString(w, `{"error":"invalid_grant"}`)
					return
				}
				n := f.refreshes.Add(1)
				json.NewEncoder(w).Encode(map[string]any{"access_token": "AT" + string(rune('1'+n)), "expires_in": 300, "token_type": "Bearer"})
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeAAS) idToken(nonce string) string {
	cl, _ := json.Marshal(map[string]any{"iss": f.URL + "/realm", "sub": "user-1", "aud": "astryon-portal", "exp": time.Now().Add(time.Hour).Unix(), "nonce": nonce})
	return b64u([]byte(`{"alg":"ES256"}`)) + "." + b64u(cl) + "." + b64u([]byte("sig"))
}

type env struct {
	aas    *fakeAAS
	bff    *httptest.Server
	srv    *Server
	client *http.Client
	got    chan http.Header
}

func setup(t *testing.T, mut func(*Options)) *env {
	t.Helper()
	aas := newFakeAAS(t)
	got := make(chan http.Header, 8)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Clone()
		h.Set("X-Path", r.URL.RequestURI())
		got <- h
		io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(up.Close)
	e := &env{aas: aas, got: got}
	e.bff = httptest.NewUnstartedServer(nil)
	e.bff.Start()
	t.Cleanup(e.bff.Close)
	o := Options{OIDC: Config{BaseURL: aas.URL + "/realm", ClientID: "astryon-portal", ClientSecret: "s3cret", RedirectURL: e.bff.URL + "/auth/callback", Scopes: []string{"openid", "astryon:events:read"}},
		PublicOrigin: e.bff.URL, Upstreams: map[string]string{"query": up.URL + "/base"}, CookieSecure: false}
	if mut != nil {
		mut(&o)
	}
	srv, err := New(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	e.srv = srv
	e.bff.Config.Handler = srv
	jar, _ := cookiejar.New(nil)
	e.client = &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}
	return e
}

func (e *env) do(t *testing.T, method, path string, hdr map[string]string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(method, e.bff.URL+path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := e.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res, string(b)
}

func (e *env) login(t *testing.T) {
	t.Helper()
	res, body := e.do(t, "GET", "/auth/login", nil)
	if res.StatusCode != 200 && res.Request.URL.Path != "/" {
		t.Fatalf("login ended at %s: %d %s", res.Request.URL, res.StatusCode, body)
	}
}

func TestLoginProxyAndSecrets(t *testing.T) {
	e := setup(t, nil)
	if res, _ := e.do(t, "GET", "/api/query/v1/x", nil); res.StatusCode != 401 {
		t.Fatalf("unauthenticated proxy = %d", res.StatusCode)
	}
	e.login(t)
	u, _ := url.Parse(e.bff.URL)
	var sess *http.Cookie
	for _, c := range e.client.Jar.Cookies(u) {
		if c.Name == "astryon_session" {
			sess = c
		}
	}
	if sess == nil {
		t.Fatal("no session cookie")
	}
	if strings.Contains(sess.Value, "AT1") {
		t.Fatal("cookie must be an opaque id, not a token")
	}
	_, me := e.do(t, "GET", "/auth/me", nil)
	if !strings.Contains(me, `"authenticated":true`) || !strings.Contains(me, "user-1") || strings.Contains(me, "AT1") {
		t.Fatalf("me = %s", me)
	}
	res, body := e.do(t, "GET", "/api/query/v1/metrics/summary?a=1", map[string]string{"Authorization": "Bearer attacker", "X-Bff-Authorization": "Bearer attacker2"})
	if res.StatusCode != 200 || body != `{"ok":true}` {
		t.Fatalf("proxy = %d %s", res.StatusCode, body)
	}
	h := <-e.got
	if h.Get("Authorization") != "Bearer AT1" || h.Get("Cookie") != "" || h.Get("X-Bff-Authorization") != "" {
		t.Fatalf("upstream headers: auth=%q cookie=%q", h.Get("Authorization"), h.Get("Cookie"))
	}
	if h.Get("X-Path") != "/base/v1/metrics/summary?a=1" {
		t.Fatalf("path = %s", h.Get("X-Path"))
	}
	if !strings.HasPrefix(e.aas.lastBasic, "Basic ") {
		t.Fatal("dev client secret must use client_secret_basic")
	}
}

func TestStateAndNonceChecks(t *testing.T) {
	e := setup(t, nil)
	// callback without a login in progress
	if res, _ := e.do(t, "GET", "/auth/callback?code=x&state=y", nil); res.StatusCode != 400 {
		t.Fatalf("forged callback = %d", res.StatusCode)
	}
	// state captured from one browser but replayed from another (no matching login cookie)
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, _ := noRedirect.Get(e.bff.URL + "/auth/login")
	loc, _ := url.Parse(res.Header.Get("Location"))
	if r, _ := e.do(t, "GET", "/auth/callback?code=x&state="+url.QueryEscape(loc.Query().Get("state")), nil); r.StatusCode != 400 {
		t.Fatalf("state replayed without the login cookie = %d", r.StatusCode)
	}
	e2 := setup(t, nil)
	e2.aas.badNonce = true
	r, _ := e2.do(t, "GET", "/auth/login", nil)
	if r.StatusCode != 401 {
		t.Fatalf("bad nonce accepted: %d", r.StatusCode)
	}
	e3 := setup(t, nil)
	e3.aas.tokenType = "DPoP"
	if r, _ := e3.do(t, "GET", "/auth/login", nil); r.StatusCode != 502 {
		t.Fatalf("DPoP token accepted: %d", r.StatusCode)
	}
}

func TestRefreshWhenExpiring(t *testing.T) {
	e := setup(t, nil)
	e.aas.expiresIn = 10 // inside the 30 s margin
	e.login(t)
	e.do(t, "GET", "/api/query/a", nil)
	h := <-e.got
	if h.Get("Authorization") != "Bearer AT2" || e.aas.refreshes.Load() != 1 {
		t.Fatalf("auth=%q refreshes=%d", h.Get("Authorization"), e.aas.refreshes.Load())
	}
	e.do(t, "GET", "/api/query/b", nil)
	if h := <-e.got; h.Get("Authorization") != "Bearer AT2" || e.aas.refreshes.Load() != 1 {
		t.Fatal("refreshed token must be reused")
	}
}

func TestCSRFAndLogout(t *testing.T) {
	e := setup(t, nil)
	e.login(t)
	if res, _ := e.do(t, "POST", "/api/query/x", nil); res.StatusCode != 403 {
		t.Fatalf("POST without CSRF header = %d", res.StatusCode)
	}
	if res, _ := e.do(t, "POST", "/api/query/x", map[string]string{CSRFHeader: CSRFValue, "Origin": "https://evil.example"}); res.StatusCode != 403 {
		t.Fatalf("foreign Origin = %d", res.StatusCode)
	}
	if res, _ := e.do(t, "POST", "/api/query/x", map[string]string{CSRFHeader: CSRFValue, "Origin": e.bff.URL}); res.StatusCode != 200 {
		t.Fatalf("legit POST = %d", res.StatusCode)
	}
	if res, _ := e.do(t, "POST", "/auth/logout", nil); res.StatusCode != 403 {
		t.Fatalf("logout without CSRF header = %d", res.StatusCode)
	}
	res, body := e.do(t, "POST", "/auth/logout", map[string]string{CSRFHeader: CSRFValue})
	if res.StatusCode != 200 || !strings.Contains(body, "id_token_hint") || !strings.Contains(body, "/realm/logout") {
		t.Fatalf("logout = %d %s", res.StatusCode, body)
	}
	if res, _ := e.do(t, "GET", "/api/query/x", nil); res.StatusCode != 401 {
		t.Fatalf("after logout = %d", res.StatusCode)
	}
}

func TestAbsoluteAfterLoginKeepsRegisteredPostLogoutURI(t *testing.T) {
	e := setup(t, func(o *Options) { o.AfterLoginPath = "http://127.0.0.1:15173/" })
	e.login(t)
	_, body := e.do(t, "POST", "/auth/logout", map[string]string{CSRFHeader: CSRFValue})
	if !strings.Contains(body, "post_logout_redirect_uri="+url.QueryEscape(e.bff.URL+"/")) {
		t.Fatalf("post-logout URI must stay on the BFF origin: %s", body)
	}
}

func TestAllowedOriginsAndRootRedirect(t *testing.T) {
	e := setup(t, func(o *Options) {
		o.AfterLoginPath = "http://127.0.0.1:15173/"
		o.AllowedOrigins = []string{"http://127.0.0.1:15173"}
	})
	e.login(t)
	if res, _ := e.do(t, "POST", "/auth/logout", map[string]string{CSRFHeader: CSRFValue, "Origin": "http://127.0.0.1:15173"}); res.StatusCode != 200 {
		t.Fatalf("logout from an allowed UI origin = %d", res.StatusCode)
	}
	e.login(t)
	if res, _ := e.do(t, "POST", "/auth/logout", map[string]string{CSRFHeader: CSRFValue, "Origin": "http://evil.example"}); res.StatusCode != 403 {
		t.Fatalf("logout from a foreign origin = %d", res.StatusCode)
	}
	c := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := c.Get(e.bff.URL + "/")
	if err != nil || res.StatusCode != 302 || res.Header.Get("Location") != "http://127.0.0.1:15173/" {
		t.Fatalf("GET / = %v %v", res, err)
	}
}

func TestUnknownServiceAndSessionMaxAge(t *testing.T) {
	e := setup(t, func(o *Options) { o.SessionMaxAge = time.Hour })
	if res, _ := e.do(t, "GET", "/api/nope/x", nil); res.StatusCode != 404 {
		t.Fatalf("unknown service = %d", res.StatusCode)
	}
	now := time.Now()
	e.srv.oc.now = func() time.Time { return now }
	e.login(t)
	now = now.Add(2 * time.Hour)
	if res, _ := e.do(t, "GET", "/api/query/x", nil); res.StatusCode != 401 {
		t.Fatalf("expired session = %d", res.StatusCode)
	}
}

func TestPrivateKeyJWTClientAuth(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	f := filepath.Join(t.TempDir(), "k.pem")
	os.WriteFile(f, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600)
	e := setup(t, func(o *Options) { o.OIDC.ClientSecret, o.OIDC.KeyFile = "", f })
	e.login(t)
	a := strings.Split(e.aas.lastAssert, ".")
	if len(a) != 3 {
		t.Fatalf("no client assertion: %q", e.aas.lastAssert)
	}
	if e.aas.lastBasic != "" {
		t.Fatal("secret auth must not be used with a key")
	}
	if _, err := New(context.Background(), Options{OIDC: Config{BaseURL: e.aas.URL + "/realm", ClientID: "c", RedirectURL: "x"}}); err == nil {
		t.Fatal("config without any client credential must be rejected")
	}
}
