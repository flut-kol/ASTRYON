package aaslive

// Browser/user tokens (authorization code + PKCE with the realm's password+TOTP browser flow), token
// exchange and the standard-address proxy (/oauth2/*) against a real AAS. Needs a test user:
// AAS_LIVE_USER, AAS_LIVE_USER_PASSWORD, AAS_LIVE_USER_TOTP_SECRET (base32-free: the raw secret string
// Keycloak was given, as in the AAS repo's E2E_TOTP_SECRET). Proxy tests need AAS_LIVE_PROXY_URL.

import (
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" // #nosec G505 -- RFC 6238 TOTP is defined over HMAC-SHA1
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/astryon/go-common/auth"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	totpMu   sync.Mutex
	totpLast int64
)

// nextTOTP returns a code Keycloak has not seen: counters must strictly increase (lookAhead 1).
func nextTOTP(t *testing.T, secret string) string {
	t.Helper()
	totpMu.Lock()
	defer totpMu.Unlock()
	// Keycloak remembers the last counter per user across processes: keep ours in a file too
	stateFile := os.TempDir() + "/aas-live-totp-last"
	if b, err := os.ReadFile(stateFile); err == nil {
		var n int64
		if _, err := fmt.Sscan(string(b), &n); err == nil && n > totpLast {
			totpLast = n
		}
	}
	defer func() { _ = os.WriteFile(stateFile, []byte(fmt.Sprint(totpLast)), 0o600) }()
	for {
		base := time.Now().Unix() / 30
		c := base
		if totpLast+1 > c {
			c = totpLast + 1
		}
		if c <= base+1 {
			totpLast = c
			var msg [8]byte
			binary.BigEndian.PutUint64(msg[:], uint64(c))
			m := hmac.New(sha1.New, []byte(secret))
			m.Write(msg[:])
			h := m.Sum(nil)
			o := h[len(h)-1] & 15
			return fmt.Sprintf("%06d", (binary.BigEndian.Uint32(h[o:o+4])&0x7fffffff)%1000000)
		}
		time.Sleep(2 * time.Second)
	}
}

var formAction = regexp.MustCompile(`action="([^"]+)"`)

func formURL(t *testing.T, page string) string {
	t.Helper()
	m := formAction.FindStringSubmatch(page)
	if m == nil {
		t.Fatalf("no form in page: %.300s", page)
	}
	return strings.ReplaceAll(m[1], "&amp;", "&")
}

type userEnv struct{ user, pass, secret string }

func needUser(t *testing.T) userEnv {
	u := userEnv{os.Getenv("AAS_LIVE_USER"), os.Getenv("AAS_LIVE_USER_PASSWORD"), os.Getenv("AAS_LIVE_USER_TOTP_SECRET")}
	if u.user == "" || u.pass == "" || u.secret == "" {
		t.Skip("AAS_LIVE_USER / _PASSWORD / _TOTP_SECRET not set")
	}
	return u
}

// userCode drives the browser flow without a browser for a fresh PKCE request.
func (l *live) userCode(t *testing.T, u userEnv, clientID, redirect, scope, challenge string) string {
	t.Helper()
	q := url.Values{"client_id": {clientID}, "redirect_uri": {redirect}, "response_type": {"code"}, "scope": {scope},
		"state": {"st"}, "nonce": {suffix()}, "code_challenge": {challenge}, "code_challenge_method": {"S256"}}
	loc := l.userLogin(t, u, l.base+"/protocol/openid-connect/auth?"+q.Encode(), redirect)
	pu, _ := url.Parse(loc)
	if pu.Query().Get("state") != "st" || pu.Query().Get("code") == "" {
		t.Fatalf("bad redirect %s", loc)
	}
	return pu.Query().Get("code")
}

// userLogin opens authURL, submits the login form, then the OTP form, and returns the redirect Location (to redirect...).
func (l *live) userLogin(t *testing.T, u userEnv, authURL, redirect string) string {
	t.Helper()
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Transport: l.http.Transport, Jar: jar, Timeout: 20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	get := func(u string) (int, string, string) {
		resp, err := c.Get(u)
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
		return resp.StatusCode, sb.String(), resp.Header.Get("Location")
	}
	post := func(u string, f url.Values) (int, string, string) {
		resp, err := c.PostForm(u, f)
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
		return resp.StatusCode, sb.String(), resp.Header.Get("Location")
	}
	code, page, loc0 := get(authURL)
	if code != 200 {
		t.Fatalf("authorize: %d %q %.300s", code, loc0, page)
	}
	code, page, _ = post(formURL(t, page), url.Values{"username": {u.user}, "password": {u.pass}})
	if code != 200 || !strings.Contains(page, "otp") {
		t.Fatalf("expected OTP form after password: %d %.300s", code, page)
	}
	var loc string
	// a counter used by an earlier process (Keycloak keeps it per user) is refused: retry with the next one
	for try := 0; try < 4; try++ {
		code, page, loc = post(formURL(t, page), url.Values{"otp": {nextTOTP(t, u.secret)}, "login": {"Sign In"}})
		if code == 302 {
			break
		}
	}
	if code != 302 || !strings.HasPrefix(loc, redirect) {
		t.Fatalf("expected redirect to %s: %d %q %.300s", redirect, code, loc, page)
	}
	return loc
}

func pkce() (verifier, challenge string) {
	verifier = b64(randBytes(32))
	d := sha256.Sum256([]byte(verifier))
	return verifier, b64(d[:])
}

func (l *live) form(t *testing.T, endpoint string, f url.Values) (int, map[string]any) {
	t.Helper()
	code, b, _ := l.do("POST", endpoint, map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, strings.NewReader(f.Encode()))
	m := map[string]any{}
	_ = json.Unmarshal(b, &m)
	return code, m
}

// TestLiveUserTokens: an interactive (password+TOTP) login yields tokens the verifier must treat fail-closed.
func TestLiveUserTokens(t *testing.T) {
	u := needUser(t)
	l := newLive(t)
	ver, chal := pkce()
	code := l.userCode(t, u, "demo-sso", "http://localhost:9101/", "openid profile", chal)
	st, tok := l.form(t, l.base+"/protocol/openid-connect/token", url.Values{"grant_type": {"authorization_code"}, "code": {code},
		"redirect_uri": {"http://localhost:9101/"}, "client_id": {"demo-sso"}, "code_verifier": {ver}})
	if st != 200 {
		t.Fatalf("code exchange: %d %v", st, tok)
	}
	at, _ := tok["access_token"].(string)
	cl := decodePart(t, at, 1)
	t.Logf("user access token claims: typ=%v aud=%v azp=%v scope=%q tenant_id=%v amr=%v sub=%v", cl["typ"], cl["aud"], cl["azp"], cl["scope"], cl["tenant_id"], cl["amr"], cl["sub"])
	idc := decodePart(t, tok["id_token"].(string), 1)
	t.Logf("id token: typ=%v amr=%v acr=%v preferred_username=%v", idc["typ"], idc["amr"], idc["acr"], idc["preferred_username"])

	t.Run("id token is not an access token", func(t *testing.T) {
		if _, err := l.verifier(nil).Verify(ctx(t), tok["id_token"].(string)); err == nil {
			t.Fatal("ID token accepted by the resource server verifier")
		}
	})
	t.Run("user access token has no machine tenant and no resource audience", func(t *testing.T) {
		if _, err := l.verifier(nil).Verify(ctx(t), at); err == nil {
			t.Fatalf("interactive user token accepted by a resource-server verifier: aud=%v tenant=%v", cl["aud"], cl["tenant_id"])
		}
	})
	t.Run("userinfo works with the user token", func(t *testing.T) {
		c, _, _ := l.do("GET", l.base+"/protocol/openid-connect/userinfo", map[string]string{"Authorization": "Bearer " + at}, nil)
		if c != 200 {
			t.Fatalf("userinfo: %d", c)
		}
	})
	t.Run("authorization code is single use", func(t *testing.T) {
		st, _ := l.form(t, l.base+"/protocol/openid-connect/token", url.Values{"grant_type": {"authorization_code"}, "code": {code},
			"redirect_uri": {"http://localhost:9101/"}, "client_id": {"demo-sso"}, "code_verifier": {ver}})
		if st == 200 {
			t.Fatal("code redeemed twice")
		}
	})
	t.Run("wrong PKCE verifier is refused", func(t *testing.T) {
		v2, c2 := pkce()
		_ = v2
		code2 := l.userCode(t, u, "demo-sso", "http://localhost:9101/", "openid", c2)
		st, _ := l.form(t, l.base+"/protocol/openid-connect/token", url.Values{"grant_type": {"authorization_code"}, "code": {code2},
			"redirect_uri": {"http://localhost:9101/"}, "client_id": {"demo-sso"}, "code_verifier": {b64(randBytes(32))}})
		if st == 200 {
			t.Fatal("token issued with the wrong code_verifier")
		}
	})
}

const exchangeGrant = "urn:ietf:params:oauth:grant-type:token-exchange"

// newPortalClient is a throw-away confidential client for the authorization code flow (PKCE S256,
// private_key_jwt) with standard token exchange switched on or off, like astranex-portal.
func (l *live) newPortalClient(exchange, keepTenant bool, redirect string) (*client, string) {
	l.t.Helper()
	c := l.newClient(suffix(), tenantA, 0)
	cur := map[string]any{}
	code, b, _ := l.adminJSON("GET", "/clients/"+c.uuid, nil)
	if code != 200 {
		l.t.Fatalf("get client: %d %s", code, b)
	}
	_ = json.Unmarshal(b, &cur)
	attrs := cur["attributes"].(map[string]any)
	attrs["pkce.code.challenge.method"] = "S256"
	attrs["standard.token.exchange.enabled"] = fmt.Sprint(exchange)
	cur["standardFlowEnabled"] = true
	cur["serviceAccountsEnabled"] = false
	cur["redirectUris"] = []string{redirect}
	cur["defaultClientScopes"] = []string{"aas-base", "basic", "profile", "acr"}
	l.mustAdmin("PUT", "/clients/"+c.uuid, cur, 204)
	// a user client carries no machine tenant: drop the hard-coded tenant_id mapper newClient adds
	if ms, ok := cur["protocolMappers"].([]any); ok {
		for _, m := range ms {
			if mm := m.(map[string]any); mm["name"] == "tenant" && !keepTenant {
				l.mustAdmin("DELETE", "/clients/"+c.uuid+"/protocol-mappers/models/"+mm["id"].(string), nil, 204)
			}
		}
	}
	// the exchange audience must be reachable through the client's scopes (audience mapper of the resource scope)
	code, b, _ = l.adminJSON("GET", "/client-scopes", nil)
	var scopes []struct{ ID, Name string }
	_ = json.Unmarshal(b, &scopes)
	for _, sc := range scopes {
		if sc.Name == scopeWrite || sc.Name == scopeQuery || sc.Name == "basic" {
			l.mustAdmin("PUT", "/clients/"+c.uuid+"/default-client-scopes/"+sc.ID, nil, 204)
		}
	}
	return c, redirect
}

func (l *live) codeToken(t *testing.T, c *client, redirect string, u userEnv) map[string]any {
	t.Helper()
	ver, chal := pkce()
	code := l.userCode(t, u, c.ID, redirect, "openid", chal)
	st, tok := l.form(t, l.base+"/protocol/openid-connect/token", url.Values{"grant_type": {"authorization_code"}, "code": {code},
		"redirect_uri": {redirect}, "client_id": {c.ID}, "code_verifier": {ver},
		"client_assertion_type": {assertionTy}, "client_assertion": {l.assertion(c)}})
	if st != 200 {
		t.Fatalf("code exchange for %s: %d %v", c.ID, st, tok)
	}
	return tok
}

// TestLiveTokenExchange: standard token exchange of a user access token to another audience.
func TestLiveTokenExchange(t *testing.T) {
	u := needUser(t)
	l := newLive(t)
	const target = "https://api.astryon.sv4.one" // resource server client defined in the realm (astryon.yaml)
	on, redirect := l.newPortalClient(true, false, "http://localhost:9199/cb")
	off, redirect2 := l.newPortalClient(false, false, "http://localhost:9199/cb")
	tokOn := l.codeToken(t, on, redirect, u)
	tokOff := l.codeToken(t, off, redirect2, u)
	src := tokOn["access_token"].(string)
	srcClaims := decodePart(t, src, 1)

	exchange := func(c *client, subject, audience string) (int, map[string]any) {
		f := url.Values{"grant_type": {exchangeGrant}, "client_id": {c.ID}, "subject_token": {subject},
			"subject_token_type":    {"urn:ietf:params:oauth:token-type:access_token"},
			"requested_token_type":  {"urn:ietf:params:oauth:token-type:access_token"},
			"client_assertion_type": {assertionTy}, "client_assertion": {l.assertion(c)}}
		if audience != "" {
			f.Set("audience", audience)
		}
		return l.form(t, l.base+"/protocol/openid-connect/token", f)
	}

	t.Run("exchange to a resource audience keeps the user", func(t *testing.T) {
		st, r := exchange(on, src, target)
		if st != 200 {
			t.Fatalf("exchange: %d %v", st, r)
		}
		ex := decodePart(t, r["access_token"].(string), 1)
		t.Logf("exchanged claims: aud=%v azp=%v sub=%v scope=%q tenant_id=%v exp=%v (source exp=%v)", ex["aud"], ex["azp"], ex["sub"], ex["scope"], ex["tenant_id"], ex["exp"], srcClaims["exp"])
		if ex["sub"] != srcClaims["sub"] {
			t.Fatalf("subject changed: %v -> %v", srcClaims["sub"], ex["sub"])
		}
		if !strings.Contains(fmt.Sprint(ex["aud"]), target) {
			t.Fatalf("target audience missing: %v", ex["aud"])
		}
		// docs/discrepancies Р5: Keycloak does not cap exp of the exchanged token by the source exp
		if d := ex["exp"].(float64) - srcClaims["exp"].(float64); d > 1 {
			t.Logf("KNOWN GAP (Р5): exchanged token outlives its source by %.0f s", d)
		}
		if ex["exp"].(float64) > float64(time.Now().Add(15*time.Minute).Unix()) {
			t.Fatalf("exchanged token lifetime unbounded: exp=%v", ex["exp"])
		}
		// no machine tenant on a user token: resource servers must keep refusing it
		v := l.verifier(func(c *auth.Config) { c.Audience = target })
		if _, err := v.Verify(ctx(t), r["access_token"].(string)); err == nil {
			t.Fatal("exchanged user token accepted without a tenant_id claim")
		}
	})
	t.Run("client without exchange permission is refused", func(t *testing.T) {
		if st, r := exchange(off, tokOff["access_token"].(string), target); st == 200 {
			t.Fatalf("exchange allowed for a client with standard.token.exchange.enabled=false: %v", r)
		}
	})
	t.Run("unknown audience is refused", func(t *testing.T) {
		if st, r := exchange(on, src, "no-such-client-"+suffix()); st == 200 {
			t.Fatalf("exchange to unknown audience succeeded: %v", r)
		}
	})
	t.Run("other client cannot exchange a token issued to someone else", func(t *testing.T) {
		if st, r := exchange(off, src, target); st == 200 {
			t.Fatalf("foreign subject token exchanged: %v", r)
		}
	})
	t.Run("garbage subject token is refused", func(t *testing.T) {
		if st, _ := exchange(on, "not.a.jwt", target); st == 200 {
			t.Fatal("garbage subject token exchanged")
		}
	})
}

// assertionWith builds a client assertion with explicit iat/exp (the proxy enforces exp-iat <= 60 s).
func (l *live) assertionWith(c *client, iat, exp int64, jti string) string {
	h, _ := json.Marshal(map[string]string{"alg": "ES256", "typ": "JWT", "kid": c.kid()})
	claims := map[string]any{"iss": c.ID, "sub": c.ID, "aud": l.base, "jti": jti, "exp": exp}
	if iat != 0 {
		claims["iat"] = iat
	}
	p, _ := json.Marshal(claims)
	in := b64(h) + "." + b64(p)
	d := sha256.Sum256([]byte(in))
	r, s, _ := ecdsa.Sign(rand.Reader, c.key, d[:])
	return in + "." + b64(append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...))
}

// TestLiveProxy: the standard-address proxy in front of Keycloak (AAS_LIVE_PROXY_URL, e.g. https://localhost:18444).
func TestLiveProxy(t *testing.T) {
	px := strings.TrimRight(os.Getenv("AAS_LIVE_PROXY_URL"), "/")
	if px == "" {
		t.Skip("AAS_LIVE_PROXY_URL not set")
	}
	l := newLive(t)
	l.newScope(scopeQuery)
	c := l.newClient(suffix(), tenantA, 0, scopeQuery)
	tokenURL := px + "/oauth2/token"
	cc := func(assertion string, extra url.Values) (int, map[string]any) {
		f := url.Values{"grant_type": {"client_credentials"}, "client_id": {c.ID}, "scope": {scopeQuery},
			"client_assertion_type": {assertionTy}, "client_assertion": {assertion}}
		for k, v := range extra {
			f[k] = v
		}
		return l.form(t, tokenURL, f)
	}
	now := time.Now().Unix()

	t.Run("metadata publishes the standard addresses", func(t *testing.T) {
		code, b, _ := l.do("GET", px+"/.well-known/oauth-authorization-server", nil, nil)
		if code != 200 {
			t.Fatalf("metadata: %d %s", code, b)
		}
		for _, p := range []string{"/oauth2/token", "/oauth2/jwks", "/oauth2/authorize"} {
			if !strings.Contains(string(b), p) {
				t.Errorf("metadata lacks %s: %.400s", p, b)
			}
		}
	})
	t.Run("jwks through the proxy equals the realm jwks", func(t *testing.T) {
		_, a, _ := l.do("GET", px+"/oauth2/jwks", nil, nil)
		_, b, _ := l.do("GET", l.base+"/protocol/openid-connect/certs", nil, nil)
		var ja, jb struct{ Keys []struct{ Kid string } }
		_ = json.Unmarshal(a, &ja)
		_ = json.Unmarshal(b, &jb)
		if len(ja.Keys) == 0 || len(ja.Keys) != len(jb.Keys) || ja.Keys[0].Kid != jb.Keys[0].Kid {
			t.Fatalf("proxy jwks %.200s != realm jwks %.200s", a, b)
		}
	})
	var tok string
	t.Run("client_credentials via proxy yields a token the verifier accepts", func(t *testing.T) {
		st, r := cc(l.assertion(c), nil)
		if st != 200 {
			t.Fatalf("token via proxy: %d %v", st, r)
		}
		tok = r["access_token"].(string)
		p, err := l.verifier(func(cfg *auth.Config) { cfg.AllowedAZP = []string{c.ID} }).Verify(ctx(t), tok)
		if err != nil || p.TenantID != tenantA || !p.HasScope(scopeQuery) {
			t.Fatalf("%+v %v", p, err)
		}
	})
	t.Run("replayed assertion jti is 401 (not Keycloak's 400)", func(t *testing.T) {
		a := l.assertionWith(c, now, now+60, "replay-"+suffix())
		if st, r := cc(a, nil); st != 200 {
			t.Fatalf("first use: %d %v", st, r)
		}
		if st, r := cc(a, nil); st != 401 {
			t.Fatalf("replay: %d %v (want 401 invalid_client)", st, r)
		}
	})
	t.Run("assertion lifetime above 60 s is 401", func(t *testing.T) {
		if st, r := cc(l.assertionWith(c, now, now+120, "long-"+suffix()), nil); st != 401 {
			t.Fatalf("exp-iat=120: %d %v (want 401)", st, r)
		}
	})
	t.Run("assertion without iat is 401", func(t *testing.T) {
		if st, r := cc(l.assertionWith(c, 0, now+60, "noiat-"+suffix()), nil); st != 401 {
			t.Fatalf("no iat: %d %v (want 401)", st, r)
		}
	})
	t.Run("resource other than the gateway is refused for client_credentials", func(t *testing.T) {
		if st, r := cc(l.assertion(c), url.Values{"resource": {"https://evil.example"}}); st != 400 {
			t.Fatalf("resource=evil: %d %v (want 400 invalid_target)", st, r)
		}
	})
	t.Run("introspection endpoint reachable via proxy", func(t *testing.T) {
		_ = tok
		f := url.Values{"token": {tok}, "client_id": {c.ID}, "client_assertion_type": {assertionTy}, "client_assertion": {l.assertion(c)}}
		st, r := l.form(t, px+"/oauth2/introspect", f)
		t.Logf("introspect via proxy: %d active=%v", st, r["active"])
		if st != 200 {
			t.Fatalf("introspect: %d %v", st, r)
		}
	})
}
