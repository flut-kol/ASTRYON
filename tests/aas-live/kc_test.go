package aaslive

// Keycloak helpers for the live harness. Everything is provisioned at run time through the
// Admin REST API of the AAS under test and removed again in t.Cleanup; nothing is committed.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	tenantA     = "0a1b2c3d-1111-4222-8333-444455556666"
	scopeWrite  = "astryon:events:write"
	scopeQuery  = "astryon:query:read"
	assertionTy = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
)

type live struct {
	t        *testing.T
	base     string // realm URL, e.g. https://host/realms/aas
	origin   string
	realm    string
	audience string
	env      string
	http     *http.Client
	jwksHits atomic.Int64
	admin    string // admin bearer
	adminUsr string
	adminPwd string
}

type countingTransport struct {
	rt   http.RoundTripper
	hits *atomic.Int64
}

func (c countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Path, "/certs") {
		c.hits.Add(1)
	}
	return c.rt.RoundTrip(r)
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func newLive(t *testing.T) *live {
	base := os.Getenv("AAS_LIVE_BASE_URL")
	if base == "" {
		t.Skip("AAS_LIVE_BASE_URL not set")
	}
	l := &live{t: t, base: strings.TrimRight(base, "/"), audience: getenv("AAS_LIVE_AUDIENCE", "https://api.aggregator.example"), env: getenv("AAS_LIVE_ENV", "dev"),
		adminUsr: os.Getenv("AAS_LIVE_ADMIN_USER"), adminPwd: os.Getenv("AAS_LIVE_ADMIN_PASSWORD")}
	if l.adminUsr == "" || l.adminPwd == "" {
		t.Skip("AAS_LIVE_ADMIN_USER / AAS_LIVE_ADMIN_PASSWORD not set (needed to provision test clients)")
	}
	u, err := url.Parse(l.base)
	if err != nil {
		t.Fatal(err)
	}
	i := strings.LastIndex(u.Path, "/realms/")
	if i < 0 {
		t.Fatalf("AAS_LIVE_BASE_URL must look like https://host[/prefix]/realms/<realm>, got %s", l.base)
	}
	l.realm = u.Path[i+len("/realms/"):]
	l.origin = u.Scheme + "://" + u.Host + u.Path[:i]
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if ca := os.Getenv("AAS_LIVE_CA_FILE"); ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			t.Fatal(err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	l.http = &http.Client{Transport: countingTransport{tr, &l.jwksHits}, Timeout: 20 * time.Second}
	l.adminLogin()
	return l
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func (l *live) do(method, u string, hdr map[string]string, body io.Reader) (int, []byte, http.Header) {
	l.t.Helper()
	code, b, h, err := l.doSoft(method, u, hdr, body)
	if err != nil {
		l.t.Fatalf("%s %s: %v", method, u, err)
	}
	return code, b, h
}

func (l *live) doSoft(method, u string, hdr map[string]string, body io.Reader) (int, []byte, http.Header, error) {
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		return 0, nil, nil, err
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := l.http.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, resp.Header, nil
}

// waitReady blocks until the token endpoint answers again (after an AAS restart).
func (l *live) waitReady(d time.Duration) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(2 * time.Second) {
		if code, _, _, err := l.doSoft("GET", l.base+"/.well-known/openid-configuration", nil, nil); err == nil && code == 200 {
			return true
		}
	}
	return false
}

func (l *live) adminLogin() {
	form := url.Values{"grant_type": {"password"}, "client_id": {"admin-cli"}, "username": {l.adminUsr}, "password": {l.adminPwd}}
	code, b, _ := l.do("POST", l.origin+"/realms/master/protocol/openid-connect/token", map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, strings.NewReader(form.Encode()))
	if code != 200 {
		l.t.Fatalf("admin login: %d", code)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.Unmarshal(b, &tok)
	l.admin = tok.AccessToken
}

func (l *live) adminJSON(method, path string, in any) (int, []byte, http.Header) {
	l.t.Helper()
	var body io.Reader
	hdr := map[string]string{"Authorization": "Bearer " + l.admin}
	if in != nil {
		raw, _ := json.Marshal(in)
		body = bytes.NewReader(raw)
		hdr["Content-Type"] = "application/json"
	}
	code, b, h := l.do(method, l.origin+"/admin/realms/"+l.realm+path, hdr, body)
	if code == 401 { // admin token (60s) expired during a long test
		l.adminLogin()
		return l.adminJSON(method, path, in)
	}
	return code, b, h
}

func (l *live) mustAdmin(method, path string, in any, want int) http.Header {
	l.t.Helper()
	code, b, h := l.adminJSON(method, path, in)
	if code != want {
		l.t.Fatalf("admin %s %s: %d %s", method, path, code, b)
	}
	return h
}

type client struct {
	ID     string
	key    *ecdsa.PrivateKey
	uuid   string
	issuer string
}

func (l *live) newScope(name string) {
	l.t.Helper()
	code, b, h := l.adminJSON("POST", "/client-scopes", map[string]any{"name": name, "protocol": "openid-connect", "attributes": map[string]string{"include.in.token.scope": "true", "display.on.consent.screen": "false"}})
	if code != 201 {
		l.t.Fatalf("create scope %s: %d %s", name, code, b)
	}
	id := h.Get("Location")[strings.LastIndex(h.Get("Location"), "/")+1:]
	l.t.Cleanup(func() { l.adminJSON("DELETE", "/client-scopes/"+id, nil) })
}

// newClient creates a service-account client authenticated by private_key_jwt (ES256), the same
// method the ASTRYON docs prescribe, with tenant as hard-coded claim mapper (as AAS client config does).
func (l *live) newClient(sfx, tenant string, lifespanSec int, optional ...string) *client {
	l.t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	x, y := key.PublicKey.X.FillBytes(make([]byte, 32)), key.PublicKey.Y.FillBytes(make([]byte, 32))
	canon := fmt.Sprintf(`{"crv":"P-256","kty":"EC","x":"%s","y":"%s"}`, b64(x), b64(y))
	th := sha256.Sum256([]byte(canon))
	jwk := map[string]string{"kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "x": b64(x), "y": b64(y), "kid": b64(th[:])}
	jwks, _ := json.Marshal(map[string]any{"keys": []any{jwk}})
	id := "astryon-live-" + sfx
	attrs := map[string]string{"use.jwks.url": "false", "use.jwks.string": "true", "jwks.string": string(jwks),
		"token.endpoint.auth.signing.alg": "ES256", "client_credentials.use_refresh_token": "false"}
	if lifespanSec > 0 {
		attrs["access.token.lifespan"] = fmt.Sprint(lifespanSec)
	}
	rep := map[string]any{"clientId": id, "enabled": true, "publicClient": false, "serviceAccountsEnabled": true,
		"standardFlowEnabled": false, "directAccessGrantsEnabled": false, "clientAuthenticatorType": "client-jwt",
		"attributes": attrs, "defaultClientScopes": []string{"aas-base", "gateway"}, "optionalClientScopes": optional,
		"protocolMappers": []any{map[string]any{"name": "tenant", "protocol": "openid-connect", "protocolMapper": "oidc-hardcoded-claim-mapper",
			"config": map[string]string{"claim.name": "tenant_id", "claim.value": tenant, "jsonType.label": "String", "access.token.claim": "true", "id.token.claim": "false"}}}}
	h := l.mustAdmin("POST", "/clients", rep, 201)
	uuid := h.Get("Location")[strings.LastIndex(h.Get("Location"), "/")+1:]
	l.t.Cleanup(func() { l.adminJSON("DELETE", "/clients/"+uuid, nil) })
	return &client{ID: id, key: key, uuid: uuid, issuer: l.base}
}

func (l *live) assertion(c *client) string {
	now := time.Now().Unix()
	h, _ := json.Marshal(map[string]string{"alg": "ES256", "typ": "JWT", "kid": c.kid()})
	p, _ := json.Marshal(map[string]any{"iss": c.ID, "sub": c.ID, "aud": l.base, "jti": b64(randBytes(12)), "iat": now, "exp": now + 60})
	in := b64(h) + "." + b64(p)
	d := sha256.Sum256([]byte(in))
	r, s, _ := ecdsa.Sign(rand.Reader, c.key, d[:])
	sig := append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	return in + "." + b64(sig)
}

func (c *client) kid() string {
	x, y := c.key.PublicKey.X.FillBytes(make([]byte, 32)), c.key.PublicKey.Y.FillBytes(make([]byte, 32))
	th := sha256.Sum256([]byte(fmt.Sprintf(`{"crv":"P-256","kty":"EC","x":"%s","y":"%s"}`, b64(x), b64(y))))
	return b64(th[:])
}

func randBytes(n int) []byte { b := make([]byte, n); _, _ = rand.Read(b); return b }

// token performs client_credentials against the real token endpoint.
func (l *live) token(c *client, scope string) string {
	l.t.Helper()
	tok, code, body := l.tryToken(c, scope)
	if code != 200 {
		l.t.Fatalf("token for %s: %d %s", c.ID, code, body)
	}
	return tok
}

func (l *live) tryToken(c *client, scope string) (string, int, string) {
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {c.ID}, "client_assertion_type": {assertionTy}, "client_assertion": {l.assertion(c)}}
	if scope != "" {
		form.Set("scope", scope)
	}
	code, b, _, err := l.doSoft("POST", l.base+"/protocol/openid-connect/token", map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err.Error()
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.Unmarshal(b, &tok)
	return tok.AccessToken, code, string(b)
}

func decodePart(t *testing.T, tok string, i int) map[string]any {
	t.Helper()
	parts := strings.Split(tok, ".")
	raw, err := base64.RawURLEncoding.DecodeString(parts[i])
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]any{}
	_ = json.Unmarshal(raw, &m)
	return m
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return c
}
