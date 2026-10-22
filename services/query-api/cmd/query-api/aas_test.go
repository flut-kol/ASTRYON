package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/astryon/audit"
	"github.com/astryon/go-common/auth"
	"github.com/astryon/query-api/internal/query"
)

const claimTenant = "0a1b2c3d-1111-4222-8333-444455556666"

func TestAASAdapterEndToEnd(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	var aas *httptest.Server
	aas = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": aas.URL + "/issuer", "jwks_uri": aas.URL + "/oauth2/jwks"})
		case "/oauth2/jwks":
			pub := key.PublicKey
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{"kid": "k", "kty": "RSA",
				"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes())}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer aas.Close()
	issuerURL = aas.URL + "/issuer"
	v, err := auth.NewVerifier(context.Background(), auth.Config{BaseURL: aas.URL, Issuer: aas.URL + "/issuer", Audience: "astryon-query", Environment: "test", AllowedAZP: []string{"portal"}})
	if err != nil {
		t.Fatal(err)
	}
	var chTenant string
	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { chTenant = r.URL.Query().Get("param_p_tenant") }))
	defer ch.Close()
	auditSink := &aasAuditCapture{}
	api := httptest.NewServer((&query.Server{Auth: aasAuthenticator{v: v}, CHURL: ch.URL, Audit: auditSink}).Handler())
	defer api.Close()

	call := func(tok string) int {
		req, _ := http.NewRequest("POST", api.URL+"/v1/query/events", strings.NewReader(`{"from":"2026-10-20T00:00:00Z","to":"2026-10-21T00:00:00Z","processing_version":1}`))
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := call(""); c != 401 {
		t.Fatalf("no token: %d", c)
	}
	if c := call(token(t, key, "astryon:query:read other")); c != 200 || chTenant != claimTenant {
		t.Fatalf("valid: %d tenant=%q", c, chTenant)
	}
	if c := call(token(t, key, "astryon:events:write")); c != 403 {
		t.Fatalf("missing scope: %d", c)
	}
	if len(auditSink.events) != 1 || auditSink.events[0].Subject != "u" || auditSink.events[0].ClientID != "portal" || auditSink.events[0].ACR != "mfa" {
		t.Fatalf("principal audit mapping = %+v", auditSink.events)
	}
	tok := token(t, key, "astryon:query:read")
	if c := call(tok[:len(tok)-4] + "AAAA"); c != 401 {
		t.Fatalf("tampered signature: %d", c)
	}
}

type aasAuditCapture struct{ events []audit.Event }

func (a *aasAuditCapture) Submit(e audit.Event) error {
	a.events = append(a.events, e)
	return nil
}

var issuerURL string

func token(t *testing.T, key *rsa.PrivateKey, scope string) string {
	t.Helper()
	now := time.Now()
	return sign(t, key, map[string]any{"iss": issuerURL, "aud": []string{"astryon-query"}, "env": "test", "typ": "at+jwt", "azp": "portal",
		"exp": now.Add(time.Hour).Unix(), "nbf": now.Add(-time.Minute).Unix(), "iat": now.Add(-time.Minute).Unix(), "jti": "j", "sub": "u",
		"client_id": "portal", "tenant_id": claimTenant, "scope": scope, "acr": "mfa", "amr": []string{"pwd", "otp"}})
}

func sign(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	head, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "k"})
	body, _ := json.Marshal(claims)
	signed := base64.RawURLEncoding.EncodeToString(head) + "." + base64.RawURLEncoding.EncodeToString(body)
	d := sha256.Sum256([]byte(signed))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, d[:])
	if err != nil {
		t.Fatal(err)
	}
	return signed + "." + base64.RawURLEncoding.EncodeToString(sig)
}
