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

	"github.com/astryon/go-common/auth"
	"github.com/astryon/quarantine/internal/replay"
)

const aasTenant = "0a1b2c3d-1111-4222-8333-444455556666"

func TestAASAdapterEndToEnd(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var aas *httptest.Server
	aas = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": aas.URL + "/issuer", "jwks_uri": aas.URL + "/oauth2/jwks"})
		case "/oauth2/jwks":
			public := key.PublicKey
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{
				"kid": "key-1", "kty": "RSA", "n": base64.RawURLEncoding.EncodeToString(public.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(public.E)).Bytes()),
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer aas.Close()

	verifier, err := auth.NewVerifier(context.Background(), auth.Config{
		BaseURL: aas.URL, Issuer: aas.URL + "/issuer", Audience: "astryon-quarantine",
		Environment: "test", AllowedAZP: []string{"portal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var clickHouseTenant string
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clickHouseTenant = r.URL.Query().Get("param_p_tenant")
	}))
	defer clickHouse.Close()
	api := httptest.NewServer((replay.BrowserHandler{
		Auth: aasAuthenticator{verifier: verifier}, Store: &replay.ClickHouseStore{URL: clickHouse.URL},
	}).Routes())
	defer api.Close()

	call := func(token string) int {
		req, _ := http.NewRequest(http.MethodGet, api.URL+"/v1/quarantine", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}
	if status := call(""); status != http.StatusUnauthorized {
		t.Fatalf("no token: %d", status)
	}
	if status := call(aasToken(t, key, aas.URL+"/issuer", replay.ScopeQuarantineRead)); status != http.StatusOK || clickHouseTenant != aasTenant {
		t.Fatalf("valid status=%d tenant=%q", status, clickHouseTenant)
	}
	if status := call(aasToken(t, key, aas.URL+"/issuer", "astryon:events:write")); status != http.StatusForbidden {
		t.Fatalf("missing scope: %d", status)
	}
	token := aasToken(t, key, aas.URL+"/issuer", replay.ScopeQuarantineRead)
	if status := call(token[:len(token)-4] + "AAAA"); status != http.StatusUnauthorized {
		t.Fatalf("tampered signature: %d", status)
	}
}

func aasToken(t *testing.T, key *rsa.PrivateKey, issuer, scope string) string {
	t.Helper()
	now := time.Now()
	claims := map[string]any{
		"iss": issuer, "aud": []string{"astryon-quarantine"}, "env": "test", "typ": "at+jwt", "azp": "portal",
		"exp": now.Add(time.Hour).Unix(), "nbf": now.Add(-time.Minute).Unix(), "iat": now.Add(-time.Minute).Unix(),
		"jti": "quarantine-test", "sub": "operator", "client_id": "portal", "tenant_id": aasTenant, "scope": scope,
	}
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "key-1"})
	body, _ := json.Marshal(claims)
	signed := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	digest := sha256.Sum256([]byte(signed))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join([]string{signed, base64.RawURLEncoding.EncodeToString(signature)}, ".")
}
