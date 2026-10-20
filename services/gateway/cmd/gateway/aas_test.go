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

	"github.com/astryon/gateway/internal/gateway"
	"github.com/astryon/go-common/auth"
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
	v, err := auth.NewVerifier(context.Background(), auth.Config{BaseURL: aas.URL, Issuer: aas.URL + "/issuer", Audience: "astryon-gateway", Environment: "test", AllowedAZP: []string{"portal"}})
	if err != nil {
		t.Fatal(err)
	}
	srv := gateway.NewServer(gateway.Config{}, aasAuthenticator{v}, &gateway.FakePublisher{})
	api := httptest.NewServer(srv.Handler())
	defer api.Close()

	call := func(tok string) int {
		req, _ := http.NewRequest("POST", api.URL+"/v1/events", strings.NewReader(`{}`))
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
	if c := call(token(t, key, "astryon:events:write other")); c == 401 || c == 403 {
		t.Fatalf("valid token rejected: %d", c)
	}
	if c := call(token(t, key, "astryon:other")); c != 403 {
		t.Fatalf("missing scope: %d", c)
	}
	tok := token(t, key, "astryon:events:write")
	if c := call(tok[:len(tok)-4] + "AAAA"); c != 401 {
		t.Fatalf("tampered signature: %d", c)
	}
}

var issuerURL string

func token(t *testing.T, key *rsa.PrivateKey, scope string) string {
	t.Helper()
	now := time.Now()
	return sign(t, key, map[string]any{"iss": issuerURL, "aud": []string{"astryon-gateway"}, "env": "test", "typ": "at+jwt", "azp": "portal",
		"exp": now.Add(time.Hour).Unix(), "nbf": now.Add(-time.Minute).Unix(), "iat": now.Add(-time.Minute).Unix(), "jti": "j", "sub": "u",
		"client_id": "portal", "tenant_id": claimTenant, "scope": scope})
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
