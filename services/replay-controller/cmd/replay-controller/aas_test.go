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
	"github.com/astryon/replay-controller/internal/replay"
)

const (
	claimTenant = "0a1b2c3d-1111-4222-8333-444455556666"
	otherTenant = "99999999-1111-4222-8333-444455556666"
	srcID       = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	replayID    = "00000000-0000-0000-0000-0000000000c1"
)

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
	v, err := auth.NewVerifier(context.Background(), auth.Config{BaseURL: aas.URL, Issuer: issuerURL, Audience: "astryon-replay", Environment: "test", AllowedAZP: []string{"portal"}})
	if err != nil {
		t.Fatal(err)
	}
	act := &replay.MemActivator{Replays: map[string]replay.MemReplay{
		replayID: {TenantID: claimTenant, SourceID: srcID, Status: "COMPLETED", Version: 1},
	}}
	audit := &replay.MemAuditSink{}
	api := httptest.NewServer((&replay.Handler{Auth: aasAuthenticator{v}, Planner: &replay.Planner{}, Activator: act, Audit: audit, AuditRequired: true}).Routes())
	defer api.Close()

	call := func(tok, body string) int {
		req, _ := http.NewRequest("POST", api.URL+"/v1/replays/"+replayID+"/activate", strings.NewReader(body))
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
	if c := call("", ""); c != 401 {
		t.Fatalf("no token: %d", c)
	}
	if c := call(token(t, key, "astryon:query:read"), ""); c != 403 {
		t.Fatalf("missing scope: %d", c)
	}
	good := token(t, key, "astryon:replay:write other")
	if c := call(good[:len(good)-4]+"AAAA", ""); c != 401 {
		t.Fatalf("tampered signature: %d", c)
	}
	if len(audit.Snapshot()) != 0 {
		t.Fatal("audit written for refused requests")
	}
	// a tenant_id in the body is ignored for activation (no body is read); tenant is the claim only
	if c := call(good, `{"tenant_id":"`+otherTenant+`"}`); c != 200 {
		t.Fatalf("valid: %d", c)
	}
	if a, _, ok := act.Active(claimTenant, srcID); !ok || a != 1 {
		t.Fatal("not activated for the claim tenant")
	}
	if _, _, ok := act.Active(otherTenant, srcID); ok {
		t.Fatal("body tenant honoured")
	}
	ev := audit.Snapshot()
	if len(ev) != 2 || ev[0].Phase != "prepared" || ev[1].Phase != "committed" || ev[1].TenantID != claimTenant || ev[1].Subject != "user-42" || ev[1].Action != "replay.activate" || ev[1].Outcome != "success" {
		t.Fatalf("audit: %+v", ev)
	}
	// POST /v1/replays: a body tenant_id is rejected (400) after authentication
	req, _ := http.NewRequest("POST", api.URL+"/v1/replays", strings.NewReader(`{"tenant_id":"`+otherTenant+`"}`))
	req.Header.Set("Authorization", "Bearer "+good)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("body tenant: %d", resp.StatusCode)
	}
}

func TestDenyAll(t *testing.T) {
	if _, err := (denyAll{}).Authenticate(httptest.NewRequest("GET", "/", nil)); err != replay.ErrUnauthenticated {
		t.Fatal(err)
	}
}

var issuerURL string

func token(t *testing.T, key *rsa.PrivateKey, scope string) string {
	t.Helper()
	now := time.Now()
	return sign(t, key, map[string]any{"iss": issuerURL, "aud": []string{"astryon-replay"}, "env": "test", "typ": "at+jwt", "azp": "portal",
		"exp": now.Add(time.Hour).Unix(), "nbf": now.Add(-time.Minute).Unix(), "iat": now.Add(-time.Minute).Unix(), "jti": "j", "sub": "user-42",
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
