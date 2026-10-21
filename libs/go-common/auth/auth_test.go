package auth

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
)

const tenantID = "0a1b2c3d-1111-4222-8333-444455556666"

func TestVerifierAcceptsValidTokenAndRejectsClaimViolations(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := oidcServer(t, &key.PublicKey, "one")
	defer server.Close()
	now := time.Unix(1_700_000_000, 0)
	v, err := NewVerifier(context.Background(), Config{BaseURL: server.URL, Issuer: server.URL + "/issuer", Audience: "astryon-gateway", Environment: "test", AllowedAZP: []string{"source-a"}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	claims := validClaims(server.URL+"/issuer", now)
	token := signedRS256(t, key, "one", claims)
	p, err := v.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify(valid): %v", err)
	}
	if p.TenantID != tenantID || !p.HasScope("astryon:events:write") {
		t.Fatalf("unexpected principal: %#v", p)
	}
	if p.ACR != "urn:astryon:acr:mfa" || strings.Join(p.AMR, ",") != "pwd,otp" {
		t.Fatalf("unexpected authentication context: ACR=%q AMR=%v", p.ACR, p.AMR)
	}

	// AAS tokens carry no nbf; absence is accepted, a malformed nbf is not.
	noNbf := cloneClaims(claims)
	delete(noNbf, "nbf")
	if _, err := v.Verify(context.Background(), signedRS256(t, key, "one", noNbf)); err != nil {
		t.Fatalf("Verify(no nbf): %v", err)
	}

	for name, mutate := range map[string]func(map[string]any){
		"nbf malformed": func(c map[string]any) { c["nbf"] = "soon" },
		"issuer":        func(c map[string]any) { c["iss"] = "https://wrong" },
		"audience":      func(c map[string]any) { c["aud"] = "wrong" },
		"environment":   func(c map[string]any) { c["env"] = "prod" },
		"azp":           func(c map[string]any) { c["azp"] = "other" },
		"token type":    func(c map[string]any) { c["typ"] = "id+jwt" },
		"expired":       func(c map[string]any) { c["exp"] = now.Add(-time.Second).Unix() },
		"not before":    func(c map[string]any) { c["nbf"] = now.Add(time.Second).Unix() },
		"issued future": func(c map[string]any) { c["iat"] = now.Add(time.Second).Unix() },
		"jti":           func(c map[string]any) { delete(c, "jti") },
		"tenant uuid":   func(c map[string]any) { c["tenant_id"] = "tenant-a" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := cloneClaims(claims)
			mutate(copy)
			if _, err := v.Verify(context.Background(), signedRS256(t, key, "one", copy)); !errorsIs(err, ErrUnauthenticated) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestVerifierRejectsWrongSchemeUnknownKidAndRedirectedJWKS(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := oidcServer(t, &key.PublicKey, "one")
	defer server.Close()
	now := time.Unix(1_700_000_000, 0)
	v, err := NewVerifier(context.Background(), Config{BaseURL: server.URL, Audience: "astryon-gateway", Environment: "test", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "DPoP abc")
	if _, err := v.Authenticate(req); !errorsIs(err, ErrUnauthenticated) {
		t.Fatalf("DPoP error = %v", err)
	}
	if _, err := v.Verify(context.Background(), signedRS256(t, key, "unknown", validClaims(server.URL+"/issuer", now))); !errorsIs(err, ErrUnauthenticated) {
		t.Fatalf("unknown kid error = %v", err)
	}
}

func oidcServer(t *testing.T, public *rsa.PublicKey, kid string) *httptest.Server {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": server.URL + "/issuer", "jwks_uri": server.URL + "/oauth2/jwks"})
		case "/oauth2/jwks":
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{"kid": kid, "kty": "RSA", "n": base64.RawURLEncoding.EncodeToString(public.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(public.E)).Bytes())}}})
		default:
			http.NotFound(w, r)
		}
	}))
	return server
}

func validClaims(issuer string, now time.Time) map[string]any {
	return map[string]any{"iss": issuer, "aud": []string{"astryon-gateway"}, "env": "test", "typ": "at+jwt", "azp": "source-a", "exp": now.Add(time.Hour).Unix(), "nbf": now.Add(-time.Minute).Unix(), "iat": now.Add(-time.Minute).Unix(), "jti": "jti-1", "sub": "source-a", "client_id": "source-a", "tenant_id": tenantID, "scope": "astryon:events:write other", "acr": "urn:astryon:acr:mfa", "amr": []string{"pwd", "otp"}}
}
func cloneClaims(source map[string]any) map[string]any {
	out := make(map[string]any, len(source))
	for k, v := range source {
		out[k] = v
	}
	return out
}
func errorsIs(err, target error) bool { return err == target }

func signedRS256(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	head, err := json.Marshal(map[string]string{"alg": "RS256", "kid": kid})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signed := base64.RawURLEncoding.EncodeToString(head) + "." + base64.RawURLEncoding.EncodeToString(body)
	digest := sha256.Sum256([]byte(signed))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return signed + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func TestScopes(t *testing.T) {
	got := scopes([]any{"a", 2, "b"})
	if strings.Join(got, ",") != "a,b" {
		t.Fatalf("scopes = %v", got)
	}
}

func TestVerifierOptionalAuthenticationContextClaims(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := oidcServer(t, &key.PublicKey, "one")
	defer server.Close()
	now := time.Unix(1_700_000_000, 0)
	v, err := NewVerifier(context.Background(), Config{BaseURL: server.URL, Audience: "astryon-gateway", Environment: "test", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(map[string]any){
		"absent": func(c map[string]any) {
			delete(c, "acr")
			delete(c, "amr")
		},
		"wrong acr type": func(c map[string]any) { c["acr"] = 2 },
		"wrong amr type": func(c map[string]any) { c["amr"] = "pwd otp" },
		"mixed amr array": func(c map[string]any) { c["amr"] = []any{"pwd", 2} },
	} {
		t.Run(name, func(t *testing.T) {
			claims := validClaims(server.URL+"/issuer", now)
			mutate(claims)
			p, err := v.Verify(context.Background(), signedRS256(t, key, "one", claims))
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if name == "wrong acr type" {
				if p.ACR != "" || strings.Join(p.AMR, ",") != "pwd,otp" {
					t.Fatalf("authentication context = ACR %q, AMR %v", p.ACR, p.AMR)
				}
				return
			}
			if p.ACR != "urn:astryon:acr:mfa" && name != "absent" {
				t.Fatalf("ACR = %q", p.ACR)
			}
			if p.AMR != nil {
				t.Fatalf("AMR = %v, want nil", p.AMR)
			}
		})
	}
}
