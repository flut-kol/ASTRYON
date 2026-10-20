package gateway

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/astryon/go-common/pkg/authn"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

const (
	testAASAudience = "astryon-gateway"
	testAASKid      = "test-key-1"
)

// fakeAAS is a minimal fake AAS (per docs/architecture/authentication.md):
// it serves an OIDC discovery document and JWKS over httptest, built only
// from libs/go-common/pkg/authn's public API plus the same jwx libraries
// that package verifies against. No real AAS/network access is used.
type fakeAAS struct {
	srv  *httptest.Server
	priv *rsa.PrivateKey
	set  jwk.Set
}

func newFakeAAS(t *testing.T) *fakeAAS {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pub, err := jwk.FromRaw(priv.PublicKey)
	if err != nil {
		t.Fatalf("jwk from public key: %v", err)
	}
	if err := pub.Set(jwk.KeyIDKey, testAASKid); err != nil {
		t.Fatalf("set kid: %v", err)
	}
	if err := pub.Set(jwk.AlgorithmKey, jwa.RS256); err != nil {
		t.Fatalf("set alg: %v", err)
	}
	set := jwk.NewSet()
	if err := set.AddKey(pub); err != nil {
		t.Fatalf("add key: %v", err)
	}

	fa := &fakeAAS{priv: priv, set: set}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		doc := map[string]string{"issuer": fa.srv.URL, "jwks_uri": fa.srv.URL + "/jwks"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		buf, err := json.Marshal(fa.set)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buf)
	})
	fa.srv = httptest.NewServer(mux)
	return fa
}

func (fa *fakeAAS) Close() { fa.srv.Close() }

func (fa *fakeAAS) BaseURL() string { return fa.srv.URL }

// validToken returns a signed JWT satisfying every claim that
// libs/go-common/pkg/authn's Verifier requires: tenant_id, sub, jti,
// scope, payload typ=Bearer, env and azp.
func (fa *fakeAAS) validToken(t *testing.T, tenantID, scopes string) string {
	t.Helper()
	now := time.Now()
	tok, err := jwt.NewBuilder().
		Issuer(fa.srv.URL).
		Audience([]string{testAASAudience}).
		Subject("user-123").
		JwtID("jti-abc-123").
		IssuedAt(now).
		NotBefore(now.Add(-time.Minute)).
		Expiration(now.Add(5*time.Minute)).
		Claim("tenant_id", tenantID).
		Claim("scope", scopes).
		Claim("typ", "Bearer").
		Claim("env", "dev").
		Claim("azp", "gateway-client").
		Build()
	if err != nil {
		t.Fatalf("build token: %v", err)
	}

	privJWK, err := jwk.FromRaw(fa.priv)
	if err != nil {
		t.Fatalf("jwk from private key: %v", err)
	}
	if err := privJWK.Set(jwk.KeyIDKey, testAASKid); err != nil {
		t.Fatalf("set kid: %v", err)
	}
	if err := privJWK.Set(jwk.AlgorithmKey, jwa.RS256); err != nil {
		t.Fatalf("set alg: %v", err)
	}

	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256, privJWK))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return string(signed)
}

func newTestVerifier(t *testing.T, baseURL string) *authn.Verifier {
	t.Helper()
	v, err := authn.New(authn.Config{BaseURL: baseURL, Audience: testAASAudience})
	if err != nil {
		t.Fatalf("authn.New: %v", err)
	}
	return v
}

func TestAASJWTAuthenticatorMissingOrMalformedAuth(t *testing.T) {
	v := newTestVerifier(t, "https://example.invalid")
	auth := NewAASJWTAuthenticator(v)

	noHeader := httptest.NewRequest("POST", "/", nil)
	if _, err := auth.Authenticate(noHeader); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("missing Authorization header must reject")
	}

	malformed := httptest.NewRequest("POST", "/", nil)
	malformed.Header.Set("Authorization", "Token abc")
	if _, err := auth.Authenticate(malformed); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("non-Bearer Authorization header must reject")
	}
}

func TestAASJWTAuthenticatorInvalidTokenRejected(t *testing.T) {
	fa := newFakeAAS(t)
	defer fa.Close()
	auth := NewAASJWTAuthenticator(newTestVerifier(t, fa.BaseURL()))

	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Authorization", "Bearer not-a-real-jwt")
	if _, err := auth.Authenticate(r); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("garbage token must reject")
	}
}

func TestAASJWTAuthenticatorValidTokenRoundTrips(t *testing.T) {
	fa := newFakeAAS(t)
	defer fa.Close()
	auth := NewAASJWTAuthenticator(newTestVerifier(t, fa.BaseURL()))

	tok := fa.validToken(t, tenantA, "astryon:events:write astryon:events:read")
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Authorization", "Bearer "+tok)

	p, err := auth.Authenticate(r)
	if err != nil {
		t.Fatalf("valid token should be accepted: %v", err)
	}
	if p.TenantID != tenantA {
		t.Fatalf("got tenant %q, want %q", p.TenantID, tenantA)
	}
	if !p.HasScope(ScopeEventsWrite) {
		t.Fatalf("expected scope %q in %v", ScopeEventsWrite, p.Scopes)
	}
}

func TestAASJWTAuthenticatorNilVerifierFailsClosed(t *testing.T) {
	var auth AASJWTAuthenticator // zero value: nil verifier
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Authorization", "Bearer x")
	if _, err := auth.Authenticate(r); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("nil-verifier authenticator must fail closed")
	}
}
