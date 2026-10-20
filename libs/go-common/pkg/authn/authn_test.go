package authn

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

const (
	testAudience = "astryon-gateway"
	testKid      = "test-key-1"
)

// testAAS is a fake AAS (Keycloak-backed, per
// docs/architecture/authentication.md) server: it serves a discovery
// document and a JWKS over httptest, so tests never hit the network or read
// any real key material.
type testAAS struct {
	srv       *httptest.Server
	issuer    string
	jwksSet   jwk.Set
	jwksCalls int
	signer    interface{} // *rsa.PrivateKey or *ecdsa.PrivateKey
	alg       jwa.SignatureAlgorithm
}

// newTestAAS spins up a fake AAS signing with alg (jwa.RS256 or jwa.ES256).
// The keypair is generated fresh in-process for each test; no real AAS key
// material is ever read.
func newTestAAS(t *testing.T, alg jwa.SignatureAlgorithm) *testAAS {
	t.Helper()

	var (
		signer interface{}
		pubRaw interface{}
	)
	switch alg {
	case jwa.ES256:
		priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate ecdsa key: %v", err)
		}
		signer = priv
		pubRaw = priv.PublicKey
	default:
		priv, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("generate rsa key: %v", err)
		}
		signer = priv
		pubRaw = priv.PublicKey
	}

	pub, err := jwk.FromRaw(pubRaw)
	if err != nil {
		t.Fatalf("jwk from public key: %v", err)
	}
	if err := pub.Set(jwk.KeyIDKey, testKid); err != nil {
		t.Fatalf("set kid: %v", err)
	}
	if err := pub.Set(jwk.AlgorithmKey, alg); err != nil {
		t.Fatalf("set alg: %v", err)
	}

	set := jwk.NewSet()
	if err := set.AddKey(pub); err != nil {
		t.Fatalf("add key to set: %v", err)
	}

	ta := &testAAS{jwksSet: set, signer: signer, alg: alg}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		doc := map[string]string{
			"issuer":   ta.issuer,
			"jwks_uri": ta.srv.URL + "/jwks",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		ta.jwksCalls++
		w.Header().Set("Content-Type", "application/json")
		buf, err := json.Marshal(ta.jwksSet)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(buf)
	})

	ta.srv = httptest.NewServer(mux)
	ta.issuer = ta.srv.URL // issuer == base URL is fine for a fake AAS

	return ta
}

func (ta *testAAS) Close() { ta.srv.Close() }

func (ta *testAAS) BaseURL() string { return ta.srv.URL }

// tokenOpts customizes a test JWT away from a baseline "valid" token. The
// baseline models a real Keycloak machine-to-gateway access token per
// docs/architecture/authentication.md: tenant_id, scope, payload typ=
// "Bearer", env and azp claims all set.
type tokenOpts struct {
	issuer     string
	audience   string
	subject    string
	tenantID   string
	clientID   string
	jti        string
	scope      string
	payloadTyp string
	env        string
	azp        string
	headerTyp  string // override the JOSE header "typ"; "" keeps jwx's default
	iat        time.Time
	nbf        time.Time
	exp        time.Time
	kid        string // kid to put in the JWS header

	omitTenantID   bool
	omitScope      bool
	omitPayloadTyp bool
	omitEnv        bool
	omitAZP        bool

	alg      jwa.SignatureAlgorithm // defaults to ta.alg
	signWith interface{}            // key to sign with; defaults to ta.signer
	tamper   bool                   // flip a byte in the signature
}

func (ta *testAAS) newToken(t *testing.T, o tokenOpts) string {
	t.Helper()

	now := time.Now()
	builder := jwt.NewBuilder()

	iss := o.issuer
	if iss == "" {
		iss = ta.issuer
	}
	aud := o.audience
	if aud == "" {
		aud = testAudience
	}
	sub := o.subject
	if sub == "" {
		sub = "user-123"
	}
	jti := o.jti
	if jti == "" {
		jti = "jti-abc-123"
	}
	iat := o.iat
	if iat.IsZero() {
		iat = now
	}
	nbf := o.nbf
	if nbf.IsZero() {
		nbf = now.Add(-time.Minute)
	}
	exp := o.exp
	if exp.IsZero() {
		exp = now.Add(5 * time.Minute)
	}

	builder.
		Issuer(iss).
		Audience([]string{aud}).
		Subject(sub).
		JwtID(jti).
		IssuedAt(iat).
		NotBefore(nbf).
		Expiration(exp)

	if !o.omitTenantID {
		tenantID := o.tenantID
		if tenantID == "" {
			tenantID = "tenant-42"
		}
		builder.Claim("tenant_id", tenantID)
	}
	if o.clientID != "" {
		builder.Claim("client_id", o.clientID)
	}
	if !o.omitScope {
		scope := o.scope
		if scope == "" {
			scope = "astryon:events:write astryon:events:read"
		}
		builder.Claim("scope", scope)
	}
	if !o.omitPayloadTyp {
		payloadTyp := o.payloadTyp
		if payloadTyp == "" {
			payloadTyp = "Bearer"
		}
		builder.Claim("typ", payloadTyp)
	}
	if !o.omitEnv {
		env := o.env
		if env == "" {
			env = "dev"
		}
		builder.Claim("env", env)
	}
	if !o.omitAZP {
		azp := o.azp
		if azp == "" {
			azp = "gateway-client"
		}
		builder.Claim("azp", azp)
	}

	tok, err := builder.Build()
	if err != nil {
		t.Fatalf("build token: %v", err)
	}

	key := o.signWith
	if key == nil {
		key = ta.signer
	}
	alg := o.alg
	if alg == "" {
		alg = ta.alg
	}

	kid := o.kid
	if kid == "" {
		kid = testKid
	}
	privJWK, err := jwk.FromRaw(key)
	if err != nil {
		t.Fatalf("jwk from private key: %v", err)
	}
	if err := privJWK.Set(jwk.KeyIDKey, kid); err != nil {
		t.Fatalf("set kid: %v", err)
	}
	if err := privJWK.Set(jwk.AlgorithmKey, alg); err != nil {
		t.Fatalf("set alg: %v", err)
	}

	signOpts := []jwt.SignOption{}
	if o.headerTyp != "" {
		hdrs := jws.NewHeaders()
		_ = hdrs.Set(jws.TypeKey, o.headerTyp)
		signOpts = append(signOpts, jwt.WithKey(alg, privJWK, jws.WithProtectedHeaders(hdrs)))
	} else {
		signOpts = append(signOpts, jwt.WithKey(alg, privJWK))
	}

	signed, err := jwt.Sign(tok, signOpts...)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	if o.tamper {
		// Flip a byte near the end of the signature segment to break it
		// without otherwise altering the token's structure.
		b := []byte(string(signed))
		if len(b) > 0 {
			b[len(b)-1] ^= 0xFF
		}
		return string(b)
	}

	return string(signed)
}

func newVerifierFor(t *testing.T, ta *testAAS) *Verifier {
	t.Helper()
	v, err := New(Config{
		BaseURL:  ta.BaseURL(),
		Audience: testAudience,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return v
}

func TestVerify_ValidTokenAccepted(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{
		tenantID: "tenant-42",
		clientID: "gateway-client",
		jti:      "jti-1",
		scope:    "astryon:events:write astryon:events:read",
		azp:      "gateway-client",
	})

	claims, err := v.Verify(context.Background(), raw)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if claims.TenantID != "tenant-42" {
		t.Errorf("TenantID = %q, want tenant-42", claims.TenantID)
	}
	if claims.ClientID != "gateway-client" {
		t.Errorf("ClientID = %q, want gateway-client", claims.ClientID)
	}
	if claims.Sub != "user-123" {
		t.Errorf("Sub = %q, want user-123", claims.Sub)
	}
	if claims.JTI != "jti-1" {
		t.Errorf("JTI = %q, want jti-1", claims.JTI)
	}
	if claims.Env != "dev" {
		t.Errorf("Env = %q, want dev", claims.Env)
	}
	if claims.AZP != "gateway-client" {
		t.Errorf("AZP = %q, want gateway-client", claims.AZP)
	}
	if !claims.HasScope("astryon:events:write") || !claims.HasScope("astryon:events:read") {
		t.Errorf("Scopes = %v, missing expected scopes", claims.Scopes)
	}
}

// TestVerify_ES256TokenAccepted covers the real AAS deployment's actual
// signing algorithm (ECDSA P-256 / ES256, per the Keycloak realm's
// astranex_es256 signer): the verifier is algorithm-agnostic (it derives
// the algorithm from the JWKS key's kty/alg via the jwx library, never
// hardcoding RS256), so this exercises that path end-to-end with a
// freshly-generated in-test ECDSA keypair. No real AAS key material is
// read.
func TestVerify_ES256TokenAccepted(t *testing.T) {
	ta := newTestAAS(t, jwa.ES256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{
		tenantID: "tenant-es256",
		jti:      "jti-es256",
	})

	claims, err := v.Verify(context.Background(), raw)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if claims.TenantID != "tenant-es256" {
		t.Errorf("TenantID = %q, want tenant-es256", claims.TenantID)
	}
}

func TestVerify_ScpArrayFallback(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	now := time.Now()
	tok, err := jwt.NewBuilder().
		Issuer(ta.issuer).
		Audience([]string{testAudience}).
		Subject("user-1").
		JwtID("jti-scp").
		IssuedAt(now).
		NotBefore(now.Add(-time.Minute)).
		Expiration(now.Add(5*time.Minute)).
		Claim("tenant_id", "tenant-9").
		Claim("scp", []string{"astryon:jobs:read", "astryon:jobs:write"}).
		Claim("typ", "Bearer").
		Claim("env", "dev").
		Claim("azp", "gateway-client").
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	privJWK, err := jwk.FromRaw(ta.signer)
	if err != nil {
		t.Fatalf("jwk: %v", err)
	}
	_ = privJWK.Set(jwk.KeyIDKey, testKid)
	_ = privJWK.Set(jwk.AlgorithmKey, jwa.RS256)
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256, privJWK))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	claims, err := v.Verify(context.Background(), string(signed))
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if !claims.HasScope("astryon:jobs:read") || !claims.HasScope("astryon:jobs:write") {
		t.Errorf("Scopes = %v, missing expected scp-derived scopes", claims.Scopes)
	}
}

func TestVerify_WrongIssuerRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{issuer: "https://not-the-real-aas.example"})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_WrongAudienceRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{audience: "some-other-service"})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_ExpiredTokenRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	now := time.Now()
	raw := ta.newToken(t, tokenOpts{
		iat: now.Add(-time.Hour),
		nbf: now.Add(-time.Hour),
		exp: now.Add(-10 * time.Minute),
	})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_NotYetValidTokenRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	now := time.Now()
	raw := ta.newToken(t, tokenOpts{
		iat: now,
		nbf: now.Add(10 * time.Minute),
		exp: now.Add(20 * time.Minute),
	})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_UnknownKidRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	// Sign with a different key than the one published in the JWKS, under
	// a kid that is also absent from the JWKS.
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	raw := ta.newToken(t, tokenOpts{kid: "unknown-kid", signWith: otherKey})

	_, err = v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
	// Unknown-kid should have triggered a JWKS refresh-on-miss retry, so
	// the JWKS endpoint should have been hit at least twice (initial +
	// forced refresh).
	if ta.jwksCalls < 2 {
		t.Errorf("jwksCalls = %d, want >= 2 (expected refresh-on-kid-miss)", ta.jwksCalls)
	}
}

func TestVerify_TamperedSignatureRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{tamper: true})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_MissingTenantIDRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{omitTenantID: true})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_MissingScopeRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{omitScope: true})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_EmptyTokenRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	_, err := v.Verify(context.Background(), "")
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

// --- typ (header + payload), env, azp coverage ---

func TestVerify_HeaderTypAtPlusJWTAccepted(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{headerTyp: "at+jwt"})

	if _, err := v.Verify(context.Background(), raw); err != nil {
		t.Fatalf("expected success with at+jwt header typ, got error: %v", err)
	}
}

func TestVerify_HeaderTypUnexpectedRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{headerTyp: "some-other-type"})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_MissingPayloadTypRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{omitPayloadTyp: true})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_WrongPayloadTypRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{payloadTyp: "ID"}) // e.g. an ID token, not an access token

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_MissingEnvRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{omitEnv: true})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_ExpectedEnvMismatchRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()

	v, err := New(Config{
		BaseURL:     ta.BaseURL(),
		Audience:    testAudience,
		ExpectedEnv: "prod",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	raw := ta.newToken(t, tokenOpts{env: "dev"})

	_, err = v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_ExpectedEnvMatchAccepted(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()

	v, err := New(Config{
		BaseURL:     ta.BaseURL(),
		Audience:    testAudience,
		ExpectedEnv: "prod",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	raw := ta.newToken(t, tokenOpts{env: "prod"})

	if _, err := v.Verify(context.Background(), raw); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}

func TestVerify_MissingAZPRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{omitAZP: true})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_AZPClientIDMismatchRejected(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{azp: "client-a", clientID: "client-b"})

	_, err := v.Verify(context.Background(), raw)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_AZPClientIDMatchAccepted(t *testing.T) {
	ta := newTestAAS(t, jwa.RS256)
	defer ta.Close()
	v := newVerifierFor(t, ta)

	raw := ta.newToken(t, tokenOpts{azp: "client-a", clientID: "client-a"})

	claims, err := v.Verify(context.Background(), raw)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if claims.AZP != "client-a" || claims.ClientID != "client-a" {
		t.Errorf("AZP/ClientID = %q/%q, want client-a/client-a", claims.AZP, claims.ClientID)
	}
}

func TestNew_RequiresBaseURLAndAudience(t *testing.T) {
	if _, err := New(Config{Audience: "aud"}); !errors.Is(err, ErrConfig) {
		t.Errorf("missing BaseURL: expected ErrConfig, got %v", err)
	}
	if _, err := New(Config{BaseURL: "https://aas.example"}); !errors.Is(err, ErrConfig) {
		t.Errorf("missing Audience: expected ErrConfig, got %v", err)
	}
}

func TestVerify_DiscoveryUnavailableAndNoCache(t *testing.T) {
	v, err := New(Config{
		BaseURL:  "http://127.0.0.1:0", // nothing listens here
		Audience: testAudience,
		HTTPClient: &http.Client{
			Timeout: 200 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = v.Verify(context.Background(), "irrelevant.token.value")
	if !errors.Is(err, ErrDiscoveryUnavailable) {
		t.Fatalf("expected ErrDiscoveryUnavailable, got %v", err)
	}
}
