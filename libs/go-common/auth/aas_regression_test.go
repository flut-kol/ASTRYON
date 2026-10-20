package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// Found by the live Keycloak run (tests/aas-live): real AAS access tokens carry typ "Bearer".
func TestDefaultTokenTypeAcceptsKeycloakBearerButNotIDOrDPoP(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	server := oidcServer(t, &key.PublicKey, "one")
	defer server.Close()
	now := time.Unix(1_700_000_000, 0)
	v, err := NewVerifier(context.Background(), Config{BaseURL: server.URL, Audience: "astryon-gateway", Environment: "test", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	for typ, ok := range map[string]bool{"Bearer": true, "at+jwt": true, "ID": false, "DPoP": false, "": false} {
		c := validClaims(server.URL+"/issuer", now)
		c["typ"] = typ
		delete(c, "nbf") // Keycloak omits nbf
		c["aud"] = "astryon-gateway"
		_, err := v.Verify(context.Background(), signedRS256(t, key, "one", c))
		if (err == nil) != ok {
			t.Errorf("typ %q: err=%v, want accepted=%v", typ, err, ok)
		}
	}
	// explicit setting stays strict
	strict, _ := NewVerifier(context.Background(), Config{BaseURL: server.URL, Audience: "astryon-gateway", Environment: "test", TokenType: "at+jwt", Now: func() time.Time { return now }})
	c := validClaims(server.URL+"/issuer", now)
	c["typ"] = "Bearer"
	if _, err := strict.Verify(context.Background(), signedRS256(t, key, "one", c)); err == nil {
		t.Error("explicit TokenType at+jwt accepted Bearer")
	}
}

func TestUnknownKidRefreshIsRateLimitedAndOutageServesStaleKey(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	inner := oidcServer(t, &key.PublicKey, "one")
	defer inner.Close()
	var jwks atomic.Int64
	var down atomic.Bool
	now := time.Unix(1_700_000_000, 0)
	// Count JWKS fetches on a transport instead.
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if down.Load() {
			return nil, context.DeadlineExceeded
		}
		if r.URL.Path == "/oauth2/jwks" {
			jwks.Add(1)
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	clock := now
	v, err := NewVerifier(context.Background(), Config{BaseURL: inner.URL, Audience: "astryon-gateway", Environment: "test",
		JWKSCacheTTL: time.Minute, HTTPClient: client, Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	claims := validClaims(inner.URL+"/issuer", now)
	claims["aud"] = "astryon-gateway"
	good := signedRS256(t, key, "one", claims)
	before := jwks.Load()
	for i := 0; i < 20; i++ {
		if _, err := v.Verify(context.Background(), signedRS256(t, key, "forged", claims)); err == nil {
			t.Fatal("unknown kid accepted")
		}
	}
	if got := jwks.Load() - before; got > 1 {
		t.Fatalf("20 unknown-kid tokens caused %d JWKS fetches, want <= 1", got)
	}
	clock = now.Add(5 * time.Minute) // cache stale, token (1h) still valid
	down.Store(true)
	if _, err := v.Verify(context.Background(), good); err != nil {
		t.Fatalf("stale cached key must verify during an outage: %v", err)
	}
	if _, err := v.Verify(context.Background(), signedRS256(t, key, "forged", claims)); err == nil {
		t.Fatal("unknown kid accepted during outage")
	}
	clock = now.Add(2 * time.Hour) // beyond token exp and beyond max stale
	if _, err := v.Verify(context.Background(), good); err == nil {
		t.Fatal("expired token accepted during outage")
	}
	down.Store(false)
	clock = now.Add(10 * time.Minute)
	if _, err := v.Verify(context.Background(), good); err != nil {
		t.Fatalf("recovery after outage: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
