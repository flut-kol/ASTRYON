package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func dpopProof(t *testing.T, key *ecdsa.PrivateKey, claims map[string]any, hdr map[string]any) string {
	t.Helper()
	header := map[string]any{"typ": "dpop+jwt", "alg": "ES256", "jwk": map[string]any{"kty": "EC", "crv": "P-256",
		"x": b64u(key.X.FillBytes(make([]byte, 32))), "y": b64u(key.Y.FillBytes(make([]byte, 32)))}}
	for k, v := range hdr {
		header[k] = v
	}
	enc := func(v any) string { b, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(b) }
	signed := enc(header) + "." + enc(claims)
	d := sha256.Sum256([]byte(signed))
	r, s, err := ecdsa.Sign(rand.Reader, key, d[:])
	if err != nil {
		t.Fatal(err)
	}
	sig := append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	return signed + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestDPoP(t *testing.T) {
	rk, _ := rsa.GenerateKey(rand.Reader, 2048)
	server := oidcServer(t, &rk.PublicKey, "one")
	defer server.Close()
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	newV := func(require bool) *Verifier {
		v, err := NewVerifier(context.Background(), Config{BaseURL: server.URL, Issuer: server.URL + "/issuer", Audience: "astryon-gateway",
			Environment: "test", AllowedAZP: []string{"source-a"}, Now: clock, PublicBaseURL: "https://API.example.com:443",
			ReplayStore: &MemoryReplayStore{Now: clock}, RequireDPoP: require})
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	v := newV(false)
	pk, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	jkt := thumbprint(&pk.PublicKey)
	bound := validClaims(server.URL+"/issuer", now)
	bound["typ"], bound["cnf"] = "DPoP", map[string]any{"jkt": jkt}
	token := signedRS256(t, rk, "one", bound)
	sum := sha256.Sum256([]byte(token))
	good := func() map[string]any {
		return map[string]any{"htm": "POST", "htu": "https://api.example.com/v1/events?x=1", "ath": b64u(sum[:]),
			"iat": now.Unix(), "jti": "j-" + time.Now().String()}
	}
	req := func(scheme, tok string, proofs ...string) *http.Request {
		r := httptest.NewRequest("POST", "/v1/events", nil)
		r.Header.Set("Authorization", scheme+" "+tok)
		for _, p := range proofs {
			r.Header.Add("DPoP", p)
		}
		return r
	}
	ok := dpopProof(t, pk, good(), nil)
	p, err := v.Authenticate(req("DPoP", token, ok))
	if err != nil || p.Confirmation != jkt {
		t.Fatalf("valid: %v %+v", err, p)
	}
	if _, err := v.Authenticate(req("DPoP", token, ok)); err == nil {
		t.Fatal("replayed proof accepted")
	}
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	with := func(f func(map[string]any)) string { c := good(); f(c); return dpopProof(t, pk, c, nil) }
	cases := map[string]string{
		"htm":       with(func(c map[string]any) { c["htm"] = "GET" }),
		"htu":       with(func(c map[string]any) { c["htu"] = "https://evil.example.com/v1/events" }),
		"ath":       with(func(c map[string]any) { c["ath"] = "AAAA" }),
		"old iat":   with(func(c map[string]any) { c["iat"] = now.Add(-2 * time.Minute).Unix() }),
		"future":    with(func(c map[string]any) { c["iat"] = now.Add(time.Minute).Unix() }),
		"no jti":    with(func(c map[string]any) { delete(c, "jti") }),
		"wrong key": dpopProof(t, other, good(), nil),
		"typ":       dpopProof(t, pk, good(), map[string]any{"typ": "JWT"}),
		"private":   dpopProof(t, pk, good(), map[string]any{"jwk": map[string]any{"kty": "EC", "crv": "P-256", "d": "x"}}),
	}
	for name, proof := range cases {
		if _, err := v.Authenticate(req("DPoP", token, proof)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := v.Authenticate(req("DPoP", token)); err == nil {
		t.Error("missing proof accepted")
	}
	if _, err := v.Authenticate(req("DPoP", token, dpopProof(t, pk, good(), nil), dpopProof(t, pk, good(), nil))); err == nil {
		t.Error("two proofs accepted")
	}
	if _, err := v.Authenticate(req("Bearer", token)); err == nil {
		t.Error("bound token under Bearer accepted (downgrade)")
	}
	unbound := signedRS256(t, rk, "one", validClaims(server.URL+"/issuer", now))
	if _, err := v.Authenticate(req("DPoP", unbound, ok)); err == nil {
		t.Error("unbound token under DPoP accepted")
	}
	if _, err := v.Authenticate(req("Bearer", unbound)); err != nil {
		t.Errorf("plain bearer broke: %v", err)
	}
	if _, err := newV(true).Authenticate(req("Bearer", unbound)); err == nil {
		t.Error("RequireDPoP accepted Bearer")
	}
}
