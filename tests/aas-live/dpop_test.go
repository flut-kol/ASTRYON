package aaslive

// Live DPoP (L2) proof against a real AAS: a client with dpop.bound.access.tokens=true gets a
// typ=DPoP token with cnf.jkt, and libs/go-common/auth accepts it only together with a matching proof.
// Skipped unless AAS_LIVE_BASE_URL and admin credentials are set (see README).

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/astryon/go-common/auth"
)

func (l *live) dpopProof(key *ecdsa.PrivateKey, method, htu, accessToken string) string {
	jwk := map[string]string{"kty": "EC", "crv": "P-256", "x": b64(key.X.FillBytes(make([]byte, 32))), "y": b64(key.Y.FillBytes(make([]byte, 32)))}
	claims := map[string]any{"htm": method, "htu": htu, "iat": time.Now().Unix(), "jti": b64(randBytes(12))}
	if accessToken != "" {
		sum := sha256.Sum256([]byte(accessToken))
		claims["ath"] = b64(sum[:])
	}
	h, _ := json.Marshal(map[string]any{"typ": "dpop+jwt", "alg": "ES256", "jwk": jwk})
	p, _ := json.Marshal(claims)
	in := b64(h) + "." + b64(p)
	d := sha256.Sum256([]byte(in))
	r, s, _ := ecdsa.Sign(rand.Reader, key, d[:])
	return in + "." + b64(append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...))
}

// dpopToken requests a DPoP-bound token; the proof htu is the token endpoint.
func (l *live) dpopToken(c *client, key *ecdsa.PrivateKey) (string, int, string) {
	endpoint := l.base + "/protocol/openid-connect/token"
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {c.ID}, "client_assertion_type": {assertionTy}, "client_assertion": {l.assertion(c)}}
	code, b, _, err := l.doSoft("POST", endpoint, map[string]string{"Content-Type": "application/x-www-form-urlencoded", "DPoP": l.dpopProof(key, "POST", endpoint, "")}, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err.Error()
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.Unmarshal(b, &tok)
	return tok.AccessToken, code, string(b)
}

func TestLiveDPoP(t *testing.T) {
	l := newLive(t)
	c := l.newClient(suffix(), tenantA, 0)
	// enable DPoP binding on the throw-away client
	_, raw, _ := l.adminJSON("GET", "/clients/"+c.uuid, nil)
	var rep map[string]any
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	rep["attributes"].(map[string]any)["dpop.bound.access.tokens"] = "true"
	l.mustAdmin("PUT", "/clients/"+c.uuid, rep, 204)

	dkey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tok, code, body := l.dpopToken(c, dkey)
	if code != 200 {
		t.Fatalf("dpop token: %d %s", code, body)
	}
	if hdr := decodePart(t, tok, 1); hdr["typ"] != "DPoP" || hdr["cnf"] == nil {
		t.Fatalf("expected typ=DPoP with cnf, got typ=%v cnf=%v", hdr["typ"], hdr["cnf"])
	}

	const public = "https://api.example.test"
	newV := func(require bool) *auth.Verifier {
		return l.verifier(func(cfg *auth.Config) {
			cfg.AllowedAZP = []string{c.ID}
			cfg.PublicBaseURL, cfg.ReplayStore, cfg.RequireDPoP = public, &auth.MemoryReplayStore{}, require
		})
	}
	v := newV(false)
	req := func(scheme, token, proof string) *http.Request {
		r := httptest.NewRequest("POST", "/v1/events", nil)
		r.Header.Set("Authorization", scheme+" "+token)
		if proof != "" {
			r.Header.Set("DPoP", proof)
		}
		return r
	}
	good := l.dpopProof(dkey, "POST", public+"/v1/events", tok)
	p, err := v.Authenticate(req("DPoP", tok, good))
	if err != nil || p.TenantID != tenantA || p.Confirmation == "" {
		t.Fatalf("valid DPoP request: %v %+v", err, p)
	}
	if _, err := v.Authenticate(req("DPoP", tok, good)); err == nil {
		t.Error("replayed proof accepted")
	}
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	for name, r := range map[string]*http.Request{
		"stolen token, other key": req("DPoP", tok, l.dpopProof(other, "POST", public+"/v1/events", tok)),
		"wrong htu":               req("DPoP", tok, l.dpopProof(dkey, "POST", public+"/v1/other", tok)),
		"wrong htm":               req("DPoP", tok, l.dpopProof(dkey, "GET", public+"/v1/events", tok)),
		"wrong ath":               req("DPoP", tok, l.dpopProof(dkey, "POST", public+"/v1/events", tok+"x")),
		"bearer downgrade":        req("Bearer", tok, ""),
		"no proof":                req("DPoP", tok, ""),
	} {
		if _, err := v.Authenticate(r); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	// replay across two verifier instances needs a shared store (AUTH_IT_REDIS_URL in libs/go-common); here: independent memory stores.
	if _, err := newV(true).Authenticate(req("Bearer", tok, "")); err == nil {
		t.Error("RequireDPoP accepted Bearer")
	}
}
