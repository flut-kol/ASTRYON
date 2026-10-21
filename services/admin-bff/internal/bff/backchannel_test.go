package bff

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

type signer struct {
	kid, alg string
	sign     func(in string) []byte
}

func (f *fakeAAS) addRSA(t *testing.T, kid string) signer {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.jwks = append(f.jwks, map[string]string{"kty": "RSA", "use": "sig", "kid": kid, "alg": "RS256", "n": b64u(k.N.Bytes()), "e": b64u(big.NewInt(int64(k.E)).Bytes())})
	f.mu.Unlock()
	return signer{kid, "RS256", func(in string) []byte {
		h := sha256.Sum256([]byte(in))
		sig, _ := rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, h[:])
		return sig
	}}
}

func (f *fakeAAS) addEC(t *testing.T, kid string) signer {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	f.mu.Lock()
	f.jwks = append(f.jwks, map[string]string{"kty": "EC", "crv": "P-256", "use": "sig", "kid": kid, "x": b64u(k.X.FillBytes(make([]byte, 32))), "y": b64u(k.Y.FillBytes(make([]byte, 32)))})
	f.mu.Unlock()
	return signer{kid, "ES256", func(in string) []byte {
		h := sha256.Sum256([]byte(in))
		r, s, _ := ecdsa.Sign(rand.Reader, k, h[:])
		return append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	}}
}

func (f *fakeAAS) logoutToken(s signer, mut func(map[string]any)) string {
	cl := map[string]any{"iss": f.URL + "/realm", "aud": "astryon-portal", "iat": time.Now().Unix(), "jti": randString(8), "sid": "sid-1",
		"events": map[string]any{backchannelEvent: map[string]any{}}}
	if mut != nil {
		mut(cl)
	}
	h, _ := json.Marshal(map[string]string{"alg": s.alg, "kid": s.kid, "typ": "logout+jwt"})
	c, _ := json.Marshal(cl)
	in := b64u(h) + "." + b64u(c)
	return in + "." + b64u(s.sign(in))
}

func (e *env) postLogout(t *testing.T, tok string) int {
	t.Helper()
	res, err := http.PostForm(e.bff.URL+"/auth/backchannel-logout", url.Values{"logout_token": {tok}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	return res.StatusCode
}

func (e *env) loggedIn(t *testing.T) bool {
	t.Helper()
	_, body := e.do(t, "GET", "/auth/me", nil)
	return strings.Contains(body, `"authenticated":true`)
}

func TestBackchannelLogout(t *testing.T) {
	for _, alg := range []string{"RS256", "ES256"} {
		t.Run(alg+" sid ends the session", func(t *testing.T) {
			e := setup(t, nil)
			var s signer
			if alg == "RS256" {
				s = e.aas.addRSA(t, "k1")
			} else {
				s = e.aas.addEC(t, "k1")
			}
			e.login(t)
			if !e.loggedIn(t) {
				t.Fatal("not logged in")
			}
			if c := e.postLogout(t, e.aas.logoutToken(s, nil)); c != 200 {
				t.Fatalf("logout: %d", c)
			}
			if e.loggedIn(t) {
				t.Fatal("session survived a valid back-channel logout")
			}
		})
	}
}

func TestBackchannelLogoutRejections(t *testing.T) {
	e := setup(t, nil)
	s := e.aas.addRSA(t, "k1")
	other := signer{"k1", "RS256", func(in string) []byte { return e.aas.addRSA(t, "unused").sign(in) }} // right kid, wrong key
	e.login(t)
	cases := map[string]string{
		"wrong key":      e.aas.logoutToken(other, nil),
		"unknown kid":    e.aas.logoutToken(signer{"nope", s.alg, s.sign}, nil),
		"wrong issuer":   e.aas.logoutToken(s, func(c map[string]any) { c["iss"] = "https://evil.example" }),
		"wrong audience": e.aas.logoutToken(s, func(c map[string]any) { c["aud"] = "someone-else" }),
		"no event":       e.aas.logoutToken(s, func(c map[string]any) { delete(c, "events") }),
		"nonce present":  e.aas.logoutToken(s, func(c map[string]any) { c["nonce"] = "x" }),
		"no sub or sid":  e.aas.logoutToken(s, func(c map[string]any) { delete(c, "sid") }),
		"stale iat":      e.aas.logoutToken(s, func(c map[string]any) { c["iat"] = time.Now().Add(-time.Hour).Unix() }),
		"future iat":     e.aas.logoutToken(s, func(c map[string]any) { c["iat"] = time.Now().Add(time.Hour).Unix() }),
		"expired":        e.aas.logoutToken(s, func(c map[string]any) { c["exp"] = time.Now().Add(-time.Minute).Unix() }),
		"no jti":         e.aas.logoutToken(s, func(c map[string]any) { delete(c, "jti") }),
		"garbage":        "not-a-jwt",
		"alg none":       b64u([]byte(`{"alg":"none","kid":"k1"}`)) + "." + b64u([]byte(`{"iss":"x"}`)) + ".",
	}
	for name, tok := range cases {
		if c := e.postLogout(t, tok); c != 400 {
			t.Errorf("%s: status %d, want 400", name, c)
		}
		if !e.loggedIn(t) {
			t.Fatalf("%s: session was ended by a rejected token", name)
		}
	}
	// a token for another session id leaves this session alone
	if c := e.postLogout(t, e.aas.logoutToken(s, func(c map[string]any) { c["sid"] = "sid-other" })); c != 200 || !e.loggedIn(t) {
		t.Fatalf("foreign sid: %d loggedIn=%v", c, e.loggedIn(t))
	}
	// replay of an accepted token is refused
	tok := e.aas.logoutToken(s, func(c map[string]any) { c["sid"] = "sid-other2" })
	if e.postLogout(t, tok) != 200 || e.postLogout(t, tok) != 400 {
		t.Fatal("jti replay not refused")
	}
	// missing parameter
	res, _ := http.Post(e.bff.URL+"/auth/backchannel-logout", "application/x-www-form-urlencoded", strings.NewReader(""))
	if res.StatusCode != 400 {
		t.Fatalf("empty body: %d", res.StatusCode)
	}
}

func TestBackchannelSubOnlyEndsAllSessionsOfSubject(t *testing.T) {
	e := setup(t, nil)
	s := e.aas.addEC(t, "k1")
	e.login(t)
	if c := e.postLogout(t, e.aas.logoutToken(s, func(c map[string]any) { delete(c, "sid"); c["sub"] = "user-1" })); c != 200 || e.loggedIn(t) {
		t.Fatalf("sub logout: %d loggedIn=%v", c, e.loggedIn(t))
	}
}

func TestBackchannelUnknownKidRefetchIsRateLimited(t *testing.T) {
	e := setup(t, nil)
	s := e.aas.addEC(t, "k1")
	bad := signer{"nope", s.alg, s.sign}
	for i := 0; i < 5; i++ {
		e.postLogout(t, e.aas.logoutToken(bad, nil))
	}
	if n := e.aas.jwksHits.Load(); n != 1 {
		t.Fatalf("JWKS fetched %d times for 5 forged kids, want 1", n)
	}
	// a rotated-in key becomes usable after the minimum refetch interval
	now := time.Now()
	e.srv.oc.now = func() time.Time { return now.Add(time.Minute) }
	s2 := e.aas.addEC(t, "k2")
	e.login(t)
	if c := e.postLogout(t, e.aas.logoutToken(s2, func(c map[string]any) { c["iat"] = now.Add(time.Minute).Unix() })); c != 200 {
		t.Fatalf("rotated key refused: %d", c)
	}
}
