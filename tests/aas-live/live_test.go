// Package aaslive runs libs/go-common/auth against a real AAS (Keycloak). Gated on AAS_LIVE_BASE_URL.
package aaslive

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/astryon/go-common/auth"
)

func (l *live) cfg() auth.Config {
	return auth.Config{BaseURL: l.base, Audience: l.audience, Environment: l.env, HTTPClient: l.http}
}

func (l *live) verifier(mod func(*auth.Config)) *auth.Verifier {
	l.t.Helper()
	c := l.cfg()
	if mod != nil {
		mod(&c)
	}
	v, err := auth.NewVerifier(ctx(l.t), c)
	if err != nil {
		l.t.Fatalf("NewVerifier: %v", err)
	}
	return v
}

func suffix() string { return fmt.Sprintf("%x", randBytes(4)) }

func TestLiveVerifier(t *testing.T) {
	l := newLive(t)
	l.newScope(scopeWrite)
	l.newScope(scopeQuery)
	a := l.newClient(suffix(), tenantA, 0, scopeWrite, scopeQuery)
	tok := l.token(a, scopeWrite+" "+scopeQuery)

	hdr, cl := decodePart(t, tok, 0), decodePart(t, tok, 1)
	keys := make([]string, 0, len(cl))
	for k := range cl {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Logf("real AAS JWT header: %v", hdr)
	t.Logf("real AAS claim names: %v", keys)
	t.Logf("typ=%v aud=%T(%v) env=%v azp=%v scope=%q tenant_id=%v iss=%v", cl["typ"], cl["aud"], cl["aud"], cl["env"], cl["azp"], cl["scope"], cl["tenant_id"], cl["iss"])

	v := l.verifier(func(c *auth.Config) { c.AllowedAZP = []string{a.ID} })

	t.Run("valid token maps tenant and scopes", func(t *testing.T) {
		p, err := v.Verify(ctx(t), tok)
		if err != nil {
			t.Fatalf("valid real token rejected: %v", err)
		}
		if p.TenantID != tenantA || !p.HasScope(scopeWrite) || !p.HasScope(scopeQuery) || p.JTI == "" || p.Subject == "" {
			t.Fatalf("bad principal: %+v", p)
		}
		if p.ClientID != a.ID {
			t.Fatalf("client_id claim not mapped: %+v", p)
		}
	})

	t.Run("scope not requested is absent", func(t *testing.T) {
		p, err := v.Verify(ctx(t), l.token(a, scopeWrite))
		if err != nil || p.HasScope(scopeQuery) || !p.HasScope(scopeWrite) {
			t.Fatalf("%+v %v", p, err)
		}
	})

	reject := func(name string, v *auth.Verifier, token string) {
		t.Run(name, func(t *testing.T) {
			if p, err := v.Verify(ctx(t), token); err == nil {
				t.Fatalf("accepted: %+v", p)
			}
		})
	}
	reject("wrong audience", l.verifier(func(c *auth.Config) { c.Audience = "https://other.example" }), tok)
	reject("wrong env", l.verifier(func(c *auth.Config) { c.Environment = "prod" }), tok)
	reject("disallowed azp", l.verifier(func(c *auth.Config) { c.AllowedAZP = []string{"someone-else"} }), tok)
	reject("strict RFC 9068 typ at+jwt rejects Keycloak Bearer", l.verifier(func(c *auth.Config) { c.TokenType = "at+jwt" }), tok)
	reject("wrong issuer pin", func() *auth.Verifier {
		// issuer pin is compared with discovery; a mismatch must fail at construction.
		c := l.cfg()
		c.Issuer = l.base + "/x"
		if _, err := auth.NewVerifier(ctx(t), c); err == nil {
			t.Fatal("issuer mismatch accepted")
		}
		return v
	}(), "not.a.jwt")
	reject("expired (clock beyond exp)", l.verifier(func(c *auth.Config) { c.Now = func() time.Time { return time.Now().Add(10 * time.Minute) } }), tok)

	parts := strings.Split(tok, ".")
	flip := func(s string) string {
		b := []byte(s)
		if b[len(b)/2] == 'A' {
			b[len(b)/2] = 'B'
		} else {
			b[len(b)/2] = 'A'
		}
		return string(b)
	}
	reject("tampered signature", v, parts[0]+"."+parts[1]+"."+flip(parts[2]))
	other := decodePart(t, tok, 1)
	other["tenant_id"] = "ffffffff-1111-4222-8333-444455556666"
	raw, _ := json.Marshal(other)
	reject("tampered payload (tenant swap)", v, parts[0]+"."+b64(raw)+"."+parts[2])
	none, _ := json.Marshal(map[string]string{"alg": "none", "typ": "JWT", "kid": hdr["kid"].(string)})
	reject("alg none", v, b64(none)+"."+parts[1]+".")
	hs, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT", "kid": hdr["kid"].(string)})
	reject("alg HS256 confusion", v, b64(hs)+"."+parts[1]+"."+parts[2])

	t.Run("really expired token", func(t *testing.T) {
		short := l.newClient(suffix(), tenantA, 3)
		st := l.token(short, "")
		sv := l.verifier(nil)
		if _, err := sv.Verify(ctx(t), st); err != nil {
			t.Fatalf("fresh short token rejected: %v", err)
		}
		time.Sleep(4 * time.Second)
		if _, err := sv.Verify(ctx(t), st); err == nil {
			t.Fatal("expired token accepted")
		}
	})

	t.Run("non-UUID tenant (AAS demo style tn_x) is rejected fail-closed", func(t *testing.T) {
		c := l.newClient(suffix(), "tn_test", 0)
		if _, err := l.verifier(nil).Verify(ctx(t), l.token(c, "")); err == nil {
			t.Fatal("non-UUID tenant accepted")
		}
	})
}

func TestLiveJWKSRotation(t *testing.T) {
	l := newLive(t)
	c := l.newClient(suffix(), tenantA, 0)
	v := l.verifier(func(cfg *auth.Config) { cfg.JWKSCacheTTL = time.Hour })
	oldTok := l.token(c, "")
	if _, err := v.Verify(ctx(t), oldTok); err != nil {
		t.Fatal(err)
	}
	oldKid := decodePart(t, oldTok, 0)["kid"]

	// Add a higher-priority signing key to the realm: new tokens get a new kid.
	realm := map[string]any{}
	_, b, _ := l.adminJSON("GET", "", nil)
	_ = json.Unmarshal(b, &realm)
	comp := map[string]any{"name": "live-rot-" + suffix(), "providerId": "ecdsa-generated", "providerType": "org.keycloak.keys.KeyProvider", "parentId": realm["id"],
		"config": map[string]any{"priority": []string{"900"}, "ecdsaEllipticCurveKey": []string{"P-256"}, "active": []string{"true"}, "enabled": []string{"true"}}}
	h := l.mustAdmin("POST", "/components", comp, 201)
	compID := h.Get("Location")[strings.LastIndex(h.Get("Location"), "/")+1:]
	deleted := false
	t.Cleanup(func() {
		if !deleted {
			l.adminJSON("DELETE", "/components/"+compID, nil)
		}
	})

	newTok := l.token(c, "")
	newKid := decodePart(t, newTok, 0)["kid"]
	if newKid == oldKid {
		t.Fatalf("rotation did not change kid (%v)", newKid)
	}
	before := l.jwksHits.Load()
	if _, err := v.Verify(ctx(t), newTok); err != nil {
		t.Fatalf("token with a new kid rejected although cache TTL is 1h (must refresh on unknown kid): %v", err)
	}
	if got := l.jwksHits.Load() - before; got != 1 {
		t.Fatalf("expected exactly 1 JWKS refetch for the rotation, got %d", got)
	}
	if _, err := v.Verify(ctx(t), oldTok); err != nil {
		t.Fatalf("old-key token must keep verifying during overlap: %v", err)
	}

	// Unknown kid signed by an attacker key: rejected, and a flood must not become a JWKS flood.
	atk, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	forged := func(kid string) string {
		hd, _ := json.Marshal(map[string]string{"alg": "ES256", "typ": "JWT", "kid": kid})
		in := b64(hd) + "." + strings.Split(newTok, ".")[1]
		d := sha256.Sum256([]byte(in))
		r, s, _ := ecdsa.Sign(rand.Reader, atk, d[:])
		return in + "." + b64(append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...))
	}
	before = l.jwksHits.Load()
	for i := 0; i < 30; i++ {
		if _, err := v.Verify(ctx(t), forged(fmt.Sprintf("unknown-%d", i))); err == nil {
			t.Fatal("unknown kid accepted")
		}
	}
	if got := l.jwksHits.Load() - before; got > 3 {
		t.Fatalf("30 unknown-kid tokens caused %d JWKS fetches: no refresh cooldown", got)
	}
	// forged token reusing the REAL kid but signed by attacker key
	if _, err := v.Verify(ctx(t), forged(newKid.(string))); err == nil {
		t.Fatal("attacker signature under real kid accepted")
	}

	// Key removal: after refresh the removed kid is no longer trusted.
	l.mustAdmin("DELETE", "/components/"+compID, nil, 204)
	deleted = true
	v2 := l.verifier(nil)
	if _, err := v2.Verify(ctx(t), newTok); err == nil {
		t.Fatal("token signed by a removed key accepted by a fresh verifier")
	}
}

func TestLiveAASOutage(t *testing.T) {
	container := getenv("AAS_LIVE_KEYCLOAK_CONTAINER", "")
	if container == "" {
		t.Skip("AAS_LIVE_KEYCLOAK_CONTAINER not set (outage test stops/starts that container)")
	}
	l := newLive(t)
	c := l.newClient(suffix(), tenantA, 0)
	tok := l.token(c, "")

	var skew time.Duration
	now := func() time.Time { return time.Now().Add(skew) }
	v := l.verifier(func(cfg *auth.Config) { cfg.JWKSCacheTTL = 2 * time.Second; cfg.Now = now })
	if _, err := v.Verify(ctx(t), tok); err != nil {
		t.Fatal(err)
	}
	restarted := false
	start := func() {
		if restarted {
			return
		}
		restarted = true
		if out, err := exec.Command("docker", "start", container).CombinedOutput(); err != nil {
			t.Fatalf("docker start: %v %s", err, out)
		}
	}
	if out, err := exec.Command("docker", "stop", container).CombinedOutput(); err != nil {
		t.Fatalf("docker stop: %v %s", err, out)
	}
	t.Cleanup(func() { start(); l.waitReady(150 * time.Second); l.adminLogin() })

	t.Run("within JWKS TTL", func(t *testing.T) {
		if _, err := v.Verify(ctx(t), tok); err != nil {
			t.Fatalf("cached key must verify while AAS is down: %v", err)
		}
	})
	t.Run("after JWKS TTL, AAS still down, token unexpired", func(t *testing.T) {
		skew = 10 * time.Second // cache stale, token (5 min) still valid
		if _, err := v.Verify(ctx(t), tok); err != nil {
			t.Fatalf("stale cached key must keep verifying unexpired tokens during an outage: %v", err)
		}
	})
	t.Run("unknown kid during outage is rejected", func(t *testing.T) {
		atk, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		hd, _ := json.Marshal(map[string]string{"alg": "ES256", "typ": "JWT", "kid": "nope"})
		in := b64(hd) + "." + strings.Split(tok, ".")[1]
		d := sha256.Sum256([]byte(in))
		r, s, _ := ecdsa.Sign(rand.Reader, atk, d[:])
		if _, err := v.Verify(ctx(t), in+"."+b64(append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...))); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("after exp during outage: fail closed", func(t *testing.T) {
		skew = 10 * time.Minute
		if _, err := v.Verify(ctx(t), tok); err == nil {
			t.Fatal("expired token accepted while AAS down")
		}
		skew = 0
	})
	t.Run("restart: recovery without restarting the verifier", func(t *testing.T) {
		start()
		deadline := time.Now().Add(150 * time.Second)
		var fresh string
		for time.Now().Before(deadline) {
			if tk, code, _ := l.tryToken(c, ""); code == 200 {
				fresh = tk
				break
			}
			time.Sleep(3 * time.Second)
		}
		if fresh == "" {
			t.Fatal("AAS did not come back")
		}
		time.Sleep(2500 * time.Millisecond) // cache TTL elapsed -> real refresh path
		if _, err := v.Verify(ctx(t), fresh); err != nil {
			t.Fatalf("verifier did not recover after AAS restart: %v", err)
		}
	})
}
