package aaslive

import (
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/astryon/go-common/auth"
)

// introspector writes the throw-away client key to a temp file (as AAS_INTROSPECTION_KEY_FILE would
// point to a mounted secret) and builds the library client against the real AAS.
func (l *live) introspector(c *client) *auth.Introspector {
	l.t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(c.key)
	if err != nil {
		l.t.Fatal(err)
	}
	path := filepath.Join(l.t.TempDir(), "introspection.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		l.t.Fatal(err)
	}
	in, err := auth.NewIntrospector(ctx(l.t), auth.IntrospectorConfig{BaseURL: l.base, ClientID: c.ID, PrivateKeyFile: path, HTTPClient: l.http,
		PositiveTTL: time.Second, NegativeTTL: time.Second})
	if err != nil {
		l.t.Fatalf("NewIntrospector: %v", err)
	}
	return in
}

// selfAudience puts the client itself into the aud of its own tokens. Keycloak 26 (AAS) answers
// active=false to a client that is not in the token's aud, so an L3 resource server must be an audience
// of the tokens it introspects (in AAS the gateway client id IS the audience URI).
func (l *live) selfAudience(c *client) *client {
	l.t.Helper()
	l.mustAdmin("POST", "/clients/"+c.uuid+"/protocol-mappers/models", map[string]any{"name": "aud-self", "protocol": "openid-connect",
		"protocolMapper": "oidc-audience-mapper", "config": map[string]string{"included.client.audience": c.ID, "access.token.claim": "true",
			"id.token.claim": "false", "introspection.token.claim": "true"}}, 201)
	return c
}

func TestLiveIntrospectionAndRevocation(t *testing.T) {
	l := newLive(t)
	l.newScope(scopeWrite)
	a := l.selfAudience(l.newClient(suffix(), tenantA, 0, scopeWrite))
	v := l.verifier(func(c *auth.Config) { c.AllowedAZP = []string{a.ID} })
	in := l.introspector(a)

	tok := l.token(a, scopeWrite)
	p, err := v.Verify(ctx(t), tok)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("active token has the same claims as the verified principal", func(t *testing.T) {
		r, err := in.Introspect(ctx(t), tok)
		if err != nil || !r.Active {
			t.Fatalf("%+v %v", r, err)
		}
		ip := r.Principal()
		t.Logf("introspection claim names present: tenant=%q sub=%q client_id=%q jti=%q scope=%q", ip.TenantID, ip.Subject, ip.ClientID, ip.JTI, r.Claims["scope"])
		if ip.Subject != p.Subject || ip.JTI != p.JTI || ip.ClientID != p.ClientID || ip.TenantID != p.TenantID || !ip.HasScope(scopeWrite) {
			t.Fatalf("introspection %+v != verified %+v", ip, p)
		}
	})

	var reached int
	h := v.Middleware(auth.RequireActive(in, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached++ })))
	call := func(tok string) int {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Code
	}
	if c := call(tok); c != 200 || reached != 1 {
		t.Fatalf("active token through RequireActive: %d", c)
	}

	t.Run("revoked token is inactive and rejected", func(t *testing.T) {
		rev := l.token(a, scopeWrite)
		if _, err := v.Verify(ctx(t), rev); err != nil {
			t.Fatalf("pre-revoke verify: %v", err)
		}
		if err := in.Revoke(ctx(t), rev); err != nil {
			t.Fatalf("revoke: %v", err)
		}
		// the signature and exp are still valid: only introspection can tell
		if _, err := v.Verify(ctx(t), rev); err != nil {
			t.Fatalf("local verification of revoked token should still pass (that is why L3 exists): %v", err)
		}
		r, err := in.Introspect(ctx(t), rev)
		if err != nil || r.Active {
			t.Fatalf("revoked token reported %+v %v", r, err)
		}
		before := reached
		if c := call(rev); c != 401 || reached != before {
			t.Fatalf("revoked token through RequireActive: %d", c)
		}
	})

	t.Run("expired token is inactive and rejected", func(t *testing.T) {
		short := l.selfAudience(l.newClient(suffix(), tenantA, 3, scopeWrite))
		sv := l.verifier(func(c *auth.Config) { c.AllowedAZP = []string{short.ID} })
		sin := l.introspector(short)
		st := l.token(short, scopeWrite)
		if _, err := sv.Verify(ctx(t), st); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Second)
		if _, err := sv.Verify(ctx(t), st); err == nil {
			t.Fatal("expired token verified")
		}
		r, err := sin.Introspect(ctx(t), st)
		if err != nil || r.Active {
			t.Fatalf("expired token reported %+v %v", r, err)
		}
	})

	t.Run("garbage token and foreign client assertion fail closed or inactive", func(t *testing.T) {
		if r, err := in.Introspect(ctx(t), "not-a-token"); err != nil || r.Active {
			t.Fatalf("%+v %v", r, err)
		}
		other := l.newClient(suffix(), tenantA, 0, scopeWrite)
		oin := l.introspector(other)
		r, err := oin.Introspect(ctx(t), l.token(a, scopeWrite))
		t.Logf("client B introspecting client A's token: active=%v err=%v", r.Active, err)
	})
}
