package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeAAS answers discovery, introspection and revocation and validates the private_key_jwt.
type fakeAAS struct {
	t        *testing.T
	srv      *httptest.Server
	pub      *ecdsa.PublicKey
	status   map[string]map[string]any // token -> response
	revoked  map[string]bool
	hits     atomic.Int64
	failWith int
}

func newFakeAAS(t *testing.T) (*fakeAAS, string) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	path := filepath.Join(t.TempDir(), "k.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	f := &fakeAAS{t: t, pub: &key.PublicKey, status: map[string]map[string]any{}, revoked: map[string]bool{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f, path
}

func (f *fakeAAS) serve(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": f.srv.URL + "/iss", "jwks_uri": f.srv.URL + "/jwks",
			"introspection_endpoint": f.srv.URL + "/introspect", "revocation_endpoint": f.srv.URL + "/revoke"})
		return
	case "/introspect", "/revoke":
	default:
		http.NotFound(w, r)
		return
	}
	f.hits.Add(1)
	if f.failWith != 0 {
		w.WriteHeader(f.failWith)
		return
	}
	if err := r.ParseForm(); err != nil || !f.assertionOK(r.PostForm.Get("client_assertion")) || r.PostForm.Get("client_id") != "svc" ||
		r.PostForm.Get("client_assertion_type") != clientAssertionType {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	tok := r.PostForm.Get("token")
	if r.URL.Path == "/revoke" {
		f.revoked[tok] = true
		return
	}
	resp, ok := f.status[tok]
	if !ok || f.revoked[tok] {
		resp = map[string]any{"active": false}
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (f *fakeAAS) assertionOK(a string) bool {
	parts := strings.Split(a, ".")
	if len(parts) != 3 {
		return false
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		return false
	}
	d := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if !ecdsa.Verify(f.pub, d[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		return false
	}
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var c struct {
		Iss, Sub, Aud, JTI string
		Iat, Exp           int64
	}
	_ = json.Unmarshal([]byte(strings.NewReplacer(`"iss"`, `"Iss"`, `"sub"`, `"Sub"`, `"aud"`, `"Aud"`, `"jti"`, `"JTI"`, `"iat"`, `"Iat"`, `"exp"`, `"Exp"`).Replace(string(raw))), &c)
	return c.Iss == "svc" && c.Sub == "svc" && c.Aud == f.srv.URL+"/iss" && c.JTI != "" && c.Iat > 0 && c.Exp-c.Iat > 0 && c.Exp-c.Iat <= 60
}

func activeResp(jti string) map[string]any {
	return map[string]any{"active": true, "sub": "u", "client_id": "svc", "jti": jti, "tenant_id": tenantID, "scope": "a b",
		"exp": float64(time.Now().Add(time.Hour).Unix())}
}

func TestIntrospectorActiveInactiveAndCache(t *testing.T) {
	f, keyFile := newFakeAAS(t)
	f.status["good"] = activeResp("j1")
	now := time.Now()
	in, err := NewIntrospector(context.Background(), IntrospectorConfig{BaseURL: f.srv.URL, ClientID: "svc", PrivateKeyFile: keyFile,
		PositiveTTL: 10 * time.Second, NegativeTTL: 30 * time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	r, err := in.Introspect(context.Background(), "good")
	if err != nil || !r.Active || r.Principal().TenantID != tenantID || !r.Principal().HasScope("b") {
		t.Fatalf("active: %+v %v", r, err)
	}
	_, _ = in.Introspect(context.Background(), "good")
	if f.hits.Load() != 1 {
		t.Fatalf("positive result not cached: %d hits", f.hits.Load())
	}
	now = now.Add(11 * time.Second)
	_, _ = in.Introspect(context.Background(), "good")
	if f.hits.Load() != 2 {
		t.Fatalf("positive cache did not expire: %d", f.hits.Load())
	}
	// inactive: claims are dropped, cached for the negative TTL
	r, err = in.Introspect(context.Background(), "bad")
	if err != nil || r.Active || len(r.Claims) != 1 {
		t.Fatalf("inactive: %+v %v", r, err)
	}
	h := f.hits.Load()
	_, _ = in.Introspect(context.Background(), "bad")
	if f.hits.Load() != h {
		t.Fatal("negative result not cached")
	}
	now = now.Add(31 * time.Second)
	_, _ = in.Introspect(context.Background(), "bad")
	if f.hits.Load() != h+1 {
		t.Fatal("negative cache did not expire")
	}
}

func TestIntrospectorFailsClosedAndDoesNotCacheErrors(t *testing.T) {
	f, keyFile := newFakeAAS(t)
	f.status["good"] = activeResp("j1")
	in, err := NewIntrospector(context.Background(), IntrospectorConfig{BaseURL: f.srv.URL, ClientID: "svc", PrivateKeyFile: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []int{500, 401, 204} {
		f.failWith = code
		if _, err := in.Introspect(context.Background(), "good"); !errors.Is(err, ErrIntrospectionUnavailable) {
			t.Fatalf("status %d: %v", code, err)
		}
	}
	f.failWith = 0
	if r, err := in.Introspect(context.Background(), "good"); err != nil || !r.Active {
		t.Fatalf("recovery: %+v %v", r, err)
	}
	f.srv.Close() // AAS gone
	in.mu.Lock()
	in.cache = map[[32]byte]introCacheEntry{}
	in.mu.Unlock()
	if _, err := in.Introspect(context.Background(), "good"); !errors.Is(err, ErrIntrospectionUnavailable) {
		t.Fatalf("outage: %v", err)
	}
}

func TestIntrospectorCacheIsBoundedAndRevokeEvicts(t *testing.T) {
	f, keyFile := newFakeAAS(t)
	in, err := NewIntrospector(context.Background(), IntrospectorConfig{BaseURL: f.srv.URL, ClientID: "svc", PrivateKeyFile: keyFile, MaxEntries: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		_, _ = in.Introspect(context.Background(), "t"+string(rune('a'+i)))
	}
	if len(in.cache) > 3 {
		t.Fatalf("cache grew to %d", len(in.cache))
	}
	f.status["good"] = activeResp("j1")
	if r, _ := in.Introspect(context.Background(), "good"); !r.Active {
		t.Fatal("expected active")
	}
	if err := in.Revoke(context.Background(), "good"); err != nil {
		t.Fatal(err)
	}
	if r, err := in.Introspect(context.Background(), "good"); err != nil || r.Active {
		t.Fatalf("revoked token still active: %+v %v", r, err)
	}
	f.failWith = 500
	if err := in.Revoke(context.Background(), "good"); !errors.Is(err, ErrIntrospectionUnavailable) {
		t.Fatalf("revoke failure: %v", err)
	}
}

func TestIntrospectorConfigErrors(t *testing.T) {
	f, keyFile := newFakeAAS(t)
	bad := filepath.Join(t.TempDir(), "bad.pem")
	_ = os.WriteFile(bad, []byte("nope"), 0o600)
	for name, cfg := range map[string]IntrospectorConfig{
		"missing":  {BaseURL: f.srv.URL},
		"not pem":  {BaseURL: f.srv.URL, ClientID: "svc", PrivateKeyFile: bad},
		"no file":  {BaseURL: f.srv.URL, ClientID: "svc", PrivateKeyFile: bad + "x"},
		"base 404": {BaseURL: f.srv.URL + "/nope", ClientID: "svc", PrivateKeyFile: keyFile},
	} {
		if _, err := NewIntrospector(context.Background(), cfg); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
	// endpoints on another origin are refused so the assertion cannot be sent elsewhere
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": "i", "introspection_endpoint": "https://evil.example/introspect"})
	}))
	defer evil.Close()
	if _, err := NewIntrospector(context.Background(), IntrospectorConfig{BaseURL: evil.URL, ClientID: "svc", PrivateKeyFile: keyFile}); err == nil {
		t.Fatal("cross-origin introspection endpoint accepted")
	}
}

func TestRequireActiveMiddleware(t *testing.T) {
	f, keyFile := newFakeAAS(t)
	in, err := NewIntrospector(context.Background(), IntrospectorConfig{BaseURL: f.srv.URL, ClientID: "svc", PrivateKeyFile: keyFile, PositiveTTL: time.Millisecond, NegativeTTL: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	reached := false
	h := RequireActive(in, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))
	call := func(p *Principal, tok string) int {
		reached = false
		req := httptest.NewRequest("GET", "/", nil)
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		if p != nil {
			req = req.WithContext(context.WithValue(req.Context(), principalKey{}, *p))
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Code
	}
	p := &Principal{TenantID: tenantID, Subject: "u", ClientID: "svc", JTI: "j1"}
	f.status["good"] = activeResp("j1")
	if c := call(p, "good"); c != 200 || !reached {
		t.Fatalf("active: %d", c)
	}
	if c := call(nil, "good"); c != 401 || reached {
		t.Fatalf("no principal: %d", c)
	}
	if c := call(p, ""); c != 401 {
		t.Fatalf("no token: %d", c)
	}
	if c := call(p, "unknown"); c != 401 || reached {
		t.Fatalf("inactive: %d", c)
	}
	other := *p
	other.TenantID = "99999999-1111-4222-8333-444455556666"
	time.Sleep(5 * time.Millisecond)
	if c := call(&other, "good"); c != 401 || reached {
		t.Fatalf("tenant mismatch: %d", c)
	}
	f.failWith = 503
	time.Sleep(5 * time.Millisecond)
	if c := call(p, "good"); c != 503 || reached {
		t.Fatalf("AAS down must deny: %d", c)
	}
}
