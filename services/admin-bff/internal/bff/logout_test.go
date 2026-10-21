package bff

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func sign(t *testing.T, key *ecdsa.PrivateKey, hdr map[string]any, cl map[string]any) string {
	t.Helper()
	hb, _ := json.Marshal(hdr)
	cb, _ := json.Marshal(cl)
	in := b64u(hb) + "." + b64u(cb)
	d := sha256.Sum256([]byte(in))
	r, s, err := ecdsa.Sign(rand.Reader, key, d[:])
	if err != nil {
		t.Fatal(err)
	}
	return in + "." + b64u(append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...))
}

func (f *fakeAAS) logoutToken(t *testing.T, mut func(cl map[string]any)) string {
	cl := map[string]any{"iss": f.URL + "/realm", "aud": "astryon-portal", "iat": time.Now().Unix(), "jti": randString(10),
		"sid": "sid-1", "events": map[string]any{backchannelEvent: map[string]any{}}}
	if mut != nil {
		mut(cl)
	}
	return sign(t, f.key, map[string]any{"alg": "ES256", "typ": "logout+jwt", "kid": "k1"}, cl)
}

func (e *env) bcl(t *testing.T, tok string) int {
	t.Helper()
	res, err := http.PostForm(e.bff.URL+"/auth/backchannel-logout", url.Values{"logout_token": {tok}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	return res.StatusCode
}

func (e *env) alive(t *testing.T) bool {
	res, _ := e.do(t, "GET", "/api/query/x", nil)
	return res.StatusCode == 200
}

func TestBackChannelLogoutBySidAndSub(t *testing.T) {
	e := setup(t, nil)
	e.login(t)
	if !e.alive(t) {
		t.Fatal("session should work")
	}
	<-e.got
	if c := e.bcl(t, e.aas.logoutToken(t, func(cl map[string]any) { cl["sid"] = "other-sid" })); c != 200 || !e.alive(t) {
		t.Fatalf("a logout for another sid must be accepted and leave this session alone (status %d)", c)
	}
	<-e.got
	if c := e.bcl(t, e.aas.logoutToken(t, nil)); c != 200 {
		t.Fatalf("valid logout = %d", c)
	}
	if e.alive(t) {
		t.Fatal("session survived back-channel logout")
	}
	// by sub only: ends every session of the user
	e.login(t)
	if c := e.bcl(t, e.aas.logoutToken(t, func(cl map[string]any) { delete(cl, "sid"); cl["sub"] = "user-1" })); c != 200 || e.alive(t) {
		t.Fatalf("logout by sub: status %d", c)
	}
}

func TestBackChannelLogoutRejects(t *testing.T) {
	e := setup(t, nil)
	e.login(t)
	other, _ := ecdsa.GenerateKey(e.aas.key.Curve, rand.Reader)
	cases := map[string]string{
		"wrong key":     sign(t, other, map[string]any{"alg": "ES256", "kid": "k1"}, map[string]any{"iss": e.aas.URL + "/realm", "aud": "astryon-portal", "iat": time.Now().Unix(), "jti": "x1", "sid": "sid-1", "events": map[string]any{backchannelEvent: map[string]any{}}}),
		"alg none":      b64u([]byte(`{"alg":"none"}`)) + "." + b64u([]byte(`{"iss":"x"}`)) + ".",
		"alg HS256":     sign(t, e.aas.key, map[string]any{"alg": "HS256", "kid": "k1"}, map[string]any{}),
		"unknown kid":   sign(t, e.aas.key, map[string]any{"alg": "ES256", "kid": "zzz"}, map[string]any{}),
		"bad typ":       sign(t, e.aas.key, map[string]any{"alg": "ES256", "kid": "k1", "typ": "at+jwt"}, map[string]any{}),
		"issuer":        e.aas.logoutToken(t, func(cl map[string]any) { cl["iss"] = "https://evil" }),
		"audience":      e.aas.logoutToken(t, func(cl map[string]any) { cl["aud"] = "someone-else" }),
		"no iat":        e.aas.logoutToken(t, func(cl map[string]any) { delete(cl, "iat") }),
		"old iat":       e.aas.logoutToken(t, func(cl map[string]any) { cl["iat"] = time.Now().Add(-time.Hour).Unix() }),
		"future iat":    e.aas.logoutToken(t, func(cl map[string]any) { cl["iat"] = time.Now().Add(time.Hour).Unix() }),
		"expired":       e.aas.logoutToken(t, func(cl map[string]any) { cl["exp"] = time.Now().Add(-time.Hour).Unix() }),
		"no jti":        e.aas.logoutToken(t, func(cl map[string]any) { delete(cl, "jti") }),
		"no events":     e.aas.logoutToken(t, func(cl map[string]any) { delete(cl, "events") }),
		"wrong event":   e.aas.logoutToken(t, func(cl map[string]any) { cl["events"] = map[string]any{"urn:x": map[string]any{}} }),
		"event not {}":  e.aas.logoutToken(t, func(cl map[string]any) { cl["events"] = map[string]any{backchannelEvent: "yes"} }),
		"nonce":         e.aas.logoutToken(t, func(cl map[string]any) { cl["nonce"] = "n" }),
		"no sub or sid": e.aas.logoutToken(t, func(cl map[string]any) { delete(cl, "sid") }),
		"garbage":       "a.b",
	}
	for name, tok := range cases {
		if c := e.bcl(t, tok); c != 400 {
			t.Errorf("%s: status %d, want 400", name, c)
		}
	}
	if !e.alive(t) {
		t.Fatal("a rejected token must not end the session")
	}
	// wrong content type / missing token
	res, _ := http.Post(e.bff.URL+"/auth/backchannel-logout", "application/json", strings.NewReader(`{"logout_token":"x"}`))
	if res.StatusCode != 400 {
		t.Fatalf("json body = %d", res.StatusCode)
	}
	if c := e.bcl(t, ""); c != 400 {
		t.Fatalf("empty = %d", c)
	}
}

func TestBackChannelLogoutReplay(t *testing.T) {
	e := setup(t, nil)
	tok := e.aas.logoutToken(t, nil)
	if c := e.bcl(t, tok); c != 200 {
		t.Fatalf("first = %d", c)
	}
	e.login(t) // a new session must not be killed by replaying the old token
	if c := e.bcl(t, tok); c != 400 || !e.alive(t) {
		t.Fatalf("replay accepted: %d", c)
	}
}

func TestBackChannelLogoutRSAAndKeyRotation(t *testing.T) {
	rk, _ := rsa.GenerateKey(rand.Reader, 2048)
	aas := newFakeAAS(t)
	// serve an RSA key in addition to the EC one by wrapping the server handler
	inner := aas.Config.Handler
	aas.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/realm/jwks" {
			json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{"kty": "RSA", "kid": "r1", "n": b64u(rk.N.Bytes()), "e": b64u(big.NewInt(int64(rk.E)).Bytes())}}})
			return
		}
		inner.ServeHTTP(w, r)
	})
	e := setupOn(t, aas, nil)
	e.login(t)
	hdr, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": "r1"})
	cb, _ := json.Marshal(map[string]any{"iss": aas.URL + "/realm", "aud": []string{"astryon-portal", "x"}, "iat": time.Now().Unix(), "jti": "rsa1", "sid": "sid-1", "events": map[string]any{backchannelEvent: map[string]any{}}})
	in := b64u(hdr) + "." + b64u(cb)
	d := sha256.Sum256([]byte(in))
	sig, _ := rsa.SignPKCS1v15(rand.Reader, rk, 5, d[:]) // crypto.SHA256 == 5
	if c := e.bcl(t, in+"."+b64u(sig)); c != 200 || e.alive(t) {
		t.Fatalf("RS256 logout: %d", c)
	}
}

// storeContract runs against any Store implementation.
func storeContract(t *testing.T, st Store) {
	ctx := context.Background()
	id := "s-" + randString(6)
	sub, sid := "sub-"+randString(6), "sid-"+randString(6)
	rec := Record{Tok: Tokens{Access: "A", Refresh: "R", ID: "I", Expires: time.Now().Add(time.Minute).Truncate(time.Second)}, Sub: sub, SID: sid, Created: time.Now()}
	if _, ok, err := st.GetSession(ctx, id); ok || err != nil {
		t.Fatalf("get missing: %v %v", ok, err)
	}
	if err := st.UpdateSession(ctx, id, rec, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.GetSession(ctx, id); ok {
		t.Fatal("update must not create a session")
	}
	if err := st.CreateSession(ctx, id, rec, time.Hour); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st.GetSession(ctx, id)
	if err != nil || !ok || got.Tok.Refresh != "R" || got.Sub != sub || got.SID != sid {
		t.Fatalf("get: %+v %v %v", got, ok, err)
	}
	rec.Tok.Access = "A2"
	if err := st.UpdateSession(ctx, id, rec, time.Hour); err != nil {
		t.Fatal(err)
	}
	if got, _, _ = st.GetSession(ctx, id); got.Tok.Access != "A2" {
		t.Fatal("update lost")
	}
	id2 := "s-" + randString(6)
	_ = st.CreateSession(ctx, id2, Record{Sub: sub, SID: "sid2-" + randString(4), Created: time.Now()}, time.Hour)
	if n, err := st.DeleteBySID(ctx, sid); n != 1 || err != nil {
		t.Fatalf("DeleteBySID = %d %v", n, err)
	}
	if _, ok, _ := st.GetSession(ctx, id); ok {
		t.Fatal("sid delete missed")
	}
	if _, ok, _ := st.GetSession(ctx, id2); !ok {
		t.Fatal("sid delete too broad")
	}
	if n, _ := st.DeleteBySub(ctx, sub); n != 1 {
		t.Fatalf("DeleteBySub = %d", n)
	}
	_ = st.CreateSession(ctx, id, rec, time.Hour)
	_ = st.DeleteSession(ctx, id)
	if _, ok, _ := st.GetSession(ctx, id); ok {
		t.Fatal("delete failed")
	}
	if n, _ := st.DeleteBySID(ctx, sid); n != 0 {
		t.Fatal("index not cleaned")
	}
	state := randString(8)
	if err := st.SavePending(ctx, state, Pending{"v", "n", time.Now().Add(time.Minute)}, time.Minute); err != nil {
		t.Fatal(err)
	}
	p, ok, _ := st.TakePending(ctx, state)
	if !ok || p.Verifier != "v" || p.Nonce != "n" {
		t.Fatalf("pending %+v %v", p, ok)
	}
	if _, ok, _ := st.TakePending(ctx, state); ok {
		t.Fatal("pending must be single use")
	}
	j := randString(8)
	if f, _ := st.SeenJTI(ctx, j, time.Minute); !f {
		t.Fatal("first jti")
	}
	if f, _ := st.SeenJTI(ctx, j, time.Minute); f {
		t.Fatal("jti replay not detected")
	}
}

func TestMemoryStoreContract(t *testing.T) { storeContract(t, NewMemoryStore(100, nil)) }

func TestMemoryStoreBounds(t *testing.T) {
	st := NewMemoryStore(2, nil)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := st.SavePending(ctx, randString(4), Pending{Exp: time.Now().Add(time.Hour)}, time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.SavePending(ctx, "x", Pending{Exp: time.Now().Add(time.Hour)}, time.Hour); err != ErrFull {
		t.Fatalf("want ErrFull, got %v", err)
	}
}

func redisURL(t *testing.T) string {
	u := os.Getenv("BFF_TEST_REDIS_URL")
	if u == "" {
		t.Skip("set BFF_TEST_REDIS_URL (e.g. redis://127.0.0.1:6379/15) to run the Redis integration tests")
	}
	return u
}

func newTestRedis(t *testing.T, key byte) Store {
	k := make([]byte, 32)
	k[0] = key
	st, err := NewRedisStore(context.Background(), RedisConfig{URL: redisURL(t), Key: k, Prefix: "bfftest:" + randString(4) + ":"})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestRedisStoreContract(t *testing.T) { storeContract(t, newTestRedis(t, 1)) }

func TestRedisStoreSealsTokensAndChecksKey(t *testing.T) {
	url := redisURL(t)
	pre := "bfftest:" + randString(4) + ":"
	k1, k2 := make([]byte, 32), make([]byte, 32)
	k2[0] = 9
	a, _ := NewRedisStore(context.Background(), RedisConfig{URL: url, Key: k1, Prefix: pre})
	b, _ := NewRedisStore(context.Background(), RedisConfig{URL: url, Key: k2, Prefix: pre})
	ctx := context.Background()
	_ = a.CreateSession(ctx, "id1", Record{Tok: Tokens{Access: "SECRET-ACCESS-TOKEN"}, Created: time.Now()}, time.Minute)
	raw, _ := a.(*redisStore).rc.do(ctx, "GET", pre+"s:id1")
	if strings.Contains(string(raw.([]byte)), "SECRET-ACCESS-TOKEN") {
		t.Fatal("tokens stored in clear text")
	}
	if _, ok, err := b.GetSession(ctx, "id1"); ok || err == nil {
		t.Fatal("a different key must not open the session")
	}
	if _, err := NewRedisStore(ctx, RedisConfig{URL: url, Key: []byte("short")}); err == nil {
		t.Fatal("short key accepted")
	}
}

// Two BFF replicas sharing one store: a session made on A works on B, is ended for both by a back-channel logout at B.
func TestTwoReplicasShareSessions(t *testing.T) {
	for name, mk := range map[string]func(*testing.T) Store{
		"memory": func(*testing.T) Store { return NewMemoryStore(100, nil) },
		"redis":  func(t *testing.T) Store { return newTestRedis(t, 2) },
	} {
		t.Run(name, func(t *testing.T) {
			store := mk(t)
			aas := newFakeAAS(t)
			a := setupOn(t, aas, func(o *Options) { o.Store = store })
			b := setupOn(t, aas, func(o *Options) { o.Store = store })
			a.login(t)
			u, _ := url.Parse(a.bff.URL)
			var cookie string
			for _, c := range a.client.Jar.Cookies(u) {
				if c.Name == "astryon_session" {
					cookie = c.Name + "=" + c.Value
				}
			}
			if cookie == "" {
				t.Fatal("no cookie")
			}
			if res, _ := b.do(t, "GET", "/api/query/x", map[string]string{"Cookie": cookie}); res.StatusCode != 200 {
				t.Fatalf("replica B does not know A's session: %d", res.StatusCode)
			}
			tok := aas.logoutToken(t, nil)
			if c := b.bcl(t, tok); c != 200 {
				t.Fatalf("logout = %d", c)
			}
			if res, _ := a.do(t, "GET", "/api/query/x", map[string]string{"Cookie": cookie}); res.StatusCode != 401 {
				t.Fatalf("A still serves the session after back-channel logout at B: %d", res.StatusCode)
			}
			if c := a.bcl(t, tok); c != 400 { // replay is detected across replicas
				t.Fatalf("replay at replica A = %d", c)
			}
		})
	}
}
