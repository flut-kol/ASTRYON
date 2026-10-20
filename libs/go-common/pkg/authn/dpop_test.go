package authn

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
)

var dpopNow = time.Unix(1_800_000_000, 0)

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

type proofOpts struct {
	typ, alg    string
	claims      map[string]any
	embedPriv   bool
	otherKey    *ecdsa.PrivateKey
	signAlgNone bool
}

func mkKey(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pk, _ := jwk.FromRaw(k.PublicKey)
	tp, _ := pk.Thumbprint(crypto.SHA256)
	return k, b64(tp)
}

func mkProof(t *testing.T, k *ecdsa.PrivateKey, o proofOpts) string {
	t.Helper()
	var jwkJSON []byte
	if o.embedPriv {
		pk, _ := jwk.FromRaw(k)
		jwkJSON, _ = json.Marshal(pk)
	} else {
		pk, _ := jwk.FromRaw(k.PublicKey)
		jwkJSON, _ = json.Marshal(pk)
	}
	typ, alg := o.typ, o.alg
	if typ == "" {
		typ = "dpop+jwt"
	}
	if alg == "" {
		alg = "ES256"
	}
	hdr, _ := json.Marshal(map[string]any{"typ": typ, "alg": alg, "jwk": json.RawMessage(jwkJSON)})
	pl, _ := json.Marshal(o.claims)
	input := b64(hdr) + "." + b64(pl)
	if alg == "none" {
		return input + "."
	}
	signer := k
	if o.otherKey != nil {
		signer = o.otherKey
	}
	// Sign with jws to get a proper ES256 signature over our custom header.
	h := sha256.Sum256([]byte(input))
	r, s, _ := ecdsa.Sign(rand.Reader, signer, h[:])
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return input + "." + b64(sig)
}

func baseClaims(jti string) map[string]any {
	return map[string]any{"jti": jti, "htm": "POST", "htu": "https://gw.example.com/v1/events", "iat": dpopNow.Unix()}
}

func newV(required bool) *DPoPVerifier {
	return NewDPoPVerifier(DPoPConfig{Required: required, Now: func() time.Time { return dpopNow }})
}

func TestDPoPVerify(t *testing.T) {
	k, jkt := mkKey(t)
	other, _ := mkKey(t)
	const at = "access.token.value"
	sum := sha256.Sum256([]byte(at))
	ath := b64(sum[:])
	with := func(m map[string]any, kv ...any) map[string]any {
		for i := 0; i < len(kv); i += 2 {
			if kv[i+1] == nil {
				delete(m, kv[i].(string))
			} else {
				m[kv[i].(string)] = kv[i+1]
			}
		}
		return m
	}
	tests := []struct {
		name string
		opts proofOpts
		req  DPoPRequest
		want error
	}{
		{"valid", proofOpts{claims: with(baseClaims("a1"), "ath", ath)}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events", AccessToken: at, BoundJKT: jkt}, nil},
		{"valid htu normalized, query ignored", proofOpts{claims: with(baseClaims("a2"), "htu", "HTTPS://GW.example.com:443/v1/events?x=1#f")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events?y=2"}, nil},
		{"wrong htm", proofOpts{claims: with(baseClaims("b1"), "htm", "GET")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
		{"wrong htu", proofOpts{claims: with(baseClaims("b2"), "htu", "https://gw.example.com/v1/other")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
		{"stale iat", proofOpts{claims: with(baseClaims("b3"), "iat", dpopNow.Add(-10*time.Minute).Unix())}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
		{"future iat", proofOpts{claims: with(baseClaims("b4"), "iat", dpopNow.Add(10*time.Minute).Unix())}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
		{"bad ath", proofOpts{claims: with(baseClaims("b5"), "ath", "nope")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events", AccessToken: at}, ErrInvalidDPoP},
		{"missing ath", proofOpts{claims: baseClaims("b6")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events", AccessToken: at}, ErrInvalidDPoP},
		{"jkt mismatch", proofOpts{claims: baseClaims("b7")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events", BoundJKT: "other"}, ErrDPoPKeyMismatch},
		{"alg none", proofOpts{alg: "none", claims: baseClaims("b8")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
		{"alg HS256", proofOpts{alg: "HS256", claims: baseClaims("b9")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
		{"wrong typ", proofOpts{typ: "JWT", claims: baseClaims("c1")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
		{"private key in jwk", proofOpts{embedPriv: true, claims: baseClaims("c2")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
		{"bad signature", proofOpts{otherKey: other, claims: baseClaims("c3")}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
		{"missing jti", proofOpts{claims: with(baseClaims("x"), "jti", nil)}, DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events"}, ErrInvalidDPoP},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.req.Proof = mkProof(t, k, tc.opts)
			res, err := newV(true).Verify(context.Background(), tc.req)
			if tc.want == nil {
				if err != nil || !res.Present || res.JKT != jkt {
					t.Fatalf("want ok, got %v %+v", err, res)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestDPoPReplay(t *testing.T) {
	k, _ := mkKey(t)
	v := newV(true)
	req := DPoPRequest{Method: "POST", URL: "https://gw.example.com/v1/events", Proof: mkProof(t, k, proofOpts{claims: baseClaims("r1")})}
	if _, err := v.Verify(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(context.Background(), req); !errors.Is(err, ErrDPoPReplay) {
		t.Fatalf("want replay, got %v", err)
	}
}

func TestDPoPRequiredAndOptional(t *testing.T) {
	req := DPoPRequest{Method: "GET", URL: "https://x.example/"}
	if _, err := newV(true).Verify(context.Background(), req); !errors.Is(err, ErrDPoPRequired) {
		t.Fatal(err)
	}
	if r, err := newV(false).Verify(context.Background(), req); err != nil || r.Present {
		t.Fatal(err, r)
	}
	req.BoundJKT = "abc"
	if _, err := newV(false).Verify(context.Background(), req); !errors.Is(err, ErrDPoPKeyMismatch) {
		t.Fatal(err)
	}
}

func TestDPoPRS256AndWrongKty(t *testing.T) {
	// RS256 proof with an EC jwk must be rejected (kty/alg mismatch).
	k, _ := mkKey(t)
	p := mkProof(t, k, proofOpts{alg: "RS256", claims: baseClaims("k1")})
	_, err := newV(true).Verify(context.Background(), DPoPRequest{Proof: p, Method: "POST", URL: "https://gw.example.com/v1/events"})
	if !errors.Is(err, ErrInvalidDPoP) {
		t.Fatal(err)
	}
}

func TestMemoryReplayCacheBounded(t *testing.T) {
	c := NewMemoryReplayCache(2)
	now := dpopNow
	c.now = func() time.Time { return now }
	ctx := context.Background()
	c.CheckAndStore(ctx, "a", time.Minute)
	c.CheckAndStore(ctx, "b", time.Minute)
	if _, err := c.CheckAndStore(ctx, "c", time.Minute); !errors.Is(err, ErrReplayCacheFull) {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if ok, err := c.CheckAndStore(ctx, "c", time.Minute); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, _ := c.CheckAndStore(ctx, "c", time.Minute); ok {
		t.Fatal("replay accepted")
	}
}

func TestNormalizeHTUAndCnf(t *testing.T) {
	if _, err := NormalizeHTU("/relative"); err == nil {
		t.Fatal("want error")
	}
	hdr := b64([]byte(`{"alg":"ES256"}`))
	pl := b64([]byte(`{"cnf":{"jkt":"abc"}}`))
	if j, err := CnfJKTFromAccessToken(hdr + "." + pl + ".sig"); err != nil || j != "abc" {
		t.Fatal(j, err)
	}
}
