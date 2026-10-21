package bff

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const backchannelEvent = "http://schemas.openid.net/event/backchannel-logout"

// jwksCache holds the AAS signing keys. An unknown kid triggers one refetch, at most once per minRefetch,
// so a forged kid cannot turn the endpoint into a request amplifier.
type jwksCache struct {
	mu      sync.Mutex
	keys    map[string]crypto.PublicKey
	fetched time.Time
}

const minRefetch = 30 * time.Second

func (c *oidcClient) signingKey(kid string) (crypto.PublicKey, error) {
	j := &c.jwks
	j.mu.Lock()
	defer j.mu.Unlock()
	if k, ok := j.keys[kid]; ok {
		return k, nil
	}
	if c.ep.JWKS == "" {
		return nil, errors.New("AAS publishes no jwks_uri")
	}
	if !j.fetched.IsZero() && c.now().Sub(j.fetched) < minRefetch {
		return nil, errors.New("unknown kid")
	}
	j.fetched = c.now()
	var doc struct {
		Keys []struct {
			Kid, Kty, Crv, N, E, X, Y, Use, Alg string
		} `json:"keys"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.getJSON(ctx, c.ep.JWKS, &doc); err != nil {
		return nil, fmt.Errorf("jwks: %w", err)
	}
	keys := map[string]crypto.PublicKey{}
	for _, k := range doc.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		switch k.Kty {
		case "RSA":
			n, e := b64Int(k.N), b64Int(k.E)
			if n != nil && e != nil && e.IsInt64() {
				keys[k.Kid] = &rsa.PublicKey{N: n, E: int(e.Int64())}
			}
		case "EC":
			if k.Crv == "P-256" {
				x, y := b64Int(k.X), b64Int(k.Y)
				if x != nil && y != nil {
					pk := &ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}
					if pk.Curve.IsOnCurve(x, y) {
						keys[k.Kid] = pk
					}
				}
			}
		}
	}
	j.keys = keys
	if k, ok := keys[kid]; ok {
		return k, nil
	}
	return nil, errors.New("unknown kid")
}

func b64Int(s string) *big.Int {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) == 0 {
		return nil
	}
	return new(big.Int).SetBytes(b)
}

// verifyLogoutToken validates an OIDC Back-Channel Logout token (OpenID Back-Channel Logout 1.0, 2.6):
// signature by an AAS key, iss, aud, iat, events, no nonce, sub or sid, and a jti not seen before.
func (c *oidcClient) verifyLogoutToken(tok string) (sub, sid string, err error) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 || len(tok) > 16<<10 {
		return "", "", errors.New("malformed")
	}
	var hdr struct{ Alg, Kid string }
	var cl struct {
		Iss    string          `json:"iss"`
		Sub    string          `json:"sub"`
		SID    string          `json:"sid"`
		Aud    any             `json:"aud"`
		Iat    int64           `json:"iat"`
		Exp    int64           `json:"exp"`
		Jti    string          `json:"jti"`
		Events map[string]any  `json:"events"`
		Nonce  json.RawMessage `json:"nonce"`
	}
	for i, dst := range []any{&hdr, &cl} {
		raw, e := base64.RawURLEncoding.DecodeString(parts[i])
		if e != nil || json.Unmarshal(raw, dst) != nil {
			return "", "", errors.New("malformed")
		}
	}
	sig, e := base64.RawURLEncoding.DecodeString(parts[2])
	if e != nil {
		return "", "", errors.New("malformed")
	}
	pub, e := c.signingKey(hdr.Kid)
	if e != nil {
		return "", "", e
	}
	h := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	switch k := pub.(type) {
	case *rsa.PublicKey:
		if hdr.Alg != "RS256" || rsa.VerifyPKCS1v15(k, crypto.SHA256, h[:], sig) != nil {
			return "", "", errors.New("bad signature")
		}
	case *ecdsa.PublicKey:
		if hdr.Alg != "ES256" || len(sig) != 64 || !ecdsa.Verify(k, h[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
			return "", "", errors.New("bad signature")
		}
	default:
		return "", "", errors.New("unsupported key")
	}
	issuer := c.ep.Issuer
	if issuer == "" {
		issuer = strings.TrimSuffix(c.cfg.BaseURL, "/")
	}
	aud := false
	switch a := cl.Aud.(type) {
	case string:
		aud = a == c.cfg.ClientID
	case []any:
		for _, x := range a {
			aud = aud || x == c.cfg.ClientID
		}
	}
	now := c.now()
	_, hasEvent := cl.Events[backchannelEvent]
	switch {
	case cl.Iss != issuer:
		return "", "", errors.New("issuer mismatch")
	case !aud:
		return "", "", errors.New("audience mismatch")
	case !hasEvent:
		return "", "", errors.New("not a logout token")
	case len(cl.Nonce) != 0:
		return "", "", errors.New("logout token must not carry a nonce")
	case cl.Sub == "" && cl.SID == "":
		return "", "", errors.New("neither sub nor sid")
	case cl.Jti == "":
		return "", "", errors.New("no jti")
	case cl.Iat == 0 || time.Unix(cl.Iat, 0).After(now.Add(time.Minute)) || time.Unix(cl.Iat, 0).Before(now.Add(-10*time.Minute)):
		return "", "", errors.New("iat out of range")
	case cl.Exp != 0 && !time.Unix(cl.Exp, 0).After(now):
		return "", "", errors.New("expired")
	}
	if !c.seen.add(cl.Jti, now.Add(10*time.Minute), now) {
		return "", "", errors.New("replayed jti")
	}
	return cl.Sub, cl.SID, nil
}

// jtiCache remembers logout token ids until the token can no longer be accepted (bounded).
type jtiCache struct {
	mu sync.Mutex
	m  map[string]time.Time
}

func (j *jtiCache) add(id string, until, now time.Time) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.m == nil {
		j.m = map[string]time.Time{}
	}
	for k, t := range j.m {
		if now.After(t) {
			delete(j.m, k)
		}
	}
	if _, dup := j.m[id]; dup || len(j.m) >= 10000 {
		return false
	}
	j.m[id] = until
	return true
}

// backchannelLogout ends the sessions named by a verified logout token: by sid, or all of a subject's when only sub is given.
func (s *Server) backchannelLogout(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	b, err := io.ReadAll(r.Body)
	if err != nil {
		jsonErr(w, http.StatusBadRequest, "invalid_request", "body too large")
		return
	}
	form, err := url.ParseQuery(string(b))
	if err != nil || form.Get("logout_token") == "" {
		jsonErr(w, http.StatusBadRequest, "invalid_request", "logout_token is required")
		return
	}
	sub, sid, err := s.oc.verifyLogoutToken(form.Get("logout_token"))
	if err != nil {
		s.o.Logger.Printf("back-channel logout rejected: %v", err)
		jsonErr(w, http.StatusBadRequest, "invalid_request", "logout token rejected")
		return
	}
	n := 0
	s.mu.Lock()
	for id, se := range s.sessions {
		if (sid != "" && se.sid == sid) || (sid == "" && se.sub == sub) {
			delete(s.sessions, id)
			n++
		}
	}
	s.mu.Unlock()
	s.o.Logger.Printf("back-channel logout: %d session(s) ended", n)
	w.WriteHeader(http.StatusOK)
}
