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
	"strings"
	"sync"
	"time"
)

// OIDC Back-Channel Logout 1.0: AAS POSTs a signed logout_token to /auth/backchannel-logout when a user's AAS
// session ends elsewhere. The token is verified against AAS's JWKS and the matching BFF sessions are dropped.
const (
	backchannelEvent  = "http://schemas.openid.net/event/backchannel-logout"
	logoutTokenMaxAge = 5 * time.Minute // iat older than this is rejected
	logoutSkew        = time.Minute
	jtiTTL            = logoutTokenMaxAge + 2*logoutSkew + time.Minute // a replay inside the acceptance window is always caught
)

type jwks struct {
	url     string
	hc      *http.Client
	now     func() time.Time
	mu      sync.Mutex
	keys    map[string]crypto.PublicKey // kid -> key ("" for a key without kid)
	fetched time.Time
}

func (j *jwks) load(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, j.url, nil)
	res, err := j.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("jwks: status %d", res.StatusCode)
	}
	var doc struct {
		Keys []struct {
			Kty, Kid, Use, Crv, N, E, X, Y string
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&doc); err != nil {
		return err
	}
	keys := map[string]crypto.PublicKey{}
	for _, k := range doc.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		switch {
		case k.Kty == "RSA":
			n, e := bigB64(k.N), bigB64(k.E)
			if n == nil || e == nil || !e.IsInt64() || e.Int64() < 3 || e.Int64() > 1<<31-1 || n.BitLen() < 2048 {
				continue
			}
			keys[k.Kid] = &rsa.PublicKey{N: n, E: int(e.Int64())}
		case k.Kty == "EC" && k.Crv == "P-256":
			x, y := bigB64(k.X), bigB64(k.Y)
			if x == nil || y == nil {
				continue
			}
			pub := &ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}
			if _, err := pub.ECDH(); err != nil { // not on the curve
				continue
			}
			keys[k.Kid] = pub
		}
	}
	j.keys, j.fetched = keys, j.now()
	return nil
}

func bigB64(s string) *big.Int {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
	if err != nil || len(b) == 0 {
		return nil
	}
	return new(big.Int).SetBytes(b)
}

// key returns the verification key for kid. An unknown kid triggers at most one refetch per 10 s (key rotation
// without letting a forged kid turn the endpoint into a request amplifier).
func (j *jwks) key(ctx context.Context, kid string) (crypto.PublicKey, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	find := func() crypto.PublicKey {
		if k, ok := j.keys[kid]; ok {
			return k
		}
		if kid == "" && len(j.keys) == 1 {
			for _, k := range j.keys {
				return k
			}
		}
		return nil
	}
	if j.keys != nil && j.now().Sub(j.fetched) < time.Hour {
		if k := find(); k != nil {
			return k, nil
		}
	}
	if j.keys == nil || j.now().Sub(j.fetched) >= 10*time.Second {
		if err := j.load(ctx); err != nil {
			return nil, err
		}
		if k := find(); k != nil {
			return k, nil
		}
	}
	return nil, errors.New("no matching signing key")
}

type logoutClaims struct {
	Iss    string                     `json:"iss"`
	Sub    string                     `json:"sub"`
	Aud    json.RawMessage            `json:"aud"`
	Iat    *int64                     `json:"iat"`
	Exp    *int64                     `json:"exp"`
	Jti    string                     `json:"jti"`
	Sid    string                     `json:"sid"`
	Events map[string]json.RawMessage `json:"events"`
	Nonce  *json.RawMessage           `json:"nonce"`
}

// verifyLogoutToken validates signature and claims per OIDC Back-Channel Logout 1.0 section 2.4 and returns them.
// Replay protection (jti) is applied by the caller after this succeeds.
func (c *oidcClient) verifyLogoutToken(ctx context.Context, tok string) (*logoutClaims, error) {
	if c.jwks == nil {
		return nil, errors.New("no jwks_uri")
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return nil, errors.New("malformed")
	}
	var h struct{ Alg, Kid, Typ string }
	hb, err1 := base64.RawURLEncoding.DecodeString(parts[0])
	pb, err2 := base64.RawURLEncoding.DecodeString(parts[1])
	sig, err3 := base64.RawURLEncoding.DecodeString(parts[2])
	if err1 != nil || err2 != nil || err3 != nil || json.Unmarshal(hb, &h) != nil {
		return nil, errors.New("malformed")
	}
	if h.Typ != "" && !strings.EqualFold(h.Typ, "logout+jwt") && !strings.EqualFold(h.Typ, "jwt") {
		return nil, errors.New("unexpected typ")
	}
	if h.Alg != "RS256" && h.Alg != "ES256" { // never "none", never HMAC
		return nil, errors.New("unsupported alg")
	}
	key, err := c.jwks.key(ctx, h.Kid)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	switch pub := key.(type) {
	case *rsa.PublicKey:
		if h.Alg != "RS256" || rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig) != nil {
			return nil, errors.New("bad signature")
		}
	case *ecdsa.PublicKey:
		if h.Alg != "ES256" || len(sig) != 64 || !ecdsa.Verify(pub, digest[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
			return nil, errors.New("bad signature")
		}
	default:
		return nil, errors.New("bad signature")
	}
	var cl logoutClaims
	if err := json.Unmarshal(pb, &cl); err != nil {
		return nil, errors.New("malformed claims")
	}
	issuer := c.ep.Issuer
	if issuer == "" {
		issuer = strings.TrimSuffix(c.cfg.BaseURL, "/")
	}
	now := c.now()
	aud := false
	var one string
	var many []string
	if json.Unmarshal(cl.Aud, &one) == nil {
		aud = one == c.cfg.ClientID
	} else if json.Unmarshal(cl.Aud, &many) == nil {
		for _, a := range many {
			aud = aud || a == c.cfg.ClientID
		}
	}
	_, hasEvent := cl.Events[backchannelEvent]
	var ev map[string]any
	if hasEvent {
		if json.Unmarshal(cl.Events[backchannelEvent], &ev) != nil || ev == nil {
			hasEvent = false // must be a JSON object
		}
	}
	switch {
	case cl.Iss != issuer:
		return nil, errors.New("issuer mismatch")
	case !aud:
		return nil, errors.New("audience mismatch")
	case cl.Iat == nil:
		return nil, errors.New("missing iat")
	case time.Unix(*cl.Iat, 0).After(now.Add(logoutSkew)):
		return nil, errors.New("iat in the future")
	case time.Unix(*cl.Iat, 0).Before(now.Add(-logoutTokenMaxAge)):
		return nil, errors.New("iat too old")
	case cl.Exp != nil && time.Unix(*cl.Exp, 0).Before(now.Add(-logoutSkew)):
		return nil, errors.New("expired")
	case cl.Jti == "":
		return nil, errors.New("missing jti")
	case !hasEvent:
		return nil, errors.New("missing backchannel-logout event")
	case cl.Nonce != nil:
		return nil, errors.New("nonce must not be present") // a logout token must never be usable as an ID token
	case cl.Sub == "" && cl.Sid == "":
		return nil, errors.New("neither sub nor sid")
	}
	return &cl, nil
}

func (s *Server) backchannelLogout(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.oc.jwks == nil {
		jsonErr(w, http.StatusNotImplemented, "not_supported", "AAS discovery has no jwks_uri")
		return
	}
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
		jsonErr(w, http.StatusBadRequest, "invalid_request", "expected application/x-www-form-urlencoded")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := r.ParseForm(); err != nil || r.PostForm.Get("logout_token") == "" {
		jsonErr(w, http.StatusBadRequest, "invalid_request", "missing logout_token")
		return
	}
	cl, err := s.oc.verifyLogoutToken(r.Context(), r.PostForm.Get("logout_token"))
	if err != nil {
		s.o.Logger.Printf("back-channel logout rejected: %v", err)
		jsonErr(w, http.StatusBadRequest, "invalid_request", "logout_token rejected")
		return
	}
	first, err := s.store.SeenJTI(r.Context(), cl.Jti, jtiTTL)
	if err != nil {
		s.o.Logger.Printf("back-channel logout: store: %v", err)
		jsonErr(w, http.StatusServiceUnavailable, "unavailable", "session store unavailable")
		return
	}
	if !first {
		s.o.Logger.Printf("back-channel logout rejected: jti replay")
		jsonErr(w, http.StatusBadRequest, "invalid_request", "logout_token rejected")
		return
	}
	var n int
	if cl.Sid != "" { // a session id names exactly one AAS session; prefer it over the broader sub
		n, err = s.store.DeleteBySID(r.Context(), cl.Sid)
	} else {
		n, err = s.store.DeleteBySub(r.Context(), cl.Sub)
	}
	if err != nil {
		// The jti is already burned, so AAS's retry of this exact token is refused; the session then ends at its expiry.
		s.o.Logger.Printf("back-channel logout: store: %v", err)
		jsonErr(w, http.StatusServiceUnavailable, "unavailable", "session store unavailable")
		return
	}
	s.o.Logger.Printf("back-channel logout: %d session(s) ended", n)
	w.WriteHeader(http.StatusOK)
}
