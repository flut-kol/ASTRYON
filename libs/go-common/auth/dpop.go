package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ReplayStore remembers DPoP proof jti values. First reports whether key was new; an error means the
// verdict is unknown and the proof is rejected (fail closed). Multi-replica deployments need a shared store.
type ReplayStore interface {
	First(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

// ErrReplayStoreFull is returned by MemoryReplayStore when it holds MaxEntries live proofs.
var ErrReplayStoreFull = errors.New("auth: dpop replay store full")

// MemoryReplayStore is a bounded single-process ReplayStore (tests, one replica).
type MemoryReplayStore struct {
	MaxEntries int // default 100000
	Now        func() time.Time

	mu   sync.Mutex
	seen map[string]time.Time
}

func (m *MemoryReplayStore) First(_ context.Context, key string, ttl time.Duration) (bool, error) {
	now := time.Now
	if m.Now != nil {
		now = m.Now
	}
	t := now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = map[string]time.Time{}
	}
	if exp, ok := m.seen[key]; ok && t.Before(exp) {
		return false, nil
	}
	limit := m.MaxEntries
	if limit <= 0 {
		limit = 100000
	}
	if len(m.seen) >= limit {
		for k, exp := range m.seen {
			if !t.Before(exp) {
				delete(m.seen, k)
			}
		}
		if len(m.seen) >= limit {
			return false, ErrReplayStoreFull
		}
	}
	m.seen[key] = t.Add(ttl)
	return true, nil
}

// authenticateDPoP handles `Authorization: DPoP <token>` + one `DPoP: <proof>` header (RFC 9449).
// The access token is verified first, then the proof bound to it.
func (v *Verifier) authenticateDPoP(r *http.Request, token string) (Principal, error) {
	proofs := r.Header.Values("DPoP")
	if v.cfg.ReplayStore == nil || v.cfg.PublicBaseURL == "" || len(proofs) != 1 {
		return Principal{}, ErrUnauthenticated
	}
	p, err := v.verify(r.Context(), token, true)
	if err != nil {
		return Principal{}, err
	}
	if err := v.checkProof(r, proofs[0], token, p.Confirmation); err != nil {
		return Principal{}, ErrUnauthenticated
	}
	return p, nil
}

func (v *Verifier) checkProof(r *http.Request, proof, token, jkt string) error {
	header, payload, sig, signed, err := splitJWT(proof)
	if err != nil {
		return err
	}
	if header["typ"] != "dpop+jwt" || header["alg"] != "ES256" {
		return errors.New("dpop: bad header")
	}
	jwkRaw, err := json.Marshal(header["jwk"])
	if err != nil {
		return err
	}
	var jwk map[string]any
	if json.Unmarshal(jwkRaw, &jwk) != nil || jwk["d"] != nil || jwk["k"] != nil {
		return errors.New("dpop: jwk must be a public key")
	}
	_, key, err := parseJWK(jwkRaw)
	if err != nil {
		return err
	}
	pub, ok := key.(*ecdsa.PublicKey)
	if !ok || !verifySignature("ES256", pub, signed, sig) {
		return errors.New("dpop: bad signature")
	}
	if subtle.ConstantTimeCompare([]byte(thumbprint(pub)), []byte(jkt)) != 1 {
		return errors.New("dpop: jkt mismatch")
	}
	if payload["htm"] != r.Method {
		return errors.New("dpop: htm mismatch")
	}
	want, err := normalizeHTU(v.cfg.PublicBaseURL + r.URL.EscapedPath())
	if err != nil {
		return err
	}
	if got, ok := payload["htu"].(string); !ok {
		return errors.New("dpop: htu missing")
	} else if n, err := normalizeHTU(got); err != nil || n != want {
		return errors.New("dpop: htu mismatch")
	}
	sum := sha256.Sum256([]byte(token))
	ath, _ := payload["ath"].(string)
	if subtle.ConstantTimeCompare([]byte(ath), []byte(base64.RawURLEncoding.EncodeToString(sum[:]))) != 1 {
		return errors.New("dpop: ath mismatch")
	}
	iat, ok := numericDate(payload["iat"])
	now := v.cfg.Now()
	if !ok || iat.Before(now.Add(-v.cfg.ProofMaxAge)) || iat.After(now.Add(v.cfg.ProofFutureSkew)) {
		return errors.New("dpop: iat out of window")
	}
	jti, _ := payload["jti"].(string)
	if jti == "" || len(jti) > 256 {
		return errors.New("dpop: jti")
	}
	id := sha256.Sum256([]byte(jti))
	fresh, err := v.cfg.ReplayStore.First(r.Context(), "dpop:"+jkt+":"+base64.RawURLEncoding.EncodeToString(id[:]), v.cfg.ReplayTTL)
	if err != nil || !fresh {
		return errors.New("dpop: replay")
	}
	return nil
}

// normalizeHTU lowercases scheme and host, drops the default port, query and fragment.
func normalizeHTU(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "", errors.New("dpop: bad htu")
	}
	host := strings.ToLower(u.Hostname())
	if port := u.Port(); port != "" && !(u.Scheme == "https" && port == "443") && !(u.Scheme == "http" && port == "80") {
		host += ":" + port
	}
	return strings.ToLower(u.Scheme) + "://" + host + u.EscapedPath(), nil
}

func confirmationJKT(claims map[string]any) string {
	cnf, _ := claims["cnf"].(map[string]any)
	return stringClaim(cnf, "jkt")
}
