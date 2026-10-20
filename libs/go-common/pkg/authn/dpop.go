package authn

// DPoP (RFC 9449) proof-of-possession validation.
//
// DPoPVerifier validates the "DPoP" request header proof JWT. It is
// independent of Verifier (access-token validation): callers first verify
// the access token with Verifier.Verify, extract its cnf.jkt via
// CnfJKTFromAccessToken, and then pass it here as BoundJKT.

import (
	"context"
	"crypto"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
)

var (
	// ErrDPoPRequired means no DPoP proof was supplied but one is required.
	ErrDPoPRequired = errors.New("authn: DPoP proof required")
	// ErrInvalidDPoP means the DPoP proof failed validation (fail closed).
	ErrInvalidDPoP = errors.New("authn: invalid DPoP proof")
	// ErrDPoPReplay means the proof's jti has already been seen.
	ErrDPoPReplay = errors.New("authn: DPoP proof replayed")
	// ErrDPoPKeyMismatch means the proof key's thumbprint does not match
	// the access token's cnf.jkt (or the token is bound but no proof given).
	ErrDPoPKeyMismatch = errors.New("authn: DPoP key does not match token binding")
)

// ReplayCache records proof jti values. Implementations must be safe for
// concurrent use; a Redis-backed implementation (SET NX PX) can replace the
// in-memory one so that multiple replicas share replay state.
type ReplayCache interface {
	// CheckAndStore atomically records jti for ttl. It returns true if the
	// jti was not seen before (fresh) and false if it is a replay.
	CheckAndStore(ctx context.Context, jti string, ttl time.Duration) (bool, error)
}

// MemoryReplayCache is a bounded in-memory ReplayCache. When full, expired
// entries are purged first; if still full it fails closed (error) rather
// than evicting live entries, which would open a replay window.
type MemoryReplayCache struct {
	mu      sync.Mutex
	max     int
	entries map[string]time.Time
	now     func() time.Time
}

// ErrReplayCacheFull is returned by MemoryReplayCache when at capacity.
var ErrReplayCacheFull = errors.New("authn: DPoP replay cache full")

// NewMemoryReplayCache creates a cache holding at most maxEntries (default
// 100000 if <= 0).
func NewMemoryReplayCache(maxEntries int) *MemoryReplayCache {
	if maxEntries <= 0 {
		maxEntries = 100000
	}
	return &MemoryReplayCache{max: maxEntries, entries: map[string]time.Time{}, now: time.Now}
}

// CheckAndStore implements ReplayCache.
func (c *MemoryReplayCache) CheckAndStore(_ context.Context, jti string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if exp, ok := c.entries[jti]; ok && exp.After(now) {
		return false, nil
	}
	if len(c.entries) >= c.max {
		for k, exp := range c.entries {
			if !exp.After(now) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= c.max {
			return false, ErrReplayCacheFull
		}
	}
	c.entries[jti] = now.Add(ttl)
	return true, nil
}

// DPoPConfig configures a DPoPVerifier.
type DPoPConfig struct {
	// Required makes a missing proof an error (use for L2+ tokens). When
	// false, a missing proof is accepted only if the token is not bound
	// (BoundJKT empty).
	Required bool
	// MaxAge is how old iat may be. Default 60s.
	MaxAge time.Duration
	// Skew is tolerated clock skew (future iat and extra age). Default 5s.
	Skew time.Duration
	// Replay is the jti store. Default: NewMemoryReplayCache(0).
	Replay ReplayCache
	// Now overrides the clock (tests).
	Now func() time.Time
}

// DPoPVerifier validates DPoP proofs. Safe for concurrent use.
type DPoPVerifier struct {
	cfg DPoPConfig
}

// NewDPoPVerifier applies defaults and returns a verifier.
func NewDPoPVerifier(cfg DPoPConfig) *DPoPVerifier {
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 60 * time.Second
	}
	if cfg.Skew <= 0 {
		cfg.Skew = 5 * time.Second
	}
	if cfg.Replay == nil {
		cfg.Replay = NewMemoryReplayCache(0)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &DPoPVerifier{cfg: cfg}
}

// DPoPRequest describes the HTTP request being authenticated.
type DPoPRequest struct {
	// Proof is the DPoP header value ("" if absent).
	Proof string
	// Method is the HTTP method (compared case-sensitively per RFC 9110).
	Method string
	// URL is the absolute request URL as seen by the client (scheme+host
	// +path; query and fragment are ignored).
	URL string
	// AccessToken is the raw access token; if non-empty the proof must
	// carry a matching ath.
	AccessToken string
	// BoundJKT is the verified access token's cnf.jkt ("" if unbound).
	BoundJKT string
}

// DPoPResult is returned for a valid proof.
type DPoPResult struct {
	// JKT is the RFC 7638 SHA-256 thumbprint of the proof key.
	JKT string
	// JTI is the proof's unique id.
	JTI string
	// Present is false when no proof was supplied and none was required.
	Present bool
}

// Verify validates the proof for req.
func (v *DPoPVerifier) Verify(ctx context.Context, req DPoPRequest) (*DPoPResult, error) {
	if strings.TrimSpace(req.Proof) == "" {
		if v.cfg.Required {
			return nil, ErrDPoPRequired
		}
		if req.BoundJKT != "" {
			return nil, fmt.Errorf("%w: token is DPoP-bound but no proof supplied", ErrDPoPKeyMismatch)
		}
		return &DPoPResult{}, nil
	}
	bad := func(format string, a ...any) (*DPoPResult, error) {
		return nil, fmt.Errorf("%w: "+format, append([]any{ErrInvalidDPoP}, a...)...)
	}

	parts := strings.Split(req.Proof, ".")
	if len(parts) != 3 {
		return bad("not a compact JWS")
	}
	hdrBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return bad("header encoding")
	}
	var hdr struct {
		Typ string          `json:"typ"`
		Alg string          `json:"alg"`
		JWK json.RawMessage `json:"jwk"`
	}
	if err := json.Unmarshal(hdrBytes, &hdr); err != nil {
		return bad("header json")
	}
	if hdr.Typ != "dpop+jwt" {
		return bad("typ must be dpop+jwt")
	}
	var alg jwa.SignatureAlgorithm
	var wantKty string
	switch hdr.Alg {
	case "ES256":
		alg, wantKty = jwa.ES256, "EC"
	case "RS256":
		alg, wantKty = jwa.RS256, "RSA"
	default:
		return bad("alg %q not allowed", hdr.Alg)
	}
	if len(hdr.JWK) == 0 {
		return bad("missing jwk header")
	}
	var rawKey map[string]any
	if err := json.Unmarshal(hdr.JWK, &rawKey); err != nil {
		return bad("jwk json")
	}
	for _, priv := range []string{"d", "p", "q", "dp", "dq", "qi", "oth", "k"} {
		if _, ok := rawKey[priv]; ok {
			return bad("jwk must be a public key")
		}
	}
	if rawKey["kty"] != wantKty {
		return bad("jwk kty does not match alg")
	}
	key, err := jwk.ParseKey(hdr.JWK)
	if err != nil {
		return bad("jwk parse: %v", err)
	}
	if _, err := key.PublicKey(); err != nil {
		return bad("jwk not a usable public key")
	}
	if key.KeyType() == "EC" {
		if ec, ok := key.(jwk.ECDSAPublicKey); !ok || ec.Crv() != jwa.P256 {
			return bad("EC key must be P-256")
		}
	}

	payload, err := jws.Verify([]byte(req.Proof), jws.WithKey(alg, key))
	if err != nil {
		return bad("signature: %v", err)
	}
	var cl struct {
		JTI string   `json:"jti"`
		HTM string   `json:"htm"`
		HTU string   `json:"htu"`
		IAT *float64 `json:"iat"`
		ATH string   `json:"ath"`
	}
	if err := json.Unmarshal(payload, &cl); err != nil {
		return bad("claims json")
	}
	if cl.JTI == "" || cl.HTM == "" || cl.HTU == "" || cl.IAT == nil {
		return bad("missing jti/htm/htu/iat")
	}
	if cl.HTM != req.Method {
		return bad("htm mismatch")
	}
	wantHTU, err := NormalizeHTU(req.URL)
	if err != nil {
		return bad("request url: %v", err)
	}
	gotHTU, err := NormalizeHTU(cl.HTU)
	if err != nil || gotHTU != wantHTU {
		return bad("htu mismatch")
	}
	now := v.cfg.Now()
	iat := time.Unix(int64(*cl.IAT), 0)
	if iat.After(now.Add(v.cfg.Skew)) {
		return bad("iat in the future")
	}
	if now.Sub(iat) > v.cfg.MaxAge+v.cfg.Skew {
		return bad("proof expired")
	}
	if req.AccessToken != "" {
		sum := sha256.Sum256([]byte(req.AccessToken))
		want := base64.RawURLEncoding.EncodeToString(sum[:])
		if subtle.ConstantTimeCompare([]byte(cl.ATH), []byte(want)) != 1 {
			return bad("ath mismatch")
		}
	}
	tp, err := key.Thumbprint(crypto.SHA256)
	if err != nil {
		return bad("thumbprint")
	}
	jkt := base64.RawURLEncoding.EncodeToString(tp)
	if req.BoundJKT != "" && subtle.ConstantTimeCompare([]byte(jkt), []byte(req.BoundJKT)) != 1 {
		return nil, ErrDPoPKeyMismatch
	}
	// Replay check last so rejected proofs do not consume cache slots.
	ttl := v.cfg.MaxAge + 2*v.cfg.Skew
	fresh, err := v.cfg.Replay.CheckAndStore(ctx, cl.JTI, ttl)
	if err != nil {
		return nil, fmt.Errorf("authn: DPoP replay store: %w", err)
	}
	if !fresh {
		return nil, ErrDPoPReplay
	}
	return &DPoPResult{JKT: jkt, JTI: cl.JTI, Present: true}, nil
}

// NormalizeHTU returns scheme://host[:port]/path with lowercase scheme and
// host, default ports removed, and query/fragment dropped.
func NormalizeHTU(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	scheme := strings.ToLower(u.Scheme)
	if (scheme != "https" && scheme != "http") || u.Hostname() == "" {
		return "", errors.New("absolute http(s) URL required")
	}
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		port = ""
	}
	if port != "" {
		host += ":" + port
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	return scheme + "://" + host + path, nil
}

// CnfJKTFromAccessToken extracts cnf.jkt from a JWT access token WITHOUT
// verifying it. Only call this on a token already accepted by Verifier.Verify.
func CnfJKTFromAccessToken(raw string) (string, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return "", ErrInvalidToken
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ErrInvalidToken
	}
	var c struct {
		Cnf struct {
			JKT string `json:"jkt"`
		} `json:"cnf"`
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return "", ErrInvalidToken
	}
	return c.Cnf.JKT, nil
}
