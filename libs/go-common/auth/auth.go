// Package auth provides shared fail-closed AAS access-token verification.
package auth

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

var (
	// ErrUnauthenticated is returned for missing or invalid credentials.
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrForbidden is returned when a valid principal lacks a required scope.
	ErrForbidden = errors.New("forbidden")
)

// Principal is the only identity object downstream handlers should use.
// TenantID is sourced exclusively from a verified token claim.
type Principal struct {
	TenantID string
	Subject  string
	ClientID string
	JTI      string
	Scopes   []string
}

// HasScope reports whether p includes scope.
func (p Principal) HasScope(scope string) bool {
	for _, got := range p.Scopes {
		if got == scope {
			return true
		}
	}
	return false
}

// Config defines the AAS resource-server trust boundary.
type Config struct {
	BaseURL     string
	Issuer      string
	Audience    string
	Environment string
	AllowedAZP  []string
	TokenType   string

	JWKSCacheTTL time.Duration
	ClockSkew    time.Duration
	HTTPClient   *http.Client
	Now          func() time.Time
}

type discovery struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}

// Verifier verifies AAS-issued Bearer access tokens and maintains a bounded
// in-memory JWKS cache. It does not accept DPoP tokens: support requires a
// shared replay store and proof validation, so unsupported credentials fail closed.
type Verifier struct {
	cfg     Config
	client  *http.Client
	issuer  string
	jwksURI string

	mu        sync.Mutex
	keys      map[string]crypto.PublicKey
	expiresAt time.Time
}

// NewVerifier loads OIDC discovery and the initial JWKS. Discovery issuer must
// match Config.Issuer exactly when it is configured.
func NewVerifier(ctx context.Context, cfg Config) (*Verifier, error) {
	if cfg.BaseURL == "" || cfg.Audience == "" || cfg.Environment == "" {
		return nil, errors.New("auth: BaseURL, Audience, and Environment are required")
	}
	base, err := url.ParseRequestURI(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("auth: invalid BaseURL: %w", err)
	}
	if base.Scheme == "" || base.Host == "" || (base.Scheme != "https" && base.Scheme != "http") {
		return nil, errors.New("auth: BaseURL must be an absolute HTTP(S) URL")
	}
	if cfg.JWKSCacheTTL <= 0 {
		cfg.JWKSCacheTTL = 5 * time.Minute
	}
	if cfg.ClockSkew < 0 {
		return nil, errors.New("auth: ClockSkew must not be negative")
	}
	if cfg.TokenType == "" {
		cfg.TokenType = "at+jwt"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	v := &Verifier{cfg: cfg, client: cfg.HTTPClient}
	if err := v.refresh(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

// Authenticate validates a Bearer JWT and returns a typed principal.
func (v *Verifier) Authenticate(r *http.Request) (Principal, error) {
	authorization := r.Header.Get("Authorization")
	parts := strings.Fields(authorization)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return Principal{}, ErrUnauthenticated
	}
	return v.Verify(r.Context(), parts[1])
}

// Verify validates token independently from HTTP middleware.
func (v *Verifier) Verify(ctx context.Context, token string) (Principal, error) {
	header, payload, signature, signed, err := splitJWT(token)
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	alg, _ := header["alg"].(string)
	kid, _ := header["kid"].(string)
	if kid == "" || (alg != "RS256" && alg != "ES256") {
		return Principal{}, ErrUnauthenticated
	}
	key, err := v.key(ctx, kid)
	if err != nil || !verifySignature(alg, key, signed, signature) {
		return Principal{}, ErrUnauthenticated
	}
	return v.principal(payload)
}

func (v *Verifier) principal(claims map[string]any) (Principal, error) {
	now := v.cfg.Now().UTC()
	issuer, ok := claims["iss"].(string)
	if !ok || issuer != v.issuer || !audienceContains(claims["aud"], v.cfg.Audience) {
		return Principal{}, ErrUnauthenticated
	}
	if stringClaim(claims, "env") != v.cfg.Environment || stringClaim(claims, "typ") != v.cfg.TokenType {
		return Principal{}, ErrUnauthenticated
	}
	if len(v.cfg.AllowedAZP) > 0 && !contains(v.cfg.AllowedAZP, stringClaim(claims, "azp")) {
		return Principal{}, ErrUnauthenticated
	}
	exp, ok := numericDate(claims["exp"])
	if !ok || !now.Before(exp.Add(v.cfg.ClockSkew)) {
		return Principal{}, ErrUnauthenticated
	}
	// AAS (Keycloak) access tokens carry no nbf; validate it only when present.
	if raw, present := claims["nbf"]; present {
		if nbf, ok := numericDate(raw); !ok || now.Add(v.cfg.ClockSkew).Before(nbf) {
			return Principal{}, ErrUnauthenticated
		}
	}
	if iat, ok := numericDate(claims["iat"]); !ok || iat.After(now.Add(v.cfg.ClockSkew)) {
		return Principal{}, ErrUnauthenticated
	}
	p := Principal{TenantID: stringClaim(claims, "tenant_id"), Subject: stringClaim(claims, "sub"), ClientID: stringClaim(claims, "client_id"), JTI: stringClaim(claims, "jti"), Scopes: scopes(claims["scope"])}
	if !isUUID(p.TenantID) || p.Subject == "" || p.JTI == "" {
		return Principal{}, ErrUnauthenticated
	}
	return p, nil
}

func (v *Verifier) key(ctx context.Context, kid string) (crypto.PublicKey, error) {
	v.mu.Lock()
	key := v.keys[kid]
	fresh := v.cfg.Now().Before(v.expiresAt)
	v.mu.Unlock()
	if key != nil && fresh {
		return key, nil
	}
	if err := v.refresh(ctx); err != nil {
		return nil, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if key := v.keys[kid]; key != nil {
		return key, nil
	}
	return nil, errors.New("auth: unknown kid")
}

func (v *Verifier) refresh(ctx context.Context) error {
	docURL := strings.TrimRight(v.cfg.BaseURL, "/") + "/.well-known/openid-configuration"
	var d discovery
	if err := v.getJSON(ctx, docURL, &d); err != nil {
		return fmt.Errorf("auth discovery: %w", err)
	}
	if d.Issuer == "" || d.JWKSURI == "" || (v.cfg.Issuer != "" && d.Issuer != v.cfg.Issuer) {
		return errors.New("auth discovery: issuer or jwks_uri is invalid")
	}
	base, _ := url.Parse(v.cfg.BaseURL)
	jwks, err := url.Parse(d.JWKSURI)
	if err != nil || !jwks.IsAbs() || jwks.Scheme != base.Scheme || jwks.Host != base.Host {
		return errors.New("auth discovery: jwks_uri must be an absolute same-origin URL")
	}
	var document struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := v.getJSON(ctx, d.JWKSURI, &document); err != nil {
		return fmt.Errorf("auth jwks: %w", err)
	}
	keys := make(map[string]crypto.PublicKey, len(document.Keys))
	for _, raw := range document.Keys {
		kid, key, err := parseJWK(raw)
		if err == nil && kid != "" && key != nil {
			keys[kid] = key
		}
	}
	if len(keys) == 0 {
		return errors.New("auth jwks: no usable signing keys")
	}
	v.mu.Lock()
	v.issuer, v.jwksURI, v.keys = d.Issuer, d.JWKSURI, keys
	v.expiresAt = v.cfg.Now().Add(v.cfg.JWKSCacheTTL)
	v.mu.Unlock()
	return nil
}

func (v *Verifier) getJSON(ctx context.Context, endpoint string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	client := *v.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(dst)
}

func splitJWT(token string) (map[string]any, map[string]any, []byte, []byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, nil, nil, nil, errors.New("not a compact JWT")
	}
	decode := func(s string, dst any) error {
		b, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil {
			return err
		}
		return json.Unmarshal(b, dst)
	}
	var header, payload map[string]any
	if err := decode(parts[0], &header); err != nil {
		return nil, nil, nil, nil, err
	}
	if err := decode(parts[1], &payload); err != nil {
		return nil, nil, nil, nil, err
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return header, payload, sig, []byte(parts[0] + "." + parts[1]), nil
}

func verifySignature(alg string, key crypto.PublicKey, signed, signature []byte) bool {
	digest := sha256.Sum256(signed)
	switch k := key.(type) {
	case *rsa.PublicKey:
		return alg == "RS256" && rsa.VerifyPKCS1v15(k, crypto.SHA256, digest[:], signature) == nil
	case *ecdsa.PublicKey:
		if alg != "ES256" || len(signature) != 64 {
			return false
		}
		return ecdsa.Verify(k, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:]))
	default:
		return false
	}
}

func parseJWK(raw json.RawMessage) (string, crypto.PublicKey, error) {
	var jwk struct{ KID, KTY, N, E, Crv, X, Y string }
	if err := json.Unmarshal(raw, &jwk); err != nil {
		return "", nil, err
	}
	decode := func(v string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(v) }
	switch jwk.KTY {
	case "RSA":
		n, err := decode(jwk.N)
		if err != nil {
			return "", nil, err
		}
		e, err := decode(jwk.E)
		if err != nil {
			return "", nil, err
		}
		if len(n) == 0 || len(e) == 0 {
			return "", nil, errors.New("invalid RSA JWK")
		}
		ei := 0
		for _, b := range e {
			ei = ei<<8 | int(b)
		}
		return jwk.KID, &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: ei}, nil
	case "EC":
		if jwk.Crv != "P-256" {
			return "", nil, errors.New("unsupported EC curve")
		}
		x, err := decode(jwk.X)
		if err != nil {
			return "", nil, err
		}
		y, err := decode(jwk.Y)
		if err != nil {
			return "", nil, err
		}
		curve := elliptic.P256()
		pointX, pointY := new(big.Int).SetBytes(x), new(big.Int).SetBytes(y)
		if !curve.IsOnCurve(pointX, pointY) {
			return "", nil, errors.New("invalid EC point")
		}
		return jwk.KID, &ecdsa.PublicKey{Curve: curve, X: pointX, Y: pointY}, nil
	default:
		return "", nil, errors.New("unsupported JWK type")
	}
}

func numericDate(v any) (time.Time, bool) {
	f, ok := v.(float64)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(int64(f), 0).UTC(), true
}
func stringClaim(m map[string]any, key string) string { s, _ := m[key].(string); return s }
func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
func audienceContains(v any, want string) bool {
	if s, ok := v.(string); ok {
		return s == want
	}
	values, ok := v.([]any)
	if !ok {
		return false
	}
	for _, value := range values {
		if s, ok := value.(string); ok && s == want {
			return true
		}
	}
	return false
}
func scopes(v any) []string {
	if s, ok := v.(string); ok {
		return strings.Fields(s)
	}
	if values, ok := v.([]any); ok {
		out := make([]string, 0, len(values))
		for _, value := range values {
			if s, ok := value.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func isUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return false
			}
			continue
		}
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}
