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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const clientAssertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

// ErrIntrospectionUnavailable means AAS could not answer (network, HTTP status, bad body or
// misconfiguration). Callers must treat it as "deny": the token is NOT known to be active.
var ErrIntrospectionUnavailable = errors.New("auth: introspection unavailable")

// IntrospectorConfig configures the optional RFC 7662 / RFC 7009 client used by L3 resources.
// The client authenticates with private_key_jwt (ES256); the key is read from PrivateKeyFile
// (PEM, PKCS#8 or SEC1, P-256) and is never part of the repository.
type IntrospectorConfig struct {
	BaseURL        string // AAS realm URL; endpoints come from its discovery document
	ClientID       string // AAS client of this workload
	PrivateKeyFile string // e.g. $AAS_INTROSPECTION_KEY_FILE
	KeyID          string // optional; default is the RFC 7638 thumbprint of the key

	PositiveTTL time.Duration // cache of active=true, capped by token exp (default 10s)
	NegativeTTL time.Duration // cache of active=false (default 30s)
	MaxEntries  int           // cache bound (default 4096)
	Timeout     time.Duration // per request (default 5s)
	HTTPClient  *http.Client
	Now         func() time.Time
}

// Introspection is the parsed RFC 7662 response. Claims holds every field AAS returned.
type Introspection struct {
	Active bool
	Claims map[string]any
}

// Principal derives the typed principal from the introspection claims (active tokens only).
func (i Introspection) Principal() Principal {
	return Principal{TenantID: stringClaim(i.Claims, "tenant_id"), Subject: stringClaim(i.Claims, "sub"),
		ClientID: stringClaim(i.Claims, "client_id"), JTI: stringClaim(i.Claims, "jti"), Scopes: scopes(i.Claims["scope"])}
}

type introCacheEntry struct {
	res     Introspection
	expires time.Time
}

// Introspector asks AAS whether a token is still active and can revoke tokens.
type Introspector struct {
	cfg    IntrospectorConfig
	client *http.Client
	key    *ecdsa.PrivateKey
	kid    string

	mu         sync.Mutex
	issuer     string
	introspect string
	revoke     string
	cache      map[[32]byte]introCacheEntry
	order      [][32]byte // insertion order for bounded eviction
}

// NewIntrospector loads the key and the discovery document. Endpoints must be same-origin as
// BaseURL so that client assertions are never sent elsewhere.
func NewIntrospector(ctx context.Context, cfg IntrospectorConfig) (*Introspector, error) {
	if cfg.BaseURL == "" || cfg.ClientID == "" || cfg.PrivateKeyFile == "" {
		return nil, errors.New("auth: introspection needs BaseURL, ClientID and PrivateKeyFile")
	}
	if cfg.PositiveTTL <= 0 {
		cfg.PositiveTTL = 10 * time.Second
	}
	if cfg.NegativeTTL <= 0 {
		cfg.NegativeTTL = 30 * time.Second
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 4096
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	key, err := loadECKey(cfg.PrivateKeyFile)
	if err != nil {
		return nil, err
	}
	in := &Introspector{cfg: cfg, client: cfg.HTTPClient, key: key, kid: cfg.KeyID, cache: map[[32]byte]introCacheEntry{}}
	if in.kid == "" {
		in.kid = thumbprint(&key.PublicKey)
	}
	if err := in.discover(ctx); err != nil {
		return nil, err
	}
	return in, nil
}

func loadECKey(path string) (*ecdsa.PrivateKey, error) {
	raw, err := os.ReadFile(path) // #nosec G304 -- path comes from operator configuration, not request input
	if err != nil {
		return nil, fmt.Errorf("auth: read introspection key: %w", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("auth: introspection key is not PEM")
	}
	var key *ecdsa.PrivateKey
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		key, _ = k.(*ecdsa.PrivateKey)
	} else if k, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		key = k
	}
	if key == nil || key.Curve != elliptic.P256() {
		return nil, errors.New("auth: introspection key must be an EC P-256 private key")
	}
	return key, nil
}

func thumbprint(pub *ecdsa.PublicKey) string {
	x, y := pub.X.FillBytes(make([]byte, 32)), pub.Y.FillBytes(make([]byte, 32))
	canon := fmt.Sprintf(`{"crv":"P-256","kty":"EC","x":"%s","y":"%s"}`, b64u(x), b64u(y))
	sum := sha256.Sum256([]byte(canon))
	return b64u(sum[:])
}

func b64u(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (in *Introspector) discover(ctx context.Context) error {
	base, err := url.ParseRequestURI(in.cfg.BaseURL)
	if err != nil || base.Host == "" {
		return errors.New("auth: invalid BaseURL")
	}
	var d struct {
		Issuer        string `json:"issuer"`
		Introspection string `json:"introspection_endpoint"`
		Revocation    string `json:"revocation_endpoint"`
	}
	if err := in.getJSON(ctx, strings.TrimRight(in.cfg.BaseURL, "/")+"/.well-known/openid-configuration", &d); err != nil {
		return fmt.Errorf("auth introspection discovery: %w", err)
	}
	sameOrigin := func(s string) bool {
		u, err := url.Parse(s)
		return err == nil && u.IsAbs() && u.Scheme == base.Scheme && u.Host == base.Host
	}
	if d.Issuer == "" || !sameOrigin(d.Introspection) {
		return errors.New("auth introspection discovery: introspection_endpoint missing or not same-origin")
	}
	if d.Revocation != "" && !sameOrigin(d.Revocation) {
		return errors.New("auth introspection discovery: revocation_endpoint not same-origin")
	}
	in.issuer, in.introspect, in.revoke = d.Issuer, d.Introspection, d.Revocation
	return nil
}

func (in *Introspector) getJSON(ctx context.Context, endpoint string, dst any) error {
	ctx, cancel := context.WithTimeout(ctx, in.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	c := *in.client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(dst)
}

// assertion builds a one-minute private_key_jwt (always with iat; AAS rejects exp-iat > 60s).
func (in *Introspector) assertion() (string, error) {
	now := in.cfg.Now().Unix()
	head, _ := json.Marshal(map[string]string{"alg": "ES256", "typ": "JWT", "kid": in.kid})
	jti := make([]byte, 16)
	if _, err := rand.Read(jti); err != nil {
		return "", err
	}
	body, _ := json.Marshal(map[string]any{"iss": in.cfg.ClientID, "sub": in.cfg.ClientID, "aud": in.issuer,
		"jti": b64u(jti), "iat": now, "exp": now + 60})
	signed := b64u(head) + "." + b64u(body)
	digest := sha256.Sum256([]byte(signed))
	r, s, err := ecdsa.Sign(rand.Reader, in.key, digest[:])
	if err != nil {
		return "", err
	}
	sig := append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	return signed + "." + b64u(sig), nil
}

func (in *Introspector) post(ctx context.Context, endpoint, token string) (int, []byte, error) {
	assertion, err := in.assertion()
	if err != nil {
		return 0, nil, err
	}
	form := url.Values{"token": {token}, "token_type_hint": {"access_token"}, "client_id": {in.cfg.ClientID},
		"client_assertion_type": {clientAssertionType}, "client_assertion": {assertion}}
	ctx, cancel := context.WithTimeout(ctx, in.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := *in.client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := c.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, b, err
}

// Introspect returns whether token is active. Any failure returns ErrIntrospectionUnavailable
// (and is never cached), so callers fail closed. Results are cached by token hash.
func (in *Introspector) Introspect(ctx context.Context, token string) (Introspection, error) {
	if token == "" {
		return Introspection{}, nil
	}
	key := sha256.Sum256([]byte(token))
	now := in.cfg.Now()
	in.mu.Lock()
	if e, ok := in.cache[key]; ok && now.Before(e.expires) {
		in.mu.Unlock()
		return e.res, nil
	}
	in.mu.Unlock()
	code, body, err := in.post(ctx, in.introspect, token)
	if err != nil || code != http.StatusOK {
		return Introspection{}, ErrIntrospectionUnavailable
	}
	var claims map[string]any
	if json.Unmarshal(body, &claims) != nil || claims == nil {
		return Introspection{}, ErrIntrospectionUnavailable
	}
	active, ok := claims["active"].(bool)
	if !ok {
		return Introspection{}, ErrIntrospectionUnavailable
	}
	res := Introspection{Active: active, Claims: claims}
	if !active {
		res.Claims = map[string]any{"active": false} // nothing from an inactive token is trusted
	}
	ttl := in.cfg.NegativeTTL
	if active {
		ttl = in.cfg.PositiveTTL
		if exp, ok := numericDate(claims["exp"]); ok {
			if left := exp.Sub(now); left < ttl {
				ttl = left
			}
		}
	}
	if ttl > 0 {
		in.store(key, res, now.Add(ttl), now)
	}
	return res, nil
}

func (in *Introspector) store(key [32]byte, res Introspection, expires, now time.Time) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if _, ok := in.cache[key]; !ok {
		for len(in.cache) >= in.cfg.MaxEntries && len(in.order) > 0 {
			delete(in.cache, in.order[0])
			in.order = in.order[1:]
		}
		in.order = append(in.order, key)
	}
	in.cache[key] = introCacheEntry{res: res, expires: expires}
}

// Revoke revokes token at AAS (RFC 7009) and drops it from the cache. AAS only revokes tokens
// issued to the authenticating client. A failure returns ErrIntrospectionUnavailable.
func (in *Introspector) Revoke(ctx context.Context, token string) error {
	if in.revoke == "" {
		return errors.New("auth: AAS does not publish a revocation_endpoint")
	}
	code, _, err := in.post(ctx, in.revoke, token)
	if err != nil || code != http.StatusOK {
		return ErrIntrospectionUnavailable
	}
	key := sha256.Sum256([]byte(token))
	in.mu.Lock()
	delete(in.cache, key)
	in.mu.Unlock()
	return nil
}

// RequireActive is an opt-in middleware for L3 routes. It must run after Verifier.Middleware: the
// already verified principal is cross-checked with the introspection answer. active=false or a
// sub/jti/client_id/tenant_id mismatch gives 401; an AAS failure gives 503 (fail closed).
func RequireActive(in *Introspector, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		parts := strings.Fields(r.Header.Get("Authorization"))
		if !ok || len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		res, err := in.Introspect(r.Context(), parts[1])
		if err != nil {
			http.Error(w, "token status unavailable", http.StatusServiceUnavailable)
			return
		}
		if !res.Active || !sameIdentity(p, res.Principal()) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sameIdentity requires every identity field AAS returned to equal the verified one.
func sameIdentity(verified, introspected Principal) bool {
	eq := func(a, b string) bool { return b == "" || a == b }
	return eq(verified.Subject, introspected.Subject) && eq(verified.JTI, introspected.JTI) &&
		eq(verified.ClientID, introspected.ClientID) && eq(verified.TenantID, introspected.TenantID)
}
