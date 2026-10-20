// Package authn verifies AAS (external IdP) OAuth2/OIDC access tokens for
// ASTRYON services, per docs/architecture/authentication.md.
//
// ASTRYON has no identity provider of its own. This package only ever
// consumes an already-deployed AAS instance over HTTPS: it performs OIDC
// discovery against a caller-supplied base URL, verifies JWTs against the
// JWKS published by that discovery document, and extracts the claims that
// downstream code is allowed to trust (tenant_id, client_id, sub, jti,
// scopes). It never reads Keycloak/Postgres directly, never accepts
// identity from HTTP headers or request bodies, and never invents a
// "success" result on error — every failure path returns a wrapped
// sentinel error.
//
// Configuration (AAS_BASE_URL, expected audience, ...) is read by the
// caller from the environment and passed in explicitly via Config; this
// package does not read environment variables itself, so it stays a pure,
// unit-testable library.
//
// DPoP proof-of-possession (RFC 9449) is implemented separately in dpop.go
// (DPoPVerifier); it is not part of Verifier.Verify.
package authn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// allowedHeaderTypes are the acceptable JOSE "typ" header values on an AAS
// (Keycloak) access token: either the generic "JWT" (jwx's own default when
// signing) or the RFC 9068 JWT-access-token value "at+jwt". Comparison is
// case-insensitive. A token with no typ header at all is also accepted,
// since RFC 7519 marks the header OPTIONAL and not every legitimate access
// token sets it.
var allowedHeaderTypes = map[string]bool{
	"jwt":    true,
	"at+jwt": true,
}

// Sentinel errors. Use errors.Is to classify a failure; the wrapped detail
// (via %w / fmt.Errorf) carries the human-readable reason for logs.
var (
	// ErrConfig means the Verifier was misconfigured (e.g. empty base URL
	// or audience). Returned only from New, never from Verify.
	ErrConfig = errors.New("authn: invalid configuration")

	// ErrDiscoveryUnavailable means the OIDC discovery document could not
	// be fetched or parsed, and no (even stale) cached copy exists to fall
	// back on. This is a hard failure: there is no issuer/JWKS to verify
	// against.
	ErrDiscoveryUnavailable = errors.New("authn: AAS discovery unavailable")

	// ErrJWKSUnavailable means the JWKS could not be fetched or parsed,
	// and no cached copy exists to fall back on.
	ErrJWKSUnavailable = errors.New("authn: AAS JWKS unavailable")

	// ErrInvalidToken means the presented token failed verification:
	// bad/missing/empty, signature mismatch, unknown kid, wrong issuer,
	// wrong audience, expired, not yet valid, or missing a required
	// claim. Treat as fail-closed: never a zero-value Claims on this
	// path.
	ErrInvalidToken = errors.New("authn: invalid token")
)

// Claims is the trusted result of verifying an access token. Its fields are
// deliberately shaped to match services/gateway's Principal{TenantID,
// Scopes} so a follow-up adapter can construct one from the other without
// any further decoding of the JWT.
type Claims struct {
	// TenantID is the signed tenant_id claim. This is the ONLY source of
	// tenant identity callers should trust; it is never taken from
	// headers or request bodies.
	TenantID string
	// ClientID is the OAuth client_id claim identifying the calling
	// client (machine-to-gateway flow). May be empty if the token does
	// not carry it.
	ClientID string
	// Sub is the subject claim.
	Sub string
	// JTI is the token's unique identifier (required; used for
	// replay/audit purposes by callers).
	JTI string
	// Scopes is the parsed scope list: the space-delimited "scope" claim
	// split on whitespace, or (if absent) the "scp" array claim.
	Scopes []string
	// Issuer is the verified iss claim (always equal to the discovery
	// document's issuer, since that is checked literally).
	Issuer string
	// Audience is the raw aud claim values.
	Audience []string
	// Env is the signed "env" claim (dev/test/prod), identifying which
	// environment the token was minted for.
	Env string
	// AZP is the signed "azp" (authorized party) claim identifying which
	// OAuth client obtained the token. Required; see azp/client_id
	// matching note on Verify.
	AZP string
	// IssuedAt, NotBefore, ExpiresAt are the token's time claims.
	IssuedAt  time.Time
	NotBefore time.Time
	ExpiresAt time.Time
}

// HasScope reports whether the verified claims include scope s.
func (c Claims) HasScope(s string) bool {
	for _, got := range c.Scopes {
		if got == s {
			return true
		}
	}
	return false
}

// Config configures a Verifier. All fields are supplied explicitly by the
// caller (who is responsible for reading any corresponding env vars, e.g.
// AAS_BASE_URL / AAS_AUDIENCE); this package never reads the environment.
type Config struct {
	// BaseURL is the AAS base URL (env AAS_BASE_URL in callers), e.g.
	// "https://aas.internal.example". Discovery is fetched from
	// "<BaseURL>/.well-known/openid-configuration". Required.
	BaseURL string

	// Audience is the expected resource-server audience (env
	// AAS_AUDIENCE in callers); a verified token's aud claim must
	// contain it. Required.
	Audience string

	// ExpectedEnv, if non-empty, requires the token's "env" claim to
	// equal it exactly (env AAS_ENV in callers, e.g. "dev"/"test"/
	// "prod"). The claim must be present and non-empty regardless; this
	// package never hardcodes an environment value of its own. If left
	// empty, any non-empty "env" claim is accepted.
	ExpectedEnv string

	// HTTPClient is used for discovery and JWKS fetches. If nil, a
	// client with a 5s timeout is used.
	HTTPClient *http.Client

	// DiscoveryTTL controls how long the OIDC discovery document is
	// cached before being re-fetched. Defaults to 10 minutes if <= 0.
	DiscoveryTTL time.Duration

	// JWKSTTL controls how long the JWKS is cached before being
	// re-fetched (independent of refresh-on-kid-miss, which always
	// forces a refresh regardless of TTL). Defaults to 5 minutes if <= 0.
	JWKSTTL time.Duration

	// ClockSkew is the tolerance applied to exp/nbf/iat checks. Defaults
	// to 60 seconds if <= 0.
	ClockSkew time.Duration
}

// Verifier verifies AAS-issued access tokens. It is safe for concurrent
// use. Construct with New.
type Verifier struct {
	baseURL     string
	audience    string
	expectedEnv string
	httpClient  *http.Client

	discoveryTTL time.Duration
	jwksTTL      time.Duration
	clockSkew    time.Duration

	mu          sync.Mutex
	discovery   *discoveryDoc
	discoveryAt time.Time
	jwksSet     jwk.Set
	jwksAt      time.Time
}

// discoveryDoc is the subset of the OIDC discovery document this package
// relies on. issuer MUST be compared literally against a token's iss claim
// (never hardcoded/assumed); jwks_uri MUST be used as-is, never
// hand-constructed.
type discoveryDoc struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}

// New constructs a Verifier. It performs no network I/O; discovery and
// JWKS are fetched lazily (and cached) on first Verify call.
func New(cfg Config) (*Verifier, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("%w: BaseURL is required", ErrConfig)
	}
	audience := strings.TrimSpace(cfg.Audience)
	if audience == "" {
		return nil, fmt.Errorf("%w: Audience is required", ErrConfig)
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}

	discoveryTTL := cfg.DiscoveryTTL
	if discoveryTTL <= 0 {
		discoveryTTL = 10 * time.Minute
	}
	jwksTTL := cfg.JWKSTTL
	if jwksTTL <= 0 {
		jwksTTL = 5 * time.Minute
	}
	clockSkew := cfg.ClockSkew
	if clockSkew <= 0 {
		clockSkew = 60 * time.Second
	}

	return &Verifier{
		baseURL:      baseURL,
		audience:     audience,
		expectedEnv:  strings.TrimSpace(cfg.ExpectedEnv),
		httpClient:   httpClient,
		discoveryTTL: discoveryTTL,
		jwksTTL:      jwksTTL,
		clockSkew:    clockSkew,
	}, nil
}

// Verify verifies a raw bearer access token: signature against the AAS
// JWKS, iss against the discovery document's issuer (literal compare),
// aud contains the configured audience, exp/nbf/iat within clock skew
// tolerance, and presence of tenant_id/sub/jti/scope(or scp). It fails
// closed: any error returns a nil Claims and a non-nil error wrapping one
// of the sentinels in this package.
func (v *Verifier) Verify(ctx context.Context, rawToken string) (*Claims, error) {
	rawToken = strings.TrimSpace(rawToken)
	if rawToken == "" {
		return nil, fmt.Errorf("%w: empty token", ErrInvalidToken)
	}

	doc, err := v.ensureDiscovery(ctx, false)
	if err != nil {
		return nil, err
	}

	set, err := v.ensureJWKS(ctx, doc, false)
	if err != nil {
		return nil, err
	}

	token, err := v.parseAndValidate(rawToken, doc, set)
	if err != nil && isKeyNotFound(err) {
		// Possible key rotation: force a JWKS refresh and retry once
		// before giving up, so rotation doesn't require a restart.
		var jerr error
		set, jerr = v.ensureJWKS(ctx, doc, true)
		if jerr != nil {
			return nil, jerr
		}
		token, err = v.parseAndValidate(rawToken, doc, set)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	// The JOSE header "typ" is part of the signed input, so by this point
	// (signature already verified above) it is safe to trust. Checked
	// separately from the payload because jwt.Token does not expose JWS
	// protected headers.
	if err := checkHeaderType(rawToken); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	return extractClaims(token, v.expectedEnv)
}

// checkHeaderType re-parses rawToken as a JWS (no signature re-verification
// cost of consequence; this is cheap structural parsing) solely to inspect
// the protected header's "typ" value against allowedHeaderTypes. Call only
// after the token's signature has already been verified by parseAndValidate.
func checkHeaderType(rawToken string) error {
	msg, err := jws.Parse([]byte(rawToken))
	if err != nil {
		return fmt.Errorf("parse JWS header: %w", err)
	}
	sigs := msg.Signatures()
	if len(sigs) != 1 {
		return fmt.Errorf("unexpected number of JWS signatures: %d", len(sigs))
	}
	typ := sigs[0].ProtectedHeaders().Type()
	if typ == "" {
		// RFC 7519: "typ" is OPTIONAL. Not every legitimate access token
		// sets it.
		return nil
	}
	if !allowedHeaderTypes[strings.ToLower(typ)] {
		return fmt.Errorf("unexpected JOSE typ header %q", typ)
	}
	return nil
}

// parseAndValidate parses and fully validates rawToken (signature, iss,
// aud, exp/nbf/iat with skew) against set and doc. It does not inspect
// ASTRYON-specific private claims (tenant_id/sub/jti/scope); that is
// extractClaims's job, run only once this succeeds.
func (v *Verifier) parseAndValidate(rawToken string, doc *discoveryDoc, set jwk.Set) (jwt.Token, error) {
	return jwt.Parse(
		[]byte(rawToken),
		jwt.WithKeySet(set),
		jwt.WithValidate(true),
		jwt.WithIssuer(doc.Issuer),
		jwt.WithAudience(v.audience),
		jwt.WithAcceptableSkew(v.clockSkew),
	)
}

// extractClaims pulls the trusted, ASTRYON-specific claims out of an
// already signature/time/iss/aud-validated token, failing closed if any
// required claim is absent or inconsistent. expectedEnv, if non-empty,
// additionally requires an exact match on the "env" claim.
func extractClaims(token jwt.Token, expectedEnv string) (*Claims, error) {
	sub := token.Subject()
	if sub == "" {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, errMissingClaim("sub"))
	}
	jti := token.JwtID()
	if jti == "" {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, errMissingClaim("jti"))
	}

	tenantID, _ := getStringClaim(token, "tenant_id")
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, errMissingClaim("tenant_id"))
	}

	clientID, _ := getStringClaim(token, "client_id") // optional claim

	scopes, err := extractScopes(token)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	// Payload claim "typ" (distinct from the JOSE header of the same
	// name) is how Keycloak marks a token as an access token ("Bearer")
	// as opposed to e.g. an ID token, which does not carry this claim
	// with this value.
	payloadTyp, _ := getStringClaim(token, "typ")
	if payloadTyp != "Bearer" {
		return nil, fmt.Errorf("%w: payload typ claim must be %q, got %q", ErrInvalidToken, "Bearer", payloadTyp)
	}

	env, _ := getStringClaim(token, "env")
	if strings.TrimSpace(env) == "" {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, errMissingClaim("env"))
	}
	if expectedEnv != "" && env != expectedEnv {
		return nil, fmt.Errorf("%w: env claim %q does not match expected %q", ErrInvalidToken, env, expectedEnv)
	}

	azp, _ := getStringClaim(token, "azp")
	if strings.TrimSpace(azp) == "" {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, errMissingClaim("azp"))
	}
	if clientID != "" && clientID != azp {
		return nil, fmt.Errorf("%w: azp claim %q does not match client_id claim %q", ErrInvalidToken, azp, clientID)
	}

	return &Claims{
		TenantID:  tenantID,
		ClientID:  clientID,
		Sub:       sub,
		JTI:       jti,
		Scopes:    scopes,
		Issuer:    token.Issuer(),
		Audience:  token.Audience(),
		Env:       env,
		AZP:       azp,
		IssuedAt:  token.IssuedAt(),
		NotBefore: token.NotBefore(),
		ExpiresAt: token.Expiration(),
	}, nil
}

// extractScopes returns the space-delimited "scope" claim split into
// fields, falling back to the "scp" array claim. It errors if neither
// claim is present at all (required-claim-presence check).
func extractScopes(token jwt.Token) ([]string, error) {
	if scopeStr, ok := getStringClaim(token, "scope"); ok {
		return strings.Fields(scopeStr), nil
	}
	if scp, ok := getStringSliceClaim(token, "scp"); ok {
		return scp, nil
	}
	return nil, errMissingClaim("scope/scp")
}

func errMissingClaim(name string) error {
	return fmt.Errorf("missing required claim %q", name)
}

// getStringClaim fetches a private/public claim and type-asserts it to a
// string, reporting ok=false if the claim is absent or not a string.
func getStringClaim(token jwt.Token, name string) (string, bool) {
	v, ok := token.Get(name)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// getStringSliceClaim fetches a claim and coerces it to a []string,
// accepting either a native []string or a []interface{} of strings (the
// latter being how JSON-array claims typically decode). ok=false if the
// claim is absent or not a string array.
func getStringSliceClaim(token jwt.Token, name string) ([]string, bool) {
	v, ok := token.Get(name)
	if !ok {
		return nil, false
	}
	switch vv := v.(type) {
	case []string:
		return vv, true
	case []interface{}:
		out := make([]string, 0, len(vv))
		for _, item := range vv {
			s, ok := item.(string)
			if !ok {
				return nil, false
			}
			out = append(out, s)
		}
		return out, true
	default:
		return nil, false
	}
}

// ensureDiscovery returns the cached discovery document, re-fetching it if
// force is true or the cache is stale (or empty). If a fetch fails but a
// (possibly stale) cached document already exists, that cached document is
// returned instead: a short AAS outage must not invalidate an otherwise
// still-locally-valid token.
func (v *Verifier) ensureDiscovery(ctx context.Context, force bool) (*discoveryDoc, error) {
	v.mu.Lock()
	if !force && v.discovery != nil && time.Since(v.discoveryAt) < v.discoveryTTL {
		doc := v.discovery
		v.mu.Unlock()
		return doc, nil
	}
	v.mu.Unlock()

	doc, err := v.fetchDiscovery(ctx)
	if err != nil {
		v.mu.Lock()
		if v.discovery != nil {
			stale := v.discovery
			v.mu.Unlock()
			return stale, nil
		}
		v.mu.Unlock()
		return nil, fmt.Errorf("%w: %v", ErrDiscoveryUnavailable, err)
	}

	v.mu.Lock()
	v.discovery = doc
	v.discoveryAt = time.Now()
	v.mu.Unlock()
	return doc, nil
}

func (v *Verifier) fetchDiscovery(ctx context.Context) (*discoveryDoc, error) {
	url := v.baseURL + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discovery endpoint returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var doc discoveryDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("decode discovery document: %w", err)
	}
	if doc.Issuer == "" {
		return nil, errors.New("discovery document missing issuer")
	}
	if doc.JWKSURI == "" {
		return nil, errors.New("discovery document missing jwks_uri")
	}
	return &doc, nil
}

// ensureJWKS returns the cached JWKS, re-fetching it if force is true or
// the cache is stale (or empty). As with discovery, a fetch failure falls
// back to a stale cached set rather than failing closed on every request
// during a short AAS outage; it fails closed only when there is no cached
// set at all.
func (v *Verifier) ensureJWKS(ctx context.Context, doc *discoveryDoc, force bool) (jwk.Set, error) {
	v.mu.Lock()
	if !force && v.jwksSet != nil && time.Since(v.jwksAt) < v.jwksTTL {
		set := v.jwksSet
		v.mu.Unlock()
		return set, nil
	}
	v.mu.Unlock()

	set, err := jwk.Fetch(ctx, doc.JWKSURI, jwk.WithHTTPClient(v.httpClient))
	if err != nil {
		v.mu.Lock()
		if v.jwksSet != nil {
			stale := v.jwksSet
			v.mu.Unlock()
			return stale, nil
		}
		v.mu.Unlock()
		return nil, fmt.Errorf("%w: %v", ErrJWKSUnavailable, err)
	}

	v.mu.Lock()
	v.jwksSet = set
	v.jwksAt = time.Now()
	v.mu.Unlock()
	return set, nil
}

// isKeyNotFound reports whether err looks like jwx's "no key found
// matching kid" failure, as opposed to any other verification failure
// (bad signature with a known key, wrong iss/aud, expired, etc). On that
// specific failure we force one JWKS refresh and retry, to support key
// rotation without a restart; any other error is not retried.
func isKeyNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "failed to find") ||
		strings.Contains(msg, "key with key id") ||
		strings.Contains(msg, "could not find key")
}
