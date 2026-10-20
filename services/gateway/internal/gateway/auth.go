package gateway

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/astryon/go-common/pkg/authn"
)

// ScopeEventsWrite is required to submit events.
const ScopeEventsWrite = "astryon:events:write"

// Principal is the trusted identity of the caller. TenantID is the ONLY
// source of tenant identity; body and headers are never trusted for it.
type Principal struct {
	TenantID string
	Scopes   []string
}

// HasScope reports whether the principal holds scope s.
func (p Principal) HasScope(s string) bool {
	for _, x := range p.Scopes {
		if x == s {
			return true
		}
	}
	return false
}

// ErrUnauthenticated means credentials are missing/invalid (HTTP 401).
var ErrUnauthenticated = errors.New("unauthenticated")

// Authenticator verifies the request credentials and returns a Principal.
// Implementations must fail closed.
type Authenticator interface {
	Authenticate(r *http.Request) (Principal, error)
}

// DevStaticAuthenticator is a DEV-ONLY authenticator with one static bearer
// token mapped to one principal. Never enable outside local development.
type DevStaticAuthenticator struct {
	Token     string
	Principal Principal
}

func (d DevStaticAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	tok, ok := bearer(r)
	if !ok || d.Token == "" || subtle.ConstantTimeCompare([]byte(tok), []byte(d.Token)) != 1 {
		return Principal{}, ErrUnauthenticated
	}
	return d.Principal, nil
}

// AASJWTAuthenticator verifies AAS access tokens
// (docs/architecture/authentication.md) using the shared
// libs/go-common/pkg/authn Verifier. It never does ad-hoc JWT parsing
// itself; all discovery/JWKS/claim verification lives in that package.
//
// A zero-value AASJWTAuthenticator (nil verifier, e.g. misconfiguration)
// fails closed rather than panicking.
type AASJWTAuthenticator struct {
	verifier *authn.Verifier
}

// NewAASJWTAuthenticator constructs an AASJWTAuthenticator backed by v.
// Passing a nil v is allowed and yields an authenticator that always fails
// closed (ErrUnauthenticated).
func NewAASJWTAuthenticator(v *authn.Verifier) AASJWTAuthenticator {
	return AASJWTAuthenticator{verifier: v}
}

func (a AASJWTAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	if a.verifier == nil {
		return Principal{}, ErrUnauthenticated
	}
	tok, ok := bearer(r)
	if !ok || tok == "" {
		return Principal{}, ErrUnauthenticated
	}
	claims, err := a.verifier.Verify(r.Context(), tok)
	if err != nil {
		// Detail is retained for logs via %w/%v wrapping, but callers must
		// not surface it: the HTTP layer always responds with a generic
		// 401 regardless of the wrapped reason.
		return Principal{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	return Principal{TenantID: claims.TenantID, Scopes: claims.Scopes}, nil
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if len(h) <= len(p) || !strings.EqualFold(h[:len(p)], p) {
		return "", false
	}
	return strings.TrimSpace(h[len(p):]), true
}

type ctxKey int

const reqIDKey ctxKey = 1

func requestID(ctx context.Context) string {
	s, _ := ctx.Value(reqIDKey).(string)
	return s
}
