package gateway

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
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

// AASJWTAuthenticator is a STUB for AAS access-token verification
// (docs/architecture/authentication.md). It always rejects (fail closed).
//
// Production auth is wired in cmd/gateway/aas.go via libs/go-common/auth (AAS_BASE_URL set); this stub is the fail-closed default.
// Original TODO(DPP-auth) scope, now covered by go-common except DPoP/L3:
// OIDC discovery from AAS_BASE_URL, JWKS verification (bounded cache), iss/aud/
// exp/nbf/jti/typ/env/azp checks, DPoP proof, scopes and signed tenant_id claim
// mapped into Principal. Do not implement ad-hoc JWT parsing here.
type AASJWTAuthenticator struct{}

func (AASJWTAuthenticator) Authenticate(*http.Request) (Principal, error) {
	return Principal{}, ErrUnauthenticated
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
