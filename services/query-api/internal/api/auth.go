package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/astryon/go-common/pkg/authn"
)

// Scopes (docs/architecture/authentication.md).
const (
	ScopeEventsRead  = "astryon:events:read"
	ScopeMetricsRead = "astryon:metrics:read"
	ScopeSourcesRead = "astryon:sources:read"
	ScopeJobsRead    = "astryon:jobs:read"
)

// Principal is the trusted caller identity. TenantID is the ONLY source of
// tenant identity; bodies, query strings and headers are never trusted for it.
type Principal struct {
	TenantID string
	Scopes   []string
}

func (p Principal) HasScope(s string) bool {
	for _, x := range p.Scopes {
		if x == s {
			return true
		}
	}
	return false
}

var ErrUnauthenticated = errors.New("unauthenticated")

// Authenticator verifies request credentials; implementations must fail closed.
type Authenticator interface {
	Authenticate(r *http.Request) (Principal, error)
}

// DevStaticAuthenticator is DEV ONLY: one static bearer token, one principal.
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

// AASJWTAuthenticator verifies AAS access tokens via libs/go-common/pkg/authn.
// A nil verifier fails closed.
type AASJWTAuthenticator struct{ verifier *authn.Verifier }

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
