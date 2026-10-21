package query

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

// ScopeQueryRead is required for /v1/query/*.
const ScopeQueryRead = "astryon:query:read"

// Principal is the trusted caller identity. TenantID is the ONLY source of tenant identity.
type Principal struct {
	TenantID string
	Subject  string
	ClientID string
	ACR      string
	AMR      []string
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

// Authenticator must fail closed. Same shape as services/gateway; production wiring adapts
// libs/go-common/auth.Verifier (TODO(DPP-014) once it is stable).
type Authenticator interface {
	Authenticate(r *http.Request) (Principal, error)
}

// DevStaticAuthenticator is DEV-ONLY: one static bearer token -> one principal.
type DevStaticAuthenticator struct {
	Token     string
	Principal Principal
}

func (d DevStaticAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if d.Token == "" || len(h) <= len(p) || !strings.EqualFold(h[:len(p)], p) ||
		subtle.ConstantTimeCompare([]byte(strings.TrimSpace(h[len(p):])), []byte(d.Token)) != 1 {
		return Principal{}, ErrUnauthenticated
	}
	return d.Principal, nil
}
