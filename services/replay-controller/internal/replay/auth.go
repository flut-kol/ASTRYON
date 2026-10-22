package replay

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

// ScopeReplayWrite is required for POST /v1/replays.
const ScopeReplayWrite = "astryon:replay:write"

// Principal is the trusted caller identity. TenantID is the ONLY source of tenant identity.
type Principal struct {
	TenantID string
	Subject  string // caller identity, audit only (never used for authorization)
	ClientID string // validated client_id claim, audit only
	ACR      string // validated acr claim, audit only
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

// Authenticator must fail closed. Same shape as services/query-api and services/gateway.
// TODO(DPP-014): production adapter over libs/go-common/auth.Verifier.
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
