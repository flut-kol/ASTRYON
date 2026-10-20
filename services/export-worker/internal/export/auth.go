package export

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

const (
	ScopeExportWrite = "astryon:export:write"
	ScopeExportRead  = "astryon:export:read"
)

// HasScope reports whether the principal carries scope.
func (p Principal) HasScope(s string) bool {
	for _, x := range p.Scopes {
		if x == s {
			return true
		}
	}
	return false
}

var ErrUnauthenticated = errors.New("unauthenticated")

// Authenticator must fail closed; production wiring adapts libs/go-common/auth (cmd/export-worker/aas.go).
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
