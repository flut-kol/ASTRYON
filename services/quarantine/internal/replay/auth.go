package replay

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

var ErrUnauthenticated = errors.New("unauthenticated")

// DevStaticAuthenticator is deliberately limited to one configured tenant.
// Production wiring uses the shared AAS verifier in cmd/quarantine.
type DevStaticAuthenticator struct {
	Token     string
	Principal BrowserPrincipal
}

func (d DevStaticAuthenticator) Authenticate(r *http.Request) (BrowserPrincipal, error) {
	header := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if d.Token == "" || len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) ||
		subtle.ConstantTimeCompare([]byte(strings.TrimSpace(header[len(prefix):])), []byte(d.Token)) != 1 {
		return BrowserPrincipal{}, ErrUnauthenticated
	}
	return d.Principal, nil
}
