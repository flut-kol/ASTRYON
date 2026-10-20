package main

import (
	"net/http"
	"os"
	"strings"

	"github.com/astryon/go-common/auth"
	"github.com/astryon/query-api/internal/query"
)

// aasAuthenticator adapts the shared AAS verifier (libs/go-common/auth) to query.Authenticator.
// Any verification failure collapses to ErrUnauthenticated (fail closed, no detail leaked).
type aasAuthenticator struct{ v *auth.Verifier }

func (a aasAuthenticator) Authenticate(r *http.Request) (query.Principal, error) {
	p, err := a.v.Authenticate(r)
	if err != nil {
		return query.Principal{}, query.ErrUnauthenticated
	}
	return query.Principal{TenantID: p.TenantID, Scopes: p.Scopes}, nil
}

func aasConfig() auth.Config {
	var azp []string
	for _, s := range strings.Split(os.Getenv("AAS_ALLOWED_AZP"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			azp = append(azp, s)
		}
	}
	return auth.Config{BaseURL: os.Getenv("AAS_BASE_URL"), Issuer: os.Getenv("AAS_ISSUER"),
		Audience: os.Getenv("AAS_AUDIENCE"), Environment: os.Getenv("AAS_ENV"), AllowedAZP: azp, TokenType: os.Getenv("AAS_TOKEN_TYPE")}
}
