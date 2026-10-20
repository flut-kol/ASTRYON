package main

import (
	"net/http"
	"strings"

	"github.com/astryon/gateway/internal/gateway"
	"github.com/astryon/go-common/auth"
)

// aasAuthenticator adapts the shared AAS verifier (libs/go-common/auth) to gateway.Authenticator.
// Every verification failure collapses to ErrUnauthenticated (fail closed, no detail leaked).
type aasAuthenticator struct{ v *auth.Verifier }

func (a aasAuthenticator) Authenticate(r *http.Request) (gateway.Principal, error) {
	p, err := a.v.Authenticate(r)
	if err != nil {
		return gateway.Principal{}, gateway.ErrUnauthenticated
	}
	return gateway.Principal{TenantID: p.TenantID, Scopes: p.Scopes}, nil
}

func aasConfig(getenv func(string) string) auth.Config {
	var azp []string
	for _, s := range strings.Split(getenv("AAS_ALLOWED_AZP"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			azp = append(azp, s)
		}
	}
	return auth.Config{BaseURL: getenv("AAS_BASE_URL"), Issuer: getenv("AAS_ISSUER"),
		Audience: getenv("AAS_AUDIENCE"), Environment: getenv("AAS_ENV"), AllowedAZP: azp, TokenType: getenv("AAS_TOKEN_TYPE")}
}
