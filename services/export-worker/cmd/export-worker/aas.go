package main

import (
	"net/http"
	"os"
	"strings"

	"github.com/astryon/export-worker/internal/export"
	"github.com/astryon/go-common/auth"
)

// aasAuthenticator adapts the shared AAS verifier (libs/go-common/auth) to export.Authenticator.
// Any verification failure collapses to ErrUnauthenticated (fail closed, no detail leaked).
type aasAuthenticator struct{ v *auth.Verifier }

func (a aasAuthenticator) Authenticate(r *http.Request) (export.Principal, error) {
	p, err := a.v.Authenticate(r)
	if err != nil {
		return export.Principal{}, export.ErrUnauthenticated
	}
	return export.Principal{TenantID: p.TenantID, Subject: p.Subject, Scopes: p.Scopes}, nil
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
