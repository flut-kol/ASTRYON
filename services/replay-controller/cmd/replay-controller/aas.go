package main

import (
	"net/http"
	"os"
	"strings"

	"github.com/astryon/go-common/auth"
	"github.com/astryon/replay-controller/internal/replay"
)

// aasAuthenticator adapts the shared AAS verifier (libs/go-common/auth) to replay.Authenticator.
// Tenant, subject and scopes come only from the verified token. Any verification failure collapses to
// ErrUnauthenticated (fail closed, no detail leaked).
type aasAuthenticator struct{ v *auth.Verifier }

func (a aasAuthenticator) Authenticate(r *http.Request) (replay.Principal, error) {
	p, err := a.v.Authenticate(r)
	if err != nil {
		return replay.Principal{}, replay.ErrUnauthenticated
	}
	return replay.Principal{TenantID: p.TenantID, Subject: p.Subject, Scopes: p.Scopes}, nil
}

// denyAll is used when neither AAS nor the dev token is configured.
type denyAll struct{}

func (denyAll) Authenticate(*http.Request) (replay.Principal, error) {
	return replay.Principal{}, replay.ErrUnauthenticated
}

func aasConfig() auth.Config {
	var azp []string
	for _, s := range strings.Split(os.Getenv("AAS_ALLOWED_AZP"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			azp = append(azp, s)
		}
	}
	return auth.Config{BaseURL: os.Getenv("AAS_BASE_URL"), Issuer: os.Getenv("AAS_ISSUER"),
		Audience: os.Getenv("AAS_AUDIENCE"), Environment: os.Getenv("AAS_ENV"), AllowedAZP: azp}
}
