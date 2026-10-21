package main

import (
	"errors"
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
	return gateway.Principal{TenantID: p.TenantID, Subject: p.Subject, ClientID: p.ClientID, ACR: p.ACR, AMR: p.AMR, Scopes: p.Scopes}, nil
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

// dpopConfig adds DPoP (L2): AAS_PUBLIC_BASE_URL enables it (rebuilds htu), AAS_REDIS_URL/AAS_REDIS_CA_FILE
// give the shared replay store (per-instance memory otherwise: single replica only), AAS_REQUIRE_DPOP=true rejects Bearer.
func dpopConfig(c auth.Config, getenv func(string) string) (auth.Config, error) {
	c.PublicBaseURL = getenv("AAS_PUBLIC_BASE_URL")
	c.RequireDPoP = getenv("AAS_REQUIRE_DPOP") == "true"
	if c.PublicBaseURL == "" {
		if c.RequireDPoP {
			return c, errors.New("AAS_REQUIRE_DPOP needs AAS_PUBLIC_BASE_URL")
		}
		return c, nil
	}
	if u := getenv("AAS_REDIS_URL"); u != "" {
		s, err := auth.NewRedisReplayStore(u, getenv("AAS_REDIS_CA_FILE"))
		if err != nil {
			return c, err
		}
		c.ReplayStore = s
	} else {
		c.ReplayStore = &auth.MemoryReplayStore{}
	}
	return c, nil
}
