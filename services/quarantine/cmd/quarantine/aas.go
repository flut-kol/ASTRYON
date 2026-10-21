package main

import (
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/astryon/go-common/auth"
	"github.com/astryon/quarantine/internal/replay"
)

type aasAuthenticator struct{ verifier *auth.Verifier }

func (a aasAuthenticator) Authenticate(r *http.Request) (replay.BrowserPrincipal, error) {
	principal, err := a.verifier.Authenticate(r)
	if err != nil {
		return replay.BrowserPrincipal{}, replay.ErrUnauthenticated
	}
	scopes := make(map[string]bool, len(principal.Scopes))
	for _, scope := range principal.Scopes {
		scopes[scope] = true
	}
	return replay.BrowserPrincipal{TenantID: principal.TenantID, Subject: principal.Subject, Scopes: scopes}, nil
}

func aasConfig() auth.Config {
	var allowedAZP []string
	for _, value := range strings.Split(os.Getenv("AAS_ALLOWED_AZP"), ",") {
		if value = strings.TrimSpace(value); value != "" {
			allowedAZP = append(allowedAZP, value)
		}
	}
	return auth.Config{
		BaseURL: os.Getenv("AAS_BASE_URL"), Issuer: os.Getenv("AAS_ISSUER"),
		Audience: os.Getenv("AAS_AUDIENCE"), Environment: os.Getenv("AAS_ENV"), AllowedAZP: allowedAZP,
		TokenType: os.Getenv("AAS_TOKEN_TYPE"),
	}
}

// dpopConfig adds DPoP (L2): AAS_PUBLIC_BASE_URL enables it (rebuilds htu), AAS_REDIS_URL/AAS_REDIS_CA_FILE give
// the shared replay store (per-instance memory otherwise: single replica only), AAS_REQUIRE_DPOP=true rejects Bearer.
func dpopConfig(c auth.Config) (auth.Config, error) {
	c.PublicBaseURL = os.Getenv("AAS_PUBLIC_BASE_URL")
	c.RequireDPoP = os.Getenv("AAS_REQUIRE_DPOP") == "true"
	if c.PublicBaseURL == "" {
		if c.RequireDPoP {
			return c, errors.New("AAS_REQUIRE_DPOP needs AAS_PUBLIC_BASE_URL")
		}
		return c, nil
	}
	if u := os.Getenv("AAS_REDIS_URL"); u != "" {
		store, err := auth.NewRedisReplayStore(u, os.Getenv("AAS_REDIS_CA_FILE"))
		if err != nil {
			return c, err
		}
		c.ReplayStore = store
	} else {
		c.ReplayStore = &auth.MemoryReplayStore{}
	}
	return c, nil
}
