package main

import (
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/astryon/file-ingestion/internal/api"
	"github.com/astryon/go-common/auth"
)

type aasAuthenticator struct{ v *auth.Verifier }

func (a aasAuthenticator) Authenticate(r *http.Request) (api.Principal, error) {
	p, err := a.v.Authenticate(r)
	if err != nil {
		return api.Principal{}, api.ErrUnauthenticated
	}
	return api.Principal{TenantID: p.TenantID, Subject: p.Subject, ClientID: p.ClientID, ACR: p.ACR, AMR: p.AMR, Scopes: p.Scopes}, nil
}

// aasConfig reads AAS_BASE_URL AAS_ISSUER AAS_AUDIENCE AAS_ENV AAS_ALLOWED_AZP AAS_TOKEN_TYPE and the optional DPoP
// settings AAS_PUBLIC_BASE_URL AAS_REDIS_URL AAS_REDIS_CA_FILE AAS_REQUIRE_DPOP (same meaning as in query-api).
func aasConfig() (auth.Config, error) {
	var azp []string
	for _, s := range strings.Split(os.Getenv("AAS_ALLOWED_AZP"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			azp = append(azp, s)
		}
	}
	c := auth.Config{BaseURL: os.Getenv("AAS_BASE_URL"), Issuer: os.Getenv("AAS_ISSUER"), Audience: os.Getenv("AAS_AUDIENCE"),
		Environment: os.Getenv("AAS_ENV"), AllowedAZP: azp, TokenType: os.Getenv("AAS_TOKEN_TYPE"),
		PublicBaseURL: os.Getenv("AAS_PUBLIC_BASE_URL"), RequireDPoP: os.Getenv("AAS_REQUIRE_DPOP") == "true"}
	if c.PublicBaseURL == "" {
		if c.RequireDPoP {
			return c, errors.New("AAS_REQUIRE_DPOP needs AAS_PUBLIC_BASE_URL")
		}
		return c, nil
	}
	if u := os.Getenv("AAS_REDIS_URL"); u != "" {
		s, err := auth.NewRedisReplayStore(u, os.Getenv("AAS_REDIS_CA_FILE"))
		if err != nil {
			return c, err
		}
		c.ReplayStore = s
	} else {
		c.ReplayStore = &auth.MemoryReplayStore{}
	}
	return c, nil
}
