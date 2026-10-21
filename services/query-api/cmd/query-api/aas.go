package main

import (
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/astryon/go-common/auth"
	"github.com/astryon/query-api/internal/query"
)

// aasAuthenticator adapts the shared AAS verifier (libs/go-common/auth) to query.Authenticator.
// Any verification failure collapses to ErrUnauthenticated (fail closed, no detail leaked).
// When in is set (opt-in L3, AAS_INTROSPECTION_*), the token must also be active at AAS and match
// the verified identity; an introspection outage fails closed.
type aasAuthenticator struct {
	v  *auth.Verifier
	in *auth.Introspector
}

func (a aasAuthenticator) Authenticate(r *http.Request) (query.Principal, error) {
	p, err := a.v.Authenticate(r)
	if err != nil {
		return query.Principal{}, query.ErrUnauthenticated
	}
	if a.in != nil {
		parts := strings.Fields(r.Header.Get("Authorization"))
		if len(parts) != 2 {
			return query.Principal{}, query.ErrUnauthenticated
		}
		res, err := a.in.Introspect(r.Context(), parts[1])
		if err != nil || !res.Active || !sameIdentity(p, res.Principal()) {
			return query.Principal{}, query.ErrUnauthenticated
		}
	}
	return query.Principal{TenantID: p.TenantID, Subject: p.Subject, ClientID: p.ClientID, ACR: p.ACR, AMR: p.AMR, Scopes: p.Scopes}, nil
}

// sameIdentity mirrors auth.RequireActive: every identity field AAS returned must equal the verified one.
func sameIdentity(v, i auth.Principal) bool {
	eq := func(a, b string) bool { return b == "" || a == b }
	return eq(v.Subject, i.Subject) && eq(v.JTI, i.JTI) && eq(v.ClientID, i.ClientID) && eq(v.TenantID, i.TenantID)
}

// introspectorConfig is nil unless AAS_INTROSPECTION_KEY_FILE is set (L3 stays opt-in).
func introspectorConfig() *auth.IntrospectorConfig {
	f := os.Getenv("AAS_INTROSPECTION_KEY_FILE")
	if f == "" {
		return nil
	}
	return &auth.IntrospectorConfig{BaseURL: os.Getenv("AAS_BASE_URL"), ClientID: os.Getenv("AAS_INTROSPECTION_CLIENT_ID"),
		PrivateKeyFile: f, KeyID: os.Getenv("AAS_INTROSPECTION_KEY_ID")}
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

// dpopConfig adds DPoP (L2) settings: AAS_PUBLIC_BASE_URL (enables it; rebuilds htu), AAS_REDIS_URL
// (+ AAS_REDIS_CA_FILE) shared replay store, AAS_REQUIRE_DPOP=true rejects Bearer. Without a Redis URL
// the replay store is per instance, which is only safe for a single replica.
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
