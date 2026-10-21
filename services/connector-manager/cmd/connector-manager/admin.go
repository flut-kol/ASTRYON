package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/astryon/connector-manager/internal/adminapi"
	"github.com/astryon/connector-manager/internal/registry"
	goauth "github.com/astryon/go-common/auth"
)

// aasAuth adapts the shared AAS verifier; any failure collapses to ErrUnauthenticated (fail closed).
type aasAuth struct{ v *goauth.Verifier }

func (a aasAuth) Authenticate(r *http.Request) (goauth.Principal, error) {
	p, err := a.v.Authenticate(r)
	if err != nil {
		return goauth.Principal{}, adminapi.ErrUnauthenticated
	}
	return p, nil
}

// devAuth is DEV-ONLY: one static bearer token -> one fixed principal.
type devAuth struct {
	token string
	p     goauth.Principal
}

func (d devAuth) Authenticate(r *http.Request) (goauth.Principal, error) {
	const pre = "Bearer "
	h := r.Header.Get("Authorization")
	if d.token == "" || len(h) <= len(pre) || !strings.EqualFold(h[:len(pre)], pre) ||
		subtle.ConstantTimeCompare([]byte(strings.TrimSpace(h[len(pre):])), []byte(d.token)) != 1 {
		return goauth.Principal{}, adminapi.ErrUnauthenticated
	}
	return d.p, nil
}

type denyAll struct{}

func (denyAll) Authenticate(*http.Request) (goauth.Principal, error) {
	return goauth.Principal{}, adminapi.ErrUnauthenticated
}

// newAuthenticator: AAS when ASTRYON_AAS_BASE_URL (+ ISSUER, AUDIENCE, ENV, ALLOWED_AZP, TOKEN_TYPE) is set;
// dev static when ASTRYON_CONNECTOR_DEV_TOKEN + ASTRYON_CONNECTOR_DEV_TENANT are set (overrides AAS); else deny all.
func newAuthenticator(ctx context.Context, getenv func(string) string) (adminapi.Authenticator, error) {
	if t := getenv("ASTRYON_CONNECTOR_DEV_TOKEN"); t != "" {
		if getenv("ASTRYON_CONNECTOR_DEV_TENANT") == "" {
			return nil, errors.New("ASTRYON_CONNECTOR_DEV_TOKEN needs ASTRYON_CONNECTOR_DEV_TENANT")
		}
		return devAuth{token: t, p: goauth.Principal{TenantID: getenv("ASTRYON_CONNECTOR_DEV_TENANT"),
			ClientID: "connector-manager-dev", Scopes: []string{registry.ManageScope, adminapi.SourcesReadScope}}}, nil
	}
	if base := getenv("ASTRYON_AAS_BASE_URL"); base != "" {
		var azp []string
		for _, s := range strings.Split(getenv("ASTRYON_AAS_ALLOWED_AZP"), ",") {
			if s = strings.TrimSpace(s); s != "" {
				azp = append(azp, s)
			}
		}
		v, err := goauth.NewVerifier(ctx, goauth.Config{BaseURL: base, Issuer: getenv("ASTRYON_AAS_ISSUER"),
			Audience: getenv("ASTRYON_AAS_AUDIENCE"), Environment: getenv("ASTRYON_AAS_ENV"), AllowedAZP: azp,
			TokenType: getenv("ASTRYON_AAS_TOKEN_TYPE")})
		if err != nil {
			return nil, err
		}
		return aasAuth{v: v}, nil
	}
	return denyAll{}, nil
}
