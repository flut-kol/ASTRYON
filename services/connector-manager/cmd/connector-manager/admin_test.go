package main

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/astryon/connector-manager/internal/registry"
)

func TestDevAuth(t *testing.T) {
	env := map[string]string{"ASTRYON_CONNECTOR_DEV_TOKEN": "tok", "ASTRYON_CONNECTOR_DEV_TENANT": "t9"}
	a, err := newAuthenticator(context.Background(), func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/", nil)
	if _, err := a.Authenticate(r); err == nil {
		t.Fatal("missing token accepted")
	}
	r.Header.Set("Authorization", "Bearer tok")
	p, err := a.Authenticate(r)
	if err != nil || p.TenantID != "t9" || !p.Scopes[registry.ManageScope] {
		t.Fatalf("%+v %v", p, err)
	}
	r.Header.Set("Authorization", "Bearer nope")
	if _, err := a.Authenticate(r); err == nil {
		t.Fatal("wrong token accepted")
	}
}

func TestAuthenticatorDefaultsToDenyAndValidates(t *testing.T) {
	none := func(string) string { return "" }
	a, _ := newAuthenticator(context.Background(), none)
	if _, err := a.Authenticate(httptest.NewRequest("GET", "/", nil)); err == nil {
		t.Fatal("deny-all expected")
	}
	if _, err := newAuthenticator(context.Background(), func(k string) string {
		if k == "ASTRYON_CONNECTOR_DEV_TOKEN" {
			return "x"
		}
		return ""
	}); err == nil {
		t.Fatal("dev token without tenant must fail")
	}
}
