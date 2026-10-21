package main

import (
	"testing"

	"github.com/astryon/go-common/auth"
)

func TestIntrospectorConfigOptIn(t *testing.T) {
	t.Setenv("AAS_INTROSPECTION_KEY_FILE", "")
	if introspectorConfig() != nil {
		t.Fatal("L3 must stay off without a key file")
	}
	t.Setenv("AAS_INTROSPECTION_KEY_FILE", "/k.pem")
	t.Setenv("AAS_BASE_URL", "http://aas")
	t.Setenv("AAS_INTROSPECTION_CLIENT_ID", "c")
	if c := introspectorConfig(); c == nil || c.ClientID != "c" || c.PrivateKeyFile != "/k.pem" || c.BaseURL != "http://aas" {
		t.Fatalf("config = %+v", c)
	}
}

func TestSameIdentity(t *testing.T) {
	v := auth.Principal{Subject: "s", JTI: "j", ClientID: "c", TenantID: "t"}
	if !sameIdentity(v, auth.Principal{Subject: "s", TenantID: "t"}) {
		t.Fatal("missing introspection fields must not fail")
	}
	if sameIdentity(v, auth.Principal{TenantID: "other"}) || sameIdentity(v, auth.Principal{JTI: "x"}) {
		t.Fatal("mismatch must fail")
	}
}

func TestDPoPConfig(t *testing.T) {
	t.Setenv("AAS_PUBLIC_BASE_URL", "")
	t.Setenv("AAS_REQUIRE_DPOP", "true")
	if _, err := dpopConfig(aasConfig()); err == nil {
		t.Fatal("RequireDPoP without public URL must fail")
	}
	t.Setenv("AAS_REQUIRE_DPOP", "")
	if c, err := dpopConfig(aasConfig()); err != nil || c.ReplayStore != nil {
		t.Fatalf("off by default: %v %+v", err, c.ReplayStore)
	}
	t.Setenv("AAS_PUBLIC_BASE_URL", "https://q.example.com")
	if c, err := dpopConfig(aasConfig()); err != nil || c.ReplayStore == nil {
		t.Fatalf("memory fallback: %v", err)
	}
	t.Setenv("AAS_REDIS_URL", "http://bad")
	if _, err := dpopConfig(aasConfig()); err == nil {
		t.Fatal("bad redis url accepted")
	}
}
