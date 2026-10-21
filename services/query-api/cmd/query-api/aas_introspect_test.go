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
