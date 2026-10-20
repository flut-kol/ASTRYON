package registry

import (
	"errors"
	"testing"
)

func TestAuthorizeTenantAndScope(t *testing.T) {
	c := Connector{TenantID: "t"}
	if err := Authorize(Principal{TenantID: "t", Scopes: map[string]bool{ManageScope: true}}, c); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(Principal{TenantID: "other", Scopes: map[string]bool{ManageScope: true}}, c); !errors.Is(err, ErrForbidden) {
		t.Fatal(err)
	}
	if err := Authorize(Principal{TenantID: "t"}, c); !errors.Is(err, ErrForbidden) {
		t.Fatal(err)
	}
}
