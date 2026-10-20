package registry

import (
	"errors"
	"testing"
)

func TestConnectorRequiresTrustedTenantAndSecretReference(t *testing.T) {
	c := Connector{ID: "c1", TenantID: "tenant-a", Name: "source", Kind: RESTPull, SecretRef: "vault://connectors/c1", Config: map[string]string{"url": "https://example.invalid"}}
	if err := c.Validate("tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate("tenant-b"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	c.SecretRef = "plaintext"
	if err := c.Validate("tenant-a"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
