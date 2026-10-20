package auth

import (
	"context"
	"os"
	"testing"
	"time"
)

// Live check against a real AAS; set ASTRYON_AAS_IT_BASE_URL (e.g. https://host/sso/realms/aas)
// and optionally ASTRYON_AAS_IT_ISSUER. No token is needed: discovery and JWKS load eagerly.
func TestRealAASDiscovery(t *testing.T) {
	base := os.Getenv("ASTRYON_AAS_IT_BASE_URL")
	if base == "" {
		t.Skip("ASTRYON_AAS_IT_BASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	v, err := NewVerifier(ctx, Config{BaseURL: base, Issuer: os.Getenv("ASTRYON_AAS_IT_ISSUER"), Audience: "astryon", Environment: "dev"})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if _, err := v.Verify(ctx, "not.a.jwt"); err == nil {
		t.Fatal("garbage token accepted")
	}
}
