package main

import "testing"

func TestDPoPConfig(t *testing.T) {
	t.Setenv("AAS_PUBLIC_BASE_URL", "")
	t.Setenv("AAS_REQUIRE_DPOP", "true")
	if _, err := dpopConfig(aasConfig()); err == nil {
		t.Fatal("RequireDPoP without public URL must fail")
	}
	t.Setenv("AAS_REQUIRE_DPOP", "")
	if c, err := dpopConfig(aasConfig()); err != nil || c.ReplayStore != nil {
		t.Fatalf("off by default: %v", err)
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
