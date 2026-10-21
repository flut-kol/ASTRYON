package main

import "testing"

func TestDPoPConfig(t *testing.T) {
	env := map[string]string{}
	get := func(k string) string { return env[k] }
	env["AAS_REQUIRE_DPOP"] = "true"
	if _, err := dpopConfig(aasConfig(get), get); err == nil {
		t.Fatal("RequireDPoP without public URL must fail")
	}
	delete(env, "AAS_REQUIRE_DPOP")
	if c, err := dpopConfig(aasConfig(get), get); err != nil || c.ReplayStore != nil {
		t.Fatalf("off by default: %v", err)
	}
	env["AAS_PUBLIC_BASE_URL"] = "https://ingest.example.com"
	if c, err := dpopConfig(aasConfig(get), get); err != nil || c.ReplayStore == nil || c.PublicBaseURL == "" {
		t.Fatalf("memory fallback: %v", err)
	}
	env["AAS_REDIS_URL"] = "http://bad"
	if _, err := dpopConfig(aasConfig(get), get); err == nil {
		t.Fatal("bad redis url accepted")
	}
}
