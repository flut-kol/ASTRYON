package main

import "testing"

// AAS_TOKEN_TYPE must reach auth.Config.TokenType; empty keeps the library default (Bearer or at+jwt).
func TestAASConfigTokenType(t *testing.T) {
	env := map[string]string{}
	get := func(k string) string { return env[k] }
	if got := aasConfig(get).TokenType; got != "" {
		t.Fatalf("empty env must leave default, got %q", got)
	}
	env["AAS_TOKEN_TYPE"] = "at+jwt"
	if got := aasConfig(get).TokenType; got != "at+jwt" {
		t.Fatalf("TokenType = %q", got)
	}
}
