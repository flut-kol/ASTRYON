package main

import "testing"

// AAS_TOKEN_TYPE must reach auth.Config.TokenType; empty keeps the library default (Bearer or at+jwt).
func TestAASConfigTokenType(t *testing.T) {
	t.Setenv("AAS_TOKEN_TYPE", "")
	if got := aasConfig().TokenType; got != "" {
		t.Fatalf("empty env must leave default, got %q", got)
	}
	t.Setenv("AAS_TOKEN_TYPE", "at+jwt")
	if got := aasConfig().TokenType; got != "at+jwt" {
		t.Fatalf("TokenType = %q", got)
	}
}
