package scanner

import (
	"context"
	"testing"
)

func TestCombinedCleanRequiresAllConfiguredProviders(t *testing.T) {
	v := (Combined{Providers: []Provider{
		fakeProvider{v: Verdict{Status: Clean, Provider: "defender"}},
		fakeProvider{v: Verdict{Status: Clean, Provider: "clamav"}},
	}}).Scan(context.Background(), File{Path: "/tmp/staged"})
	if v.Status != Clean || v.Provider != "fake+fake" {
		t.Fatalf("all clean providers should produce clean verdict: %+v", v)
	}
}

func TestCombinedInfectedWinsAndDoesNotExposeEngineOutput(t *testing.T) {
	v := (Combined{Providers: []Provider{
		fakeProvider{v: Verdict{Status: Clean, Provider: "defender"}},
		fakeProvider{v: Verdict{Status: Infected, Provider: "clamav", ReasonCode: "malware_detected"}},
	}}).Scan(context.Background(), File{Path: "/tmp/staged"})
	if v.Status != Infected || v.ReasonCode != "malware_detected" || v.Provider != "fake" {
		t.Fatalf("infection must quarantine before clean result: %+v", v)
	}
}

func TestCombinedErrorFailsClosedAndPreservesRetryability(t *testing.T) {
	v := (Combined{Providers: []Provider{
		fakeProvider{v: Verdict{Status: Clean, Provider: "defender"}},
		fakeProvider{v: Verdict{Status: ScanError, Provider: "clamav", Retryable: true, ReasonCode: "provider_unavailable"}},
	}}).Scan(context.Background(), File{Path: "/tmp/staged"})
	if v.Status != ScanError || !v.Retryable || v.ReasonCode != "provider_unavailable" {
		t.Fatalf("provider error must not fail open as clean: %+v", v)
	}
}

func TestCombinedNoProvidersFailsClosed(t *testing.T) {
	v := (Combined{}).Scan(context.Background(), File{Path: "/tmp/staged"})
	if v.Status != ScanError || v.ReasonCode != "not_configured" || v.Retryable {
		t.Fatalf("empty scanner set must be explicit non-retryable configuration error: %+v", v)
	}
}
