package replay

import (
	"testing"
	"time"
)

func TestAuditMirrorMappingUsesTrustedClaimsAndStableID(t *testing.T) {
	v := int32(7)
	e := AuditEvent{EventID: "00112233445566778899aabbccddeeff", Time: time.Now().UTC(),
		TenantID: "11111111-1111-4111-8111-111111111111", Subject: "alice", ClientID: "portal", ACR: "mfa",
		Action: ActionActivate, ReplayID: "22222222-2222-4222-8222-222222222222", ToVersion: &v,
		Outcome: OutcomeSuccess, Phase: PhaseCommitted, RequestID: "req-1"}
	got, err := toLibraryEvent(e).Normalize(time.Now())
	if err != nil {
		t.Fatalf("normalize mirror event: %v", err)
	}
	if got.ID != eventUUID(e.EventID) || got.TenantID != e.TenantID || got.ClientID != e.ClientID || got.ACR != e.ACR {
		t.Fatalf("mapping lost trusted claims/id: %+v", got)
	}
}

func TestUnauthenticatedDeniedHasNoTenantMirror(t *testing.T) {
	e := DeniedEvent{EventID: "00112233445566778899aabbccddeeff", Time: time.Now().UTC(), Action: ActionAuthDenied,
		Reason: ReasonUnauthenticated, RemoteIP: "203.0.113.0/24", Path: "/v1/replays", Method: "POST"}
	if e.TenantID != "" {
		t.Fatal("unauthenticated denial unexpectedly has a tenant")
	}
}
