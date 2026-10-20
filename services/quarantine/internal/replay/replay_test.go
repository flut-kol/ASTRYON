package replay

import (
	"errors"
	"testing"
)

func TestReplayRequiresExplicitTenantScopeAndRange(t *testing.T) {
	p := Principal{TenantID: "t", Scopes: map[string]bool{ReplayScope: true}}
	r := Request{TenantID: "t", Source: "events.quarantine.v1", FromOffset: "10", ToOffset: "20", PipelineVersion: "v2", DryRun: true}
	if err := Validate(p, r); err != nil {
		t.Fatal(err)
	}
	r.ToOffset = ""
	if err := Validate(p, r); !errors.Is(err, ErrForbidden) {
		t.Fatal(err)
	}
}
