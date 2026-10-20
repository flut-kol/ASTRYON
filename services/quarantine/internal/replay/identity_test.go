package replay

import (
	"errors"
	"testing"
)

func TestIdentityIsVersionScoped(t *testing.T) {
	a := Identity{TenantID: "t", EventID: "e", PipelineVersion: "v1"}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	if !SameBusinessResult(a, a) {
		t.Fatal("same identity must dedup")
	}
	b := a
	b.PipelineVersion = "v2"
	if SameBusinessResult(a, b) {
		t.Fatal("new version is explicit replay domain")
	}
	if err := (Identity{}).Validate(); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
}
