package cdc

import (
	"errors"
	"testing"
)

func TestSnapshotTailTransition(t *testing.T) {
	if err := Transition(Snapshotting, Tailing); err != nil {
		t.Fatal(err)
	}
	if err := Transition(Tailing, Snapshotting); !errors.Is(err, ErrTransition) {
		t.Fatal(err)
	}
}
func TestSchemaCompatibility(t *testing.T) {
	if !Compatible(map[string]string{"id": "uuid"}, map[string]string{"id": "uuid", "name": "string"}) {
		t.Fatal("additive must pass")
	}
	if Compatible(map[string]string{"id": "uuid"}, map[string]string{"id": "string"}) {
		t.Fatal("type change must fail")
	}
}
