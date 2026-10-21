package listing

import (
	"strings"
	"testing"
	"time"
)

func TestBuildParameterisesEverything(t *testing.T) {
	cur := encodeCursor(time.Unix(100, 0), "0a1b2c3d-1111-4222-8333-444455556666")
	q, args, err := Build("T", Filter{SourceType: "geo'; DROP", Sensitivity: "restricted", Status: "QUARANTINED", Label: "contains_pii",
		Tags: []Tag{{"team", "ops", true}, {"env", "", false}}, From: time.Unix(1, 0), To: time.Unix(2, 0), Limit: 9999, Cursor: cur})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(q, "DROP") || strings.Contains(q, "ops") {
		t.Fatalf("value interpolated: %s", q)
	}
	for _, want := range []string{"f.tenant_id = $1::uuid", "f.tags @>", "f.tags ? ", "j.status =", "j.content_labels ?", "(f.created_at, f.id) <", "ORDER BY f.created_at DESC, f.id DESC"} {
		if !strings.Contains(q, want) {
			t.Errorf("missing %q in %s", want, q)
		}
	}
	if args[0] != "T" || args[len(args)-1] != MaxLimit+1 {
		t.Fatalf("tenant first, limit clamped: %v", args)
	}
}

func TestBuildTenantOnlyAndBounds(t *testing.T) {
	q, args, err := Build("T", Filter{})
	if err != nil || len(args) != 2 || args[1] != DefaultLimit+1 || !strings.Contains(q, "deleted_at IS NULL") {
		t.Fatalf("%v %v %s", err, args, q)
	}
	if _, _, err := Build("T", Filter{Tags: make([]Tag, MaxTagFilter+1)}); err == nil {
		t.Fatal("tag filter bound")
	}
	if _, _, err := Build("T", Filter{Cursor: "garbage"}); err != ErrBadCursor {
		t.Fatalf("%v", err)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	id := "0a1b2c3d-1111-4222-8333-444455556666"
	c, err := decodeCursor(encodeCursor(ts, id))
	if err != nil || !c.T.Equal(ts) || c.ID != id {
		t.Fatalf("%+v %v", c, err)
	}
}
