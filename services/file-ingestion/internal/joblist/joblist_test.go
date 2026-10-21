package joblist

import (
	"strings"
	"testing"
	"time"
)

func TestBuildParameterisesAndScopesTenant(t *testing.T) {
	cur := encodeCursor(time.Unix(100, 0), "0a1b2c3d-1111-4222-8333-444455556666")
	q, args, err := Build("T", Filter{Status: "FAILED'; DROP", SourceID: "S", Limit: 9999, Cursor: cur})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(q, "DROP") {
		t.Fatalf("value interpolated: %s", q)
	}
	for _, want := range []string{"tenant_id = $1::uuid", "deleted_at IS NULL", "status =", "source_id =", "(created_at, id) <", "ORDER BY created_at DESC, id DESC"} {
		if !strings.Contains(q, want) {
			t.Errorf("missing %q in %s", want, q)
		}
	}
	for _, banned := range []string{"payload", "idempotency_key", "locked_by"} {
		if strings.Contains(q, banned) {
			t.Errorf("selects %s", banned)
		}
	}
	if args[0] != "T" || args[len(args)-1] != MaxLimit+1 {
		t.Fatalf("%v", args)
	}
}

func TestBuildDefaultsAndBadCursor(t *testing.T) {
	_, args, err := Build("T", Filter{})
	if err != nil || len(args) != 2 || args[1] != DefaultLimit+1 {
		t.Fatalf("%v %v", err, args)
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

func TestCountsDropFieldNames(t *testing.T) {
	m := Counts(`{"contains_pii":{"count":3,"fields":["email","ssn"]},"contains_geo":{"count":1}}`)
	if len(m) != 2 || m["contains_pii"] != 3 || m["contains_geo"] != 1 {
		t.Fatalf("%v", m)
	}
	if len(Counts("not json")) != 0 {
		t.Fatal("bad json must give empty map")
	}
}

func TestSafeError(t *testing.T) {
	if SafeError("CREDENTIALS_DETECTED") != "CREDENTIALS_DETECTED" || SafeError("lease_expired") != "lease_expired" {
		t.Fatal("codes pass")
	}
	if SafeError("dial tcp 10.0.0.1:5432: refused") != "" || SafeError(strings.Repeat("A", 65)) != "" {
		t.Fatal("free text must be hidden")
	}
}
