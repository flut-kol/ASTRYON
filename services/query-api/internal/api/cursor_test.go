package api

import (
	"strings"
	"testing"
	"time"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")

func TestCursorRoundTrip(t *testing.T) {
	c := NewCursors(testKey, time.Minute)
	at := time.Date(2026, 1, 2, 3, 4, 5, 678000000, time.UTC)
	s := c.Encode("events", tenantA, "h", at, srcID)
	gt, id, err := c.Decode(s, "events", tenantA, "h")
	if err != nil || !gt.Equal(at) || id != srcID {
		t.Fatalf("got %v %v %v", gt, id, err)
	}
}

func TestCursorRejects(t *testing.T) {
	c := NewCursors(testKey, time.Minute)
	at := time.Now()
	s := c.Encode("events", tenantA, "h", at, srcID)
	other := NewCursors([]byte("ffffffffffffffffffffffffffffffff"), time.Minute)

	parts := strings.Split(s, ".")
	flip := func(x string) string {
		b := []byte(x)
		if b[2] == 'A' {
			b[2] = 'B'
		} else {
			b[2] = 'A'
		}
		return string(b)
	}
	cases := map[string]string{
		"payload tampered": flip(parts[0]) + "." + parts[1],
		"sig tampered":     parts[0] + "." + flip(parts[1]),
		"no sig":           parts[0],
		"empty":            "",
		"garbage":          "!!!.???",
		"extra part":       s + ".x",
		"huge":             strings.Repeat("a", 2000),
		"other key":        other.Encode("events", tenantA, "h", at, srcID),
	}
	for name, cur := range cases {
		if _, _, err := c.Decode(cur, "events", tenantA, "h"); err != ErrBadCursor {
			t.Errorf("%s: err=%v", name, err)
		}
	}
	// binding checks
	if _, _, err := c.Decode(s, "events", tenantB, "h"); err == nil {
		t.Error("cross-tenant replay accepted")
	}
	if _, _, err := c.Decode(s, "events", tenantA, "other-query"); err == nil {
		t.Error("cross-query replay accepted")
	}
	if _, _, err := c.Decode(s, "jobs", tenantA, "h"); err == nil {
		t.Error("cross-kind replay accepted")
	}
	// expiry
	c.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, _, err := c.Decode(s, "events", tenantA, "h"); err == nil {
		t.Error("expired cursor accepted")
	}
}

func TestRateLimiter(t *testing.T) {
	l := NewRateLimiter(1, 2)
	now := time.Now()
	l.now = func() time.Time { return now }
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatal("burst denied")
		}
	}
	if ok, ra := l.Allow("a"); ok || ra < 1 {
		t.Fatalf("expected deny with retry-after, got %v %d", ok, ra)
	}
	if ok, _ := l.Allow("b"); !ok {
		t.Fatal("keys must be independent")
	}
	now = now.Add(2 * time.Second)
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("refill failed")
	}
}
