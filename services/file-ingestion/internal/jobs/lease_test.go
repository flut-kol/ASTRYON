package jobs

import (
	"errors"
	"testing"
	"time"
)

func TestLeasePreventsLiveStealAndAllowsExpiredClaim(t *testing.T) {
	now := time.Unix(100, 0)
	l, err := (Lease{}).Acquire("a", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Acquire("b", now.Add(time.Second), time.Minute); !errors.Is(err, ErrLeaseOwner) {
		t.Fatal(err)
	}
	got, err := l.Acquire("b", now.Add(time.Minute), time.Minute)
	if err != nil || got.Owner != "b" {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestHeartbeatRequiresLiveOwner(t *testing.T) {
	now := time.Unix(100, 0)
	l := Lease{Owner: "a", ExpiresAt: now.Add(time.Minute)}
	if _, err := l.Heartbeat("b", now, time.Minute); !errors.Is(err, ErrLeaseOwner) {
		t.Fatal(err)
	}
	if _, err := l.Heartbeat("a", now.Add(time.Minute), time.Minute); !errors.Is(err, ErrLeaseOwner) {
		t.Fatal(err)
	}
}
