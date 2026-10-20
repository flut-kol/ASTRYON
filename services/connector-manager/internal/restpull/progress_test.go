package restpull

import (
	"testing"
	"time"
)

func TestProgressOnlyAdvancesAfterSuccessfulHandling(t *testing.T) {
	now := time.Unix(10, 0)
	p := Progress{Checkpoint: Checkpoint{Cursor: "old", UpdatedAt: now}}
	failed, err := p.Fail(now.Add(time.Second), time.Second, time.Minute, 0, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Checkpoint.Cursor != "old" || failed.ConsecutiveFailures != 1 || !failed.NextAttemptAt.Equal(now.Add(6*time.Second)) {
		t.Fatalf("unexpected failed progress: %#v", failed)
	}
	succeeded, err := failed.Succeed("new", now.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if succeeded.Checkpoint.Cursor != "new" || succeeded.ConsecutiveFailures != 0 || !succeeded.NextAttemptAt.IsZero() {
		t.Fatalf("unexpected successful progress: %#v", succeeded)
	}
}

func TestProgressKeepsCheckpointWhenSourceHasNoNextCursor(t *testing.T) {
	p := Progress{Checkpoint: Checkpoint{Cursor: "old", UpdatedAt: time.Unix(1, 0)}, ConsecutiveFailures: 2}
	got, err := p.Succeed("", time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got.Checkpoint.Cursor != "old" || got.ConsecutiveFailures != 0 {
		t.Fatalf("unexpected progress: %#v", got)
	}
}
