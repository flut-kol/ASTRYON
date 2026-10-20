package restpull

import (
	"errors"
	"testing"
	"time"
)

func TestBackoffCaps(t *testing.T) {
	d, err := NextDelay(10, time.Second, 10*time.Second, 0)
	if err != nil || d != 10*time.Second {
		t.Fatalf("%v %v", d, err)
	}
}
func TestCheckpointNeverMovesBackInTime(t *testing.T) {
	c := Checkpoint{Cursor: "a", UpdatedAt: time.Unix(2, 0)}
	if _, err := c.Advance("b", time.Unix(1, 0)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
