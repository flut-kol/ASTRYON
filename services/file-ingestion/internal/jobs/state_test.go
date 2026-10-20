package jobs

import "testing"

func TestTransitions(t *testing.T) {
	for _, tc := range []struct {
		from, to State
		ok       bool
	}{
		{New, Queued, true}, {Queued, Running, true}, {Running, Retrying, true},
		{Retrying, Queued, true}, {Running, Succeeded, true}, {Succeeded, Running, false},
		{New, Succeeded, false}, {Queued, Retrying, false},
	} {
		err := Transition(tc.from, tc.to)
		if (err == nil) != tc.ok {
			t.Fatalf("%s -> %s err=%v", tc.from, tc.to, err)
		}
	}
}

func TestTerminal(t *testing.T) {
	if !Succeeded.Terminal() || !Failed.Terminal() || !Cancelled.Terminal() || Running.Terminal() {
		t.Fatal("wrong terminal classification")
	}
}
