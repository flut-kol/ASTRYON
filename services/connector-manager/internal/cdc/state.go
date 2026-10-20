package cdc

import (
	"errors"
	"fmt"
)

var ErrTransition = errors.New("invalid CDC sync transition")

type Phase string

const (
	New          Phase = "NEW"
	Snapshotting Phase = "SNAPSHOTTING"
	Tailing      Phase = "TAILING"
	Paused       Phase = "PAUSED"
	Failed       Phase = "FAILED"
)

func Transition(from, to Phase) error {
	if from == to {
		return nil
	}
	if map[Phase]map[Phase]bool{
		New: {Snapshotting: true, Failed: true}, Snapshotting: {Tailing: true, Paused: true, Failed: true},
		Tailing: {Paused: true, Failed: true}, Paused: {Snapshotting: true, Tailing: true, Failed: true},
	}[from][to] {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrTransition, from, to)
}

// Compatible permits additive fields only; removals/type changes require a
// versioned connector migration and cannot silently corrupt the tail stream.
func Compatible(previous, next map[string]string) bool {
	for field, typ := range previous {
		if next[field] != typ {
			return false
		}
	}
	return true
}
