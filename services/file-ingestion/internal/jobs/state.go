// Package jobs defines the durable batch-job state contract.
package jobs

import "fmt"

type State string

const (
	New       State = "NEW"
	Queued    State = "QUEUED"
	Running   State = "RUNNING"
	Retrying  State = "RETRYING"
	Succeeded State = "SUCCEEDED"
	Failed    State = "FAILED"
	Cancelled State = "CANCELLED"
	// Quarantined: held back by a content policy (credentials/PII); nothing was written to the sink.
	Quarantined State = "QUARANTINED"
)

func (s State) Terminal() bool {
	return s == Succeeded || s == Failed || s == Cancelled || s == Quarantined
}

// Transition validates a state change before it is persisted. Retrying is
// explicit so an attempt counter and next-run timestamp cannot be skipped.
func Transition(from, to State) error {
	if from == to {
		return nil
	}
	ok := map[State]map[State]bool{
		New:      {Queued: true, Cancelled: true},
		Queued:   {Running: true, Cancelled: true},
		Running:  {Quarantined: true, Succeeded: true, Retrying: true, Failed: true, Cancelled: true},
		Retrying: {Queued: true, Failed: true, Cancelled: true},
	}[from][to]
	if !ok {
		return fmt.Errorf("invalid job transition %s -> %s", from, to)
	}
	return nil
}
