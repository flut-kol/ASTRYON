package replay

import (
	"errors"
	"testing"
	"time"
)

func TestReplayDryRunRequestValidation(t *testing.T) {
	valid := ReplayDryRunRequest{
		SourceID:        "00000000-0000-4000-8000-000000000001",
		OccurredFrom:    time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		OccurredTo:      time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
		PipelineVersion: 2,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []ReplayDryRunRequest{
		{},
		func() ReplayDryRunRequest { r := valid; r.SourceID = "source"; return r }(),
		func() ReplayDryRunRequest { r := valid; r.PipelineVersion = 0; return r }(),
		func() ReplayDryRunRequest { r := valid; r.OccurredTo = r.OccurredFrom; return r }(),
		func() ReplayDryRunRequest {
			r := valid
			r.OccurredTo = r.OccurredFrom.Add(maxReplayRange + time.Second)
			return r
		}(),
	}
	for i, request := range tests {
		if err := request.Validate(); !errors.Is(err, ErrInvalidReplayRequest) {
			t.Fatalf("case %d: %v", i, err)
		}
	}
}
