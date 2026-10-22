package parquetout

import (
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
)

type stubStore struct {
	job     jobs.Job
	claimed bool
	ok      bool
}

func (s *stubStore) Claim(context.Context) (jobs.Job, error) {
	if s.claimed {
		return jobs.Job{}, jobs.ErrNoJob
	}
	s.claimed = true
	return s.job, nil
}
func (s *stubStore) Heartbeat(context.Context, jobs.Job) error { return nil }
func (s *stubStore) SaveCheckpoint(context.Context, jobs.Job, json.RawMessage, int64, int64, classify.Labels) error {
	return nil
}
func (s *stubStore) SetLabels(context.Context, jobs.Job, classify.Labels) error { return nil }
func (s *stubStore) Quarantine(context.Context, jobs.Job, string, classify.Labels) error {
	return nil
}
func (s *stubStore) Succeed(context.Context, jobs.Job) error { s.ok = true; return nil }
func (s *stubStore) Fail(context.Context, jobs.Job, string, bool) (jobs.State, error) {
	return jobs.Failed, nil
}

type strSource string

func (s strSource) Open(_ context.Context, _ jobs.Job, off int64) (io.ReadCloser, string, error) {
	return io.NopCloser(strings.NewReader(string(s)[off:])), strings.Repeat("f", 64), nil
}
