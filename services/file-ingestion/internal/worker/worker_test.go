package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/parse"
)

const file = "{\"a\":1}\n{\"a\":2}\nnot json\n{\"a\":4}\n{\"a\":5}\n"

type fakeStore struct {
	mu        sync.Mutex
	job       jobs.Job
	claimed   bool
	cps       []parse.Checkpoint
	processed int64
	failed    int64
	succeeded bool
	failCode  string
	failRetry bool
	hbErr     error
	saveErr   error
}

func (s *fakeStore) Claim(context.Context) (jobs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed {
		return jobs.Job{}, jobs.ErrNoJob
	}
	s.claimed = true
	return s.job, nil
}
func (s *fakeStore) Heartbeat(context.Context, jobs.Job) error { return s.hbErr }
func (s *fakeStore) SaveCheckpoint(_ context.Context, _ jobs.Job, c json.RawMessage, p, f int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	var cp parse.Checkpoint
	_ = json.Unmarshal(c, &cp)
	s.cps = append(s.cps, cp)
	s.processed += p
	s.failed += f
	return nil
}
func (s *fakeStore) Succeed(context.Context, jobs.Job) error { s.succeeded = true; return nil }
func (s *fakeStore) Fail(_ context.Context, _ jobs.Job, code string, retry bool) (jobs.State, error) {
	s.failCode, s.failRetry = code, retry
	return jobs.Failed, nil
}

type fakeSource struct {
	data, fp string
}

func (f fakeSource) Open(_ context.Context, _ jobs.Job, off int64) (io.ReadCloser, string, error) {
	return io.NopCloser(strings.NewReader(f.data[off:])), f.fp, nil
}

type fakeSink struct {
	keys []string
	bad  int
	err  error
}

func (f *fakeSink) Accept(_ context.Context, _ jobs.Job, valid, invalid []Item) error {
	if f.err != nil {
		return f.err
	}
	for _, v := range valid {
		f.keys = append(f.keys, v.DedupKey)
	}
	f.bad += len(invalid)
	return nil
}

func run(t *testing.T, st *fakeStore, src Source, sink Sink) error {
	t.Helper()
	w := New(Config{Store: st, Source: src, Sink: sink, BatchSize: 2, Heartbeat: time.Hour})
	_, err := w.RunOnce(context.Background())
	return err
}

func TestProcessesFileInBatchesAndCheckpointsEachBatch(t *testing.T) {
	st, sink := &fakeStore{}, &fakeSink{}
	if err := run(t, st, fakeSource{file, "fp1"}, sink); err != nil {
		t.Fatal(err)
	}
	if !st.succeeded || st.processed != 4 || st.failed != 1 || sink.bad != 1 || len(sink.keys) != 4 {
		t.Fatalf("%+v sink=%+v", st, sink)
	}
	if len(st.cps) != 3 || st.cps[0].NextRecordNumber != 3 || st.cps[2].NextOffset != int64(len(file)) {
		t.Fatalf("checkpoints: %+v", st.cps)
	}
}

func TestResumesFromCheckpointWithoutReprocessing(t *testing.T) {
	full := &fakeStore{}
	if err := run(t, full, fakeSource{file, "fp1"}, &fakeSink{}); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(full.cps[0]) // after the first batch (2 records)
	st, sink := &fakeStore{job: jobs.Job{Fence: 2, Checkpoint: raw}}, &fakeSink{}
	if err := run(t, st, fakeSource{file, "fp1"}, sink); err != nil {
		t.Fatal(err)
	}
	if !st.succeeded || st.processed+st.failed != 3 || len(sink.keys) != 2 {
		t.Fatalf("resume reprocessed or skipped: %+v sink=%+v", st, sink)
	}
	// keys are stable across restarts (offset based), so replays dedupe
	all := &fakeSink{}
	_ = run(t, &fakeStore{}, fakeSource{file, "fp1"}, all)
	if all.keys[2] != sink.keys[0] {
		t.Fatal("dedup key changed after resume")
	}
}

func TestSourceChangedIsPermanent(t *testing.T) {
	full := &fakeStore{}
	_ = run(t, full, fakeSource{file, "fp1"}, &fakeSink{})
	raw, _ := json.Marshal(full.cps[0])
	st := &fakeStore{job: jobs.Job{Checkpoint: raw}}
	if err := run(t, st, fakeSource{file, "OTHER"}, &fakeSink{}); err == nil || st.failCode != "source_changed" || st.failRetry {
		t.Fatalf("err=%v %+v", err, st)
	}
}

func TestSinkFailureIsRetryableAndDoesNotAdvanceCheckpoint(t *testing.T) {
	st := &fakeStore{}
	if err := run(t, st, fakeSource{file, "fp1"}, &fakeSink{err: errors.New("down")}); err == nil || !st.failRetry || len(st.cps) != 0 || st.succeeded {
		t.Fatalf("err=%v %+v", err, st)
	}
	st = &fakeStore{}
	_ = run(t, st, fakeSource{file, "fp1"}, &fakeSink{err: Permanent("schema")})
	if st.failCode != "schema" || st.failRetry {
		t.Fatalf("%+v", st)
	}
}

func TestFencedWorkerWritesNothingMore(t *testing.T) {
	st := &fakeStore{saveErr: jobs.ErrFenced}
	if err := run(t, st, fakeSource{file, "fp1"}, &fakeSink{}); !errors.Is(err, jobs.ErrFenced) || st.succeeded || st.failCode != "" {
		t.Fatalf("err=%v %+v", err, st)
	}
}

func TestHeartbeatLossStopsProcessing(t *testing.T) {
	st := &fakeStore{hbErr: jobs.ErrFenced}
	w := New(Config{Store: st, Sink: &fakeSink{}, BatchSize: 1, Heartbeat: time.Millisecond,
		Source: slowSource{}})
	if _, err := w.RunOnce(context.Background()); !errors.Is(err, jobs.ErrFenced) || st.succeeded {
		t.Fatalf("err=%v succeeded=%v", err, st.succeeded)
	}
}

type slowSource struct{}

func (slowSource) Open(ctx context.Context, _ jobs.Job, _ int64) (io.ReadCloser, string, error) {
	pr, pw := io.Pipe()
	go func() { // endless slow stream, ends only when the worker aborts
		for ctx.Err() == nil {
			time.Sleep(2 * time.Millisecond)
			if _, err := pw.Write([]byte("{\"a\":1}\n")); err != nil {
				return
			}
		}
		pw.Close()
	}()
	return pr, "fp", nil
}
