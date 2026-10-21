package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/parse"
	malwarescan "github.com/astryon/file-ingestion/internal/scanner"
)

const file = "{\"a\":1}\n{\"a\":2}\nnot json\n{\"a\":4}\n{\"a\":5}\n"

type fakeStore struct {
	mu         sync.Mutex
	job        jobs.Job
	claimed    bool
	cps        []parse.Checkpoint
	processed  int64
	failed     int64
	succeeded  bool
	failCode   string
	failRetry  bool
	hbErr      error
	saveErr    error
	deltas     []classify.Labels
	setLabels  classify.Labels
	quarCode   string
	quarLabels classify.Labels
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
func (s *fakeStore) SaveCheckpoint(_ context.Context, _ jobs.Job, c json.RawMessage, p, f int64, l classify.Labels) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	var cp parse.Checkpoint
	_ = json.Unmarshal(c, &cp)
	s.cps = append(s.cps, cp)
	if len(l) > 0 {
		s.deltas = append(s.deltas, l)
	}
	s.processed += p
	s.failed += f
	return nil
}
func (s *fakeStore) SetLabels(_ context.Context, _ jobs.Job, l classify.Labels) error {
	s.setLabels = l
	return nil
}
func (s *fakeStore) Quarantine(_ context.Context, _ jobs.Job, code string, l classify.Labels) error {
	s.quarCode, s.quarLabels = code, l
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

type fakeMalwareScanner struct {
	verdict malwarescan.Verdict
	called  bool
	path    string
}

func (f *fakeMalwareScanner) Scan(_ context.Context, in malwarescan.File) malwarescan.Verdict {
	f.called, f.path = true, in.Path
	return f.verdict
}

func TestMalwareScannerRunsBeforeSinkAndCleansStage(t *testing.T) {
	ms := &fakeMalwareScanner{verdict: malwarescan.Verdict{Status: malwarescan.Clean}}
	st, sink := &fakeStore{}, &fakeSink{}
	w := New(Config{Store: st, Source: fakeSource{file, "fp1"}, Sink: sink, BatchSize: 2,
		Heartbeat: time.Hour, MalwareScanner: ms, ScannerStageDir: t.TempDir()})
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !ms.called || ms.path == "" || !st.succeeded || len(sink.keys) == 0 {
		t.Fatalf("scanner=%+v store=%+v sink=%+v", ms, st, sink)
	}
	if _, err := os.Stat(ms.path); !os.IsNotExist(err) {
		t.Fatalf("staged scanner file remains: %v", err)
	}
}

func TestMalwareScannerQuarantinesPositiveVerdict(t *testing.T) {
	for _, status := range []malwarescan.Status{malwarescan.Infected, malwarescan.Encrypted} {
		ms := &fakeMalwareScanner{verdict: malwarescan.Verdict{Status: status}}
		st, sink := &fakeStore{}, &fakeSink{}
		w := New(Config{Store: st, Source: fakeSource{file, "fp1"}, Sink: sink, MalwareScanner: ms, ScannerStageDir: t.TempDir(), Heartbeat: time.Hour})
		if _, err := w.RunOnce(context.Background()); err != nil {
			t.Fatalf("%s: %v", status, err)
		}
		if st.quarCode == "" || st.succeeded || len(sink.keys) != 0 {
			t.Fatalf("%s: store=%+v sink=%+v", status, st, sink)
		}
	}
}

func TestMalwareScannerErrorRetriesAndNeverSinks(t *testing.T) {
	ms := &fakeMalwareScanner{verdict: malwarescan.Verdict{Status: malwarescan.ScanError, ReasonCode: "provider_unavailable", Retryable: true}}
	st, sink := &fakeStore{}, &fakeSink{}
	w := New(Config{Store: st, Source: fakeSource{file, "fp1"}, Sink: sink, MalwareScanner: ms, ScannerStageDir: t.TempDir(), Heartbeat: time.Hour})
	if _, err := w.RunOnce(context.Background()); err == nil || !st.failRetry || len(sink.keys) != 0 {
		t.Fatalf("err=%v store=%+v sink=%+v", err, st, sink)
	}
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

func TestMetricsCountOutcomes(t *testing.T) {
	st := &fakeStore{}
	w := New(Config{Store: st, Source: fakeSource{file, "fp1"}, Sink: &fakeSink{}, BatchSize: 2, Heartbeat: time.Hour})
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	w.M.WritePrometheus(&b)
	for _, want := range []string{"astryon_file_worker_jobs_claimed_total 1", "astryon_file_worker_jobs_succeeded_total 1",
		"astryon_file_worker_batches_total 3", "astryon_file_worker_records_total 4", "astryon_file_worker_invalid_records_total 1"} {
		if !strings.Contains(b.String(), want+"\n") {
			t.Errorf("missing %q in\n%s", want, b.String())
		}
	}
	st2 := &fakeStore{}
	w2 := New(Config{Store: st2, Source: fakeSource{file, "fp1"}, Sink: &fakeSink{err: errors.New("down")}, BatchSize: 2, Heartbeat: time.Hour})
	_, _ = w2.RunOnce(context.Background())
	if w2.M.SinkErrors.Load() != 1 || w2.M.Failed.Load() != 1 { // fake Fail reports FAILED
		t.Fatalf("sink=%d failed=%d", w2.M.SinkErrors.Load(), w2.M.Failed.Load())
	}
}

const secretFile = "{\"user\":\"a\",\"password\":\"hunter2-very-secret\"}\n{\"x\":1}\n"

func runJob(t *testing.T, st *fakeStore, data string, pol classify.PolicySet) (*Worker, *fakeSink) {
	t.Helper()
	sink := &fakeSink{}
	w := New(Config{Store: st, Source: fakeSource{data, "fp"}, Sink: sink, BatchSize: 1, Heartbeat: time.Hour, Policies: pol})
	_, _ = w.RunOnce(context.Background())
	return w, sink
}

func TestCredentialsQuarantineWritesNothingToSink(t *testing.T) {
	st := &fakeStore{}
	w, sink := runJob(t, st, secretFile, classify.PolicySet{})
	if st.quarCode != classify.CodeCredentials || len(sink.keys) != 0 || len(st.cps) != 0 || st.succeeded {
		t.Fatalf("%+v sink=%+v", st, sink)
	}
	if !st.quarLabels.Has(classify.Credentials) {
		t.Fatalf("labels %+v", st.quarLabels)
	}
	var b strings.Builder
	w.M.WritePrometheus(&b)
	if !strings.Contains(b.String(), `content_labels_total{label="contains_credentials"} 1`) || !strings.Contains(b.String(), `policy_actions_total{action="quarantine"} 1`) {
		t.Fatal(b.String())
	}
}

func TestCredentialsBlockFailsWithCatalogCode(t *testing.T) {
	st := &fakeStore{}
	_, sink := runJob(t, st, secretFile, classify.PolicySet{Default: classify.Block})
	if st.failCode != classify.CodeCredentials || st.failRetry || len(sink.keys) != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestCredentialsAllowTagsAndProcesses(t *testing.T) {
	st := &fakeStore{}
	_, sink := runJob(t, st, secretFile, classify.PolicySet{Default: classify.Allow})
	if !st.succeeded || len(sink.keys) != 2 || len(st.deltas) == 0 || !st.deltas[0].Has(classify.Credentials) {
		t.Fatalf("%+v sink=%+v", st, sink)
	}
}

func TestTenantOverrideBeatsEnvDefault(t *testing.T) {
	st := &fakeStore{job: jobs.Job{TenantID: "t1", SourceID: "s1"}}
	_, sink := runJob(t, st, secretFile, classify.PolicySet{Default: classify.Block, Overrides: map[string]classify.Policy{"t1/s1": classify.Allow}})
	if !st.succeeded || len(sink.keys) != 2 {
		t.Fatalf("%+v", st)
	}
}

func TestProfileOutcomes(t *testing.T) {
	// credentials_bundle blocks, personal_data quarantines PII, config quarantines credentials, geo requires coordinates
	pii := "{\"email\":\"jane@example.com\"}\n"
	cases := []struct {
		name, typ, data    string
		wantQuar, wantFail string
		sunk               int
	}{
		{"bundle blocks credentials", "credentials_bundle", secretFile, "", classify.CodeCredentials, 0},
		{"personal data quarantines pii", "personal_data", pii, "PII_DETECTED", "", 0},
		{"report tags pii and runs", "report", pii, "", "", 1},
		{"config quarantines credentials", "config", secretFile, classify.CodeCredentials, "", 0},
		{"geo without coordinates blocks", "geo", "{\"a\":1}\n", "", "GEO_COORDINATES_REQUIRED", 0},
		{"geo with coordinates runs", "geo", "{\"lat\":10.5,\"lon\":20.1}\n", "", "", 1},
		{"telemetry tags geo", "telemetry", "{\"lat\":10.5,\"lon\":20.1}\n", "", "", 1},
		{"credentials_bundle never sinks", "credentials_bundle", "{\"a\":1}\n", "", "", 0},
	}
	for _, c := range cases {
		st := &fakeStore{job: jobs.Job{SourceType: c.typ}}
		_, sink := runJob(t, st, c.data, classify.PolicySet{Default: classify.Allow})
		if st.quarCode != c.wantQuar || st.failCode != c.wantFail || len(sink.keys) != c.sunk {
			t.Errorf("%s: quar=%q fail=%q sunk=%d", c.name, st.quarCode, st.failCode, len(sink.keys))
		}
	}
}

func TestFailFileProfileRejectsMalformedBeforeSink(t *testing.T) {
	st := &fakeStore{job: jobs.Job{SourceType: "financial"}}
	_, sink := runJob(t, st, "{\"a\":1}\nnot json\n", classify.PolicySet{})
	if st.failCode != "SCHEMA_VIOLATION" || len(sink.keys) != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestMatchedValuesNeverReachLabelsMetricsOrLogs(t *testing.T) {
	secrets := []string{"hunter2-very-secret", "jane@example.com", "AKIAIOSFODNN7EXAMPLE", "4111111111111111"}
	data := "{\"password\":\"hunter2-very-secret\",\"contact\":\"jane@example.com\",\"k\":\"AKIAIOSFODNN7EXAMPLE\",\"note\":\"card 4111 1111 1111 1111\",\"AKIAIOSFODNN7EXAMPLE\":\"bob@example.org\"}\n"
	var logs strings.Builder
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	st := &fakeStore{job: jobs.Job{SourceType: "export"}}
	w, _ := runJob(t, st, data, classify.PolicySet{Default: classify.Allow})
	raw, _ := json.Marshal(st.deltas)
	var prom strings.Builder
	w.M.WritePrometheus(&prom)
	all := string(raw) + logs.String() + prom.String()
	for _, sec := range secrets {
		if strings.Contains(all, sec) || strings.Contains(all, strings.ReplaceAll(sec, "4111111111111111", "4111 1111 1111 1111")) {
			t.Fatalf("secret %q leaked in %s", sec, all)
		}
	}
	if !strings.Contains(string(raw), "(redacted)") {
		t.Fatalf("secret-looking key was not redacted: %s", raw)
	}
}
