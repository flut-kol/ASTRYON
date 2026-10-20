package export

import (
	"context"
	"testing"
	"time"
)

type fakeUploads struct {
	ups     []Upload
	aborted []string
}

func (f *fakeUploads) ListMultipartUploads(context.Context, string) ([]Upload, error) {
	return f.ups, nil
}
func (f *fakeUploads) AbortUpload(_ context.Context, key, id string) error {
	f.aborted = append(f.aborted, key+"#"+id)
	return nil
}

type fakeReader map[string]Job

func (f fakeReader) Get(tenant, id string) (Job, error) {
	if j, ok := f[tenant+"/"+id]; ok {
		return j, nil
	}
	return Job{}, ErrJobNotFound
}

func TestUploadSweeperAbortsOnlyOrphans(t *testing.T) {
	const ten = "11111111-1111-1111-1111-111111111111"
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	key := func(id string) string { return "exports/" + ten + "/" + id + ".ndjson" }
	f := &fakeUploads{ups: []Upload{
		{key("running-job"), "u1", now.Add(-10 * time.Minute)}, // RUNNING, young: keep
		{key("failed-job"), "u2", now.Add(-2 * time.Minute)},   // FAILED worker_lost: abort
		{key("failed-new"), "u3", now.Add(-10 * time.Second)},  // FAILED but inside grace: keep
		{key("unknown-job"), "u4", now.Add(-5 * time.Minute)},  // no row: abort
		{key("running-old"), "u5", now.Add(-3 * time.Hour)},    // RUNNING but older than MaxAge: abort
		{key("done-job"), "u6", now.Add(-5 * time.Minute)},     // SUCCEEDED (stale upload id): abort
		{"exports/foreign.bin", "u7", now.Add(-10 * time.Minute)},
		{"other/x.bin", "u8", now.Add(-5 * time.Hour)}, // outside the sweeper prefix: never touched
	}}
	jobs := fakeReader{
		ten + "/running-job": {Status: StatusRunning},
		ten + "/failed-job":  {Status: StatusFailed},
		ten + "/failed-new":  {Status: StatusFailed},
		ten + "/running-old": {Status: StatusRunning},
		ten + "/done-job":    {Status: StatusSucceeded},
	}
	s := &UploadSweeper{Store: f, Jobs: jobs, Now: func() time.Time { return now }}
	n, err := s.Sweep(context.Background())
	if err != nil || n != 4 {
		t.Fatalf("aborted %d err %v: %v", n, err, f.aborted)
	}
	want := map[string]bool{key("failed-job") + "#u2": true, key("unknown-job") + "#u4": true, key("running-old") + "#u5": true, key("done-job") + "#u6": true}
	for _, a := range f.aborted {
		if !want[a] {
			t.Fatalf("unexpected abort %s", a)
		}
	}
}
