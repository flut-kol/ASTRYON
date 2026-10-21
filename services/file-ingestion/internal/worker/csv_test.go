package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
)

const csvFile = "\xef\xbb\xbfid,note,id,\r\n1,\"multi\nline\",x,y\r\n2,short row\r\n3,bad\"quote,z,w\r\n4,ok,p,q\r\n5,fine,r,s"

type capSink struct {
	payloads []string
	keys     []string
	bad      int
}

func (c *capSink) Accept(_ context.Context, _ jobs.Job, valid, invalid []Item) error {
	for _, v := range valid {
		c.payloads = append(c.payloads, string(v.Record.Raw))
		c.keys = append(c.keys, v.DedupKey)
	}
	c.bad += len(invalid)
	return nil
}

func runCSV(t *testing.T, st *fakeStore, data string, pol classify.PolicySet, batch int) (*Worker, *capSink) {
	t.Helper()
	if st.job.ContentType == "" {
		st.job.ContentType = "text/csv"
	}
	sink := &capSink{}
	w := New(Config{Store: st, Source: fakeSource{data, "fp"}, Sink: sink, BatchSize: batch, Heartbeat: time.Hour, Policies: pol})
	_, _ = w.RunOnce(context.Background())
	return w, sink
}

func TestCSVProcessesWithHeaderKeysAndInvalidRows(t *testing.T) {
	st := &fakeStore{}
	_, sink := runCSV(t, st, csvFile, classify.PolicySet{Default: classify.Allow}, 2)
	if !st.succeeded || st.processed != 3 || st.failed != 2 || sink.bad != 2 {
		t.Fatalf("%+v sink=%+v", st, sink)
	}
	// BOM stripped, blank header -> col4, repeated id -> id_2, quoted newline preserved
	var m map[string]string
	if err := json.Unmarshal([]byte(sink.payloads[0]), &m); err != nil || m["id"] != "1" || m["note"] != "multi\nline" || m["id_2"] != "x" || m["col4"] != "y" {
		t.Fatalf("%v %v", m, err)
	}
	last := st.cps[len(st.cps)-1]
	if last.NextOffset != int64(len(csvFile)) || last.NextRecordNumber != 7 || len(last.Header) != 4 {
		t.Fatalf("%+v", last)
	}
}

func TestCSVResumesFromCheckpointWithHeader(t *testing.T) {
	full := &fakeStore{}
	all := func() *capSink { _, s := runCSV(t, full, csvFile, classify.PolicySet{Default: classify.Allow}, 2); return s }()
	for i := range full.cps {
		raw, _ := json.Marshal(full.cps[i])
		st := &fakeStore{job: jobs.Job{Checkpoint: raw}}
		_, sink := runCSV(t, st, csvFile, classify.PolicySet{Default: classify.Allow}, 2)
		if !st.succeeded || len(sink.payloads) == 0 && i < len(full.cps)-1 && st.processed+st.failed == 0 {
			t.Fatalf("cp %d: %+v", i, st)
		}
		// whatever remained after the checkpoint is exactly the tail of the full run, same keys and payloads
		n := len(sink.payloads)
		if n > 0 && (strings.Join(sink.payloads, "|") != strings.Join(all.payloads[len(all.payloads)-n:], "|") ||
			strings.Join(sink.keys, "|") != strings.Join(all.keys[len(all.keys)-n:], "|")) {
			t.Fatalf("cp %d diverged: %v vs %v", i, sink.payloads, all.payloads)
		}
	}
}

func TestCSVCredentialsQuarantineAndBlockMatchNDJSON(t *testing.T) {
	data := "user,password\na,hunter2-very-secret\nb,x\n"
	st := &fakeStore{}
	w, sink := runCSV(t, st, data, classify.PolicySet{}, 1)
	if st.quarCode != classify.CodeCredentials || len(sink.payloads) != 0 || len(st.cps) != 0 || st.succeeded {
		t.Fatalf("%+v", st)
	}
	raw, _ := json.Marshal(st.quarLabels)
	var prom strings.Builder
	w.M.WritePrometheus(&prom)
	if !strings.Contains(string(raw), "password") || strings.Contains(string(raw)+prom.String(), "hunter2") {
		t.Fatalf("labels must name the column and never the value: %s", raw)
	}
	st = &fakeStore{}
	_, sink = runCSV(t, st, data, classify.PolicySet{Default: classify.Block}, 1)
	if st.failCode != classify.CodeCredentials || st.failRetry || len(sink.payloads) != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestCSVAllowClassifiesWhileProcessing(t *testing.T) {
	st := &fakeStore{}
	_, sink := runCSV(t, st, "email,n\njane@example.com,1\nbob,2\n", classify.PolicySet{Default: classify.Allow}, 1)
	if !st.succeeded || len(sink.payloads) != 2 || len(st.deltas) == 0 || !st.deltas[0].Has(classify.PII) {
		t.Fatalf("%+v", st)
	}
	if got := st.deltas[0][classify.PII].Fields; len(got) != 1 || got[0] != "email" {
		t.Fatalf("fields %v", got)
	}
}

func TestCSVProfilePoliciesAndLimits(t *testing.T) {
	// fail_file profile: the short row fails the file before anything is written
	st := &fakeStore{job: jobs.Job{SourceType: "financial"}}
	_, sink := runCSV(t, st, "a,b\n1,2\n3\n", classify.PolicySet{Default: classify.Allow}, 1)
	if st.failCode != "SCHEMA_VIOLATION" || len(sink.payloads) != 0 {
		t.Fatalf("%+v", st)
	}
	// unparseable header is permanent
	st = &fakeStore{}
	_, _ = runCSV(t, st, "\"a\"b,c\n1,2\n", classify.PolicySet{Default: classify.Allow}, 1)
	if st.failCode != "csv_header_invalid" || st.failRetry {
		t.Fatalf("%+v", st)
	}
	// checkpoint beyond the header without header names
	st = &fakeStore{job: jobs.Job{Checkpoint: json.RawMessage(`{"SourceFingerprint":"fp","NextOffset":4,"NextRecordNumber":2}`)}}
	_, _ = runCSV(t, st, "a,b\n1,2\n", classify.PolicySet{Default: classify.Allow}, 1)
	if st.failCode != "bad_checkpoint" || st.failRetry {
		t.Fatalf("%+v", st)
	}
}

func TestFormatChosenFromUploadRecord(t *testing.T) {
	for ct, want := range map[string]format{"text/csv": formatCSV, "TEXT/CSV; charset=utf-8": formatCSV, "application/x-ndjson": formatNDJSON, "": formatNDJSON} {
		if got := formatOf(jobs.Job{ContentType: ct}); got != want {
			t.Fatalf("%q -> %v", ct, got)
		}
	}
}
