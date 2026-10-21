package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
)

type pqRow struct {
	ID   int64  `parquet:"id"`
	Note string `parquet:"note,optional"`
}

func pqFile(t *testing.T, n, perGroup int, note func(int) string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[pqRow](&buf, parquet.MaxRowsPerRowGroup(int64(perGroup)))
	rows := make([]pqRow, n)
	for i := range rows {
		rows[i] = pqRow{ID: int64(i + 1), Note: note(i)}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type pqSource struct {
	fakeSource
	data []byte
	fp   string
}

func (p pqSource) OpenAt(context.Context, jobs.Job) (io.ReaderAt, int64, string, error) {
	return bytes.NewReader(p.data), int64(len(p.data)), p.fp, nil
}

func pqJob() jobs.Job {
	return jobs.Job{ID: "j1", TenantID: "t1", FileObjectID: "f1", ContentType: "application/vnd.apache.parquet; charset=binary"}
}

func runPQ(t *testing.T, st *fakeStore, data []byte, sink Sink, lin jobs.LineageRecorder, batch int) {
	t.Helper()
	if st.job.ContentType == "" {
		st.job.ContentType = pqJob().ContentType
		st.job.ID, st.job.TenantID, st.job.FileObjectID = "j1", "t1", "f1"
	}
	w := New(Config{Store: st, Source: pqSource{data: data, fp: "fp"}, Sink: sink, BatchSize: batch, Heartbeat: time.Hour,
		Policies: classify.PolicySet{Default: classify.Allow}, Lineage: lin})
	_, _ = w.RunOnce(context.Background())
}

func TestParquetProcessesRowGroupsAndCheckpointsOnlyAtGroupEnds(t *testing.T) {
	data := pqFile(t, 7, 3, func(i int) string { return "x" })
	st, sink, lin := &fakeStore{}, &capSink{}, &jobs.MemLineage{}
	runPQ(t, st, data, sink, lin, 2) // sink batches of 2 inside groups of 3
	if !st.succeeded || st.processed != 7 || st.failed != 0 || len(sink.payloads) != 7 {
		t.Fatalf("%+v sink=%d", st, len(sink.payloads))
	}
	if len(st.cps) != 3 {
		t.Fatalf("want one checkpoint per row group, got %+v", st.cps)
	}
	if c := st.cps[0]; c.NextGroup != 1 || c.NextOffset != 3 || c.NextRecordNumber != 4 {
		t.Fatalf("%+v", c)
	}
	if c := st.cps[2]; c.NextGroup != 3 || c.NextOffset != 7 || c.NextRecordNumber != 8 {
		t.Fatalf("%+v", c)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(sink.payloads[0]), &m); err != nil || m["id"] != float64(1) {
		t.Fatal(m, err)
	}
	// lineage: batches [1-2] [3] [4-5] [6] [7]; every one traces to the source file and checksum
	all := lin.All()
	if len(all) != 5 {
		t.Fatalf("%+v", all)
	}
	rows := 0
	for _, b := range all {
		if b.SourceFileID != "f1" || b.SourceChecksum != "fp" || b.JobID != "j1" || b.PipelineVersion != "file-ingestion/1" || b.Destination != jobs.ClickHouseBulk || b.Validate() != nil {
			t.Fatalf("%+v", b)
		}
		rows += b.Valid
	}
	if rows != 7 {
		t.Fatal(rows)
	}
}

func TestParquetResumesAtNextRowGroup(t *testing.T) {
	data := pqFile(t, 7, 3, func(int) string { return "x" })
	full := &fakeStore{}
	fullSink := &capSink{}
	runPQ(t, full, data, fullSink, nil, 2)
	raw, _ := json.Marshal(full.cps[0]) // after group 0
	st, sink := &fakeStore{job: jobs.Job{Checkpoint: raw}}, &capSink{}
	runPQ(t, st, data, sink, nil, 2)
	if !st.succeeded || st.processed != 4 || len(sink.keys) != 4 || sink.keys[0] != fullSink.keys[3] {
		t.Fatalf("%+v keys=%v", st, sink.keys)
	}
}

func TestParquetSourceChangedAndBadFile(t *testing.T) {
	data := pqFile(t, 4, 2, func(int) string { return "x" })
	full := &fakeStore{}
	runPQ(t, full, data, &capSink{}, nil, 2)
	raw, _ := json.Marshal(full.cps[0])
	st := &fakeStore{job: jobs.Job{Checkpoint: raw}}
	st.job.ContentType = pqJob().ContentType
	w := New(Config{Store: st, Source: pqSource{data: data, fp: "OTHER"}, Sink: &capSink{}, Heartbeat: time.Hour, Policies: classify.PolicySet{Default: classify.Allow}})
	_, _ = w.RunOnce(context.Background())
	if st.failCode != "source_changed" || st.failRetry {
		t.Fatalf("%+v", st)
	}
	bad := &fakeStore{}
	runPQ(t, bad, []byte("not parquet at all, sorry"), &capSink{}, nil, 2)
	if bad.failCode != "parquet_invalid" || bad.failRetry || bad.succeeded {
		t.Fatalf("%+v", bad)
	}
}

func TestParquetNeedsRandomAccessSource(t *testing.T) {
	st := &fakeStore{job: pqJob()}
	w := New(Config{Store: st, Source: fakeSource{"", "fp"}, Sink: &capSink{}, Heartbeat: time.Hour, Policies: classify.PolicySet{Default: classify.Allow}})
	_, _ = w.RunOnce(context.Background())
	if st.failCode != "parquet_unsupported_source" || st.failRetry {
		t.Fatalf("%+v", st)
	}
}

func TestParquetCredentialsPolicyQuarantinesBeforeSink(t *testing.T) {
	data := pqFile(t, 4, 2, func(i int) string {
		if i == 3 {
			return "password=hunter2hunter2 AKIAABCDEFGHIJKLMNOP"
		}
		return "fine"
	})
	st, sink := &fakeStore{}, &capSink{}
	w := New(Config{Store: st, Source: pqSource{data: data, fp: "fp"}, Sink: sink, Heartbeat: time.Hour}) // zero policy quarantines
	st.job = pqJob()
	_, _ = w.RunOnce(context.Background())
	if strings.TrimSpace(st.quarCode) == "" || len(sink.payloads) != 0 || st.succeeded {
		t.Fatalf("%+v sink=%d", st, len(sink.payloads))
	}
}
