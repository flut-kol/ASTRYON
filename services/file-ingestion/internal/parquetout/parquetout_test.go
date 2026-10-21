package parquetout

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/parse"
	"github.com/astryon/file-ingestion/internal/worker"
)

type memPut struct {
	objs map[string][]byte
	err  error
}

func (m *memPut) Put(_ context.Context, k string, b []byte, ct string) error {
	if m.err != nil {
		return m.err
	}
	if ct != ContentType {
		return errors.New("content type")
	}
	if m.objs == nil {
		m.objs = map[string][]byte{}
	}
	m.objs[k] = append([]byte(nil), b...)
	return nil
}

type innerSink struct{ valid, invalid int }

func (i *innerSink) Accept(_ context.Context, _ jobs.Job, v, bad []worker.Item) error {
	i.valid += len(v)
	i.invalid += len(bad)
	return nil
}

func items(nums ...int) []worker.Item {
	var out []worker.Item
	for _, n := range nums {
		out = append(out, worker.Item{Record: parse.Record{Number: n, Raw: []byte(`{"a":1}`)}, DedupKey: "k"})
	}
	return out
}

func TestSinkWritesReadablePartWithDeterministicKeyAndForwards(t *testing.T) {
	p, in := &memPut{}, &innerSink{}
	s := &Sink{Put: p, Prefix: "/normalized/", Pipeline: "v1", Inner: in}
	j := jobs.Job{ID: "j", TenantID: "t", FileObjectID: "f"}
	if err := s.Accept(context.Background(), j, items(5, 6, 8), items(7)); err != nil {
		t.Fatal(err)
	}
	key := "normalized/tenants/t/f/v1/part-000000000005.parquet"
	if s.OutputKey(j, items(5)) != key || s.OutputKey(j, nil) != "" {
		t.Fatal(s.OutputKey(j, items(5)))
	}
	data, ok := p.objs[key]
	if !ok || in.valid != 3 || in.invalid != 1 {
		t.Fatalf("%v %+v", ok, in)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil || f.NumRows() != 3 {
		t.Fatal(f, err)
	}
	if v, _ := f.Lookup("astryon.pipeline_version"); v != "v1" {
		t.Fatalf("metadata %q", v)
	}
	// replay overwrites the same part
	if err := s.Accept(context.Background(), j, items(5, 6, 8), nil); err != nil || len(p.objs) != 1 {
		t.Fatal(err, len(p.objs))
	}
	// a batch with only invalid rows writes no object
	if err := s.Accept(context.Background(), j, nil, items(9)); err != nil || len(p.objs) != 1 {
		t.Fatal(err)
	}
	p.err = errors.New("s3 down")
	if err := s.Accept(context.Background(), j, items(10), nil); err == nil || in.valid != 6 {
		t.Fatalf("put failure must stop before the inner sink: %v", err)
	}
	if (&Sink{Put: p}).Accept(context.Background(), j, items(1), nil) == nil {
		t.Fatal("pipeline required")
	}
}

func TestWorkerRecordsOutputKeyInLineage(t *testing.T) {
	st := &stubStore{job: jobs.Job{ID: "j", TenantID: "t", FileObjectID: "f", ContentType: "application/x-ndjson"}}
	p, lin := &memPut{}, &jobs.MemLineage{}
	w := worker.New(worker.Config{Store: st, Source: strSource("{\"a\":1}\n{\"a\":2}\n"), Sink: &Sink{Put: p, Prefix: "n", Pipeline: "v9"},
		Heartbeat: time.Hour, Policies: classify.PolicySet{Default: classify.Allow}, Lineage: lin, PipelineVersion: "v9"})
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	all := lin.All()
	if len(all) != 1 || all[0].Destination != jobs.ParquetS3 || all[0].OutputKey != "n/tenants/t/f/v9/part-000000000001.parquet" || all[0].Valid != 2 {
		t.Fatalf("%+v", all)
	}
	if _, ok := p.objs[all[0].OutputKey]; !ok {
		t.Fatal("lineage points at a missing object")
	}
}
