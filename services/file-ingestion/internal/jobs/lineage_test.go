package jobs

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestLineageRequiresTraceableBulkDestination(t *testing.T) {
	ok := Lineage{TenantID: "t", SourceObjectKey: "t/uploads/a.csv", Checksum: "abc", PipelineVersion: "v1", Destination: ParquetS3}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := ok
	bad.Destination = "KAFKA"
	if err := bad.Validate(); !errors.Is(err, ErrLineage) {
		t.Fatal(err)
	}
}

func TestBatchLineageValidateAndStableID(t *testing.T) {
	b := BatchLineage{TenantID: "t", JobID: "j", SourceFileID: "f", SourceChecksum: "abc", PipelineVersion: "v1",
		Destination: ClickHouseBulk, FirstRow: 1, LastRow: 5}
	b.BatchID = NewBatchID("abc", "v1", 1, 5)
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	if b.BatchID != NewBatchID("abc", "v1", 1, 5) || b.BatchID == NewBatchID("abc", "v2", 1, 5) || b.BatchID == NewBatchID("abd", "v1", 1, 5) {
		t.Fatal("batch id must depend on checksum, version and range only")
	}
	for _, mut := range []func(*BatchLineage){func(x *BatchLineage) { x.SourceFileID = "" }, func(x *BatchLineage) { x.LastRow = 0 }, func(x *BatchLineage) { x.Destination = "KAFKA" }} {
		c := b
		mut(&c)
		if err := c.Validate(); !errors.Is(err, ErrLineage) {
			t.Fatal(err)
		}
	}
}

func TestMemLineageUpsertsByBatchID(t *testing.T) {
	var m MemLineage
	b := BatchLineage{TenantID: "t", BatchID: "x", Valid: 1}
	_ = m.Record(context.Background(), b)
	b.Valid = 2
	_ = m.Record(context.Background(), b)
	if all := m.All(); len(all) != 1 || all[0].Valid != 2 {
		t.Fatalf("%+v", all)
	}
	var sb strings.Builder
	l := &LogLineage{W: &sb}
	if err := l.Record(context.Background(), b); err != nil || !strings.Contains(sb.String(), `"BatchID":"x"`) || strings.Count(sb.String(), "\n") != 1 {
		t.Fatalf("%v %q", err, sb.String())
	}
}
