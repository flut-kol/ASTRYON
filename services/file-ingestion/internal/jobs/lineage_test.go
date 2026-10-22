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
	checksum := strings.Repeat("a", 64)
	b := BatchLineage{TenantID: "t", JobID: "j", SourceFileID: "f", SourceChecksum: checksum, PipelineVersion: "v1",
		Destination: ClickHouseBulk, FirstRow: 1, LastRow: 5, Valid: 4, Invalid: 1}
	b.BatchID = NewBatchID(checksum, "v1", 1, 5)
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	if b.BatchID != NewBatchID(checksum, "v1", 1, 5) || b.BatchID == NewBatchID(checksum, "v2", 1, 5) || b.BatchID == NewBatchID(strings.Repeat("b", 64), "v1", 1, 5) {
		t.Fatal("batch id must depend on checksum, version and range only")
	}
	for _, mut := range []func(*BatchLineage){func(x *BatchLineage) { x.SourceFileID = "" }, func(x *BatchLineage) { x.LastRow = 0 }, func(x *BatchLineage) { x.Destination = "KAFKA" }} {
		c := b
		mut(&c)
		if err := c.Validate(); !errors.Is(err, ErrLineage) {
			t.Fatal(err)
		}
	}
	for _, mut := range []func(*BatchLineage){func(x *BatchLineage) { x.SourceChecksum = "not-sha256" }, func(x *BatchLineage) { x.Invalid = 2 }} {
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

func TestPGLineageIsTenantScopedAndIdempotent(t *testing.T) {
	tx := &fakeTx{}
	l := &PGLineage{DB: &fakeDB{tx: tx}}
	b := BatchLineage{TenantID: "tenant-1", JobID: "job-1", SourceFileID: "file-1", SourceChecksum: strings.Repeat("a", 64),
		PipelineVersion: "file-ingestion/1", BatchID: "batch-1", Destination: ClickHouseBulk, FirstRow: 1, LastRow: 3, Valid: 2, Invalid: 1}
	if err := l.Record(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if !tx.committed || len(tx.log) != 2 || !strings.Contains(tx.log[0], "set_config('astryon.tenant_id'") || !strings.Contains(tx.log[1], "ON CONFLICT (tenant_id, batch_id)") {
		t.Fatalf("lineage transaction = committed:%v log:%v", tx.committed, tx.log)
	}
}
