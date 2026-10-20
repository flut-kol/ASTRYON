package jobs

import (
	"errors"
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
