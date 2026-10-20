package jobs

import (
	"errors"
	"fmt"
)

var ErrLineage = errors.New("invalid lineage")

type Destination string

const (
	ParquetS3      Destination = "PARQUET_S3"
	ClickHouseBulk Destination = "CLICKHOUSE_BULK"
)

type Lineage struct {
	TenantID, SourceObjectKey, Checksum, PipelineVersion string
	Destination                                          Destination
}

// Validate ensures a bulk result can be traced to an authenticated tenant,
// immutable input object and processing version. Kafka is deliberately absent:
// large files use object storage and bulk paths, not per-row event transport.
func (l Lineage) Validate() error {
	if l.TenantID == "" || l.SourceObjectKey == "" || l.Checksum == "" || l.PipelineVersion == "" {
		return ErrLineage
	}
	if l.Destination != ParquetS3 && l.Destination != ClickHouseBulk {
		return fmt.Errorf("%w: destination", ErrLineage)
	}
	return nil
}
