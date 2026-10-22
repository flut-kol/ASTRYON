package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"sync"
)

var ErrLineage = errors.New("invalid lineage")
var sha256Hex = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

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

// BatchLineage ties one processed batch of rows back to its immutable source file and the pipeline version that
// produced it: source file (FileObjectID + SHA-256) -> batch (BatchID, row range) -> output (OutputKey, if the sink
// writes objects). It carries identifiers and counts only, never row values.
type BatchLineage struct {
	TenantID, JobID, SourceFileID, SourceChecksum, PipelineVersion string
	BatchID                                                        string // deterministic: replays of the same batch upsert
	Destination                                                    Destination
	FirstRow, LastRow                                              int64 // 1-based record numbers (CSV: the header is record 1)
	Valid, Invalid                                                 int
	OutputKey                                                      string // object written for the batch, "" for row sinks
}

// NewBatchID is stable across retries and restarts for the same source content, pipeline version and row range.
func NewBatchID(checksum, pipelineVersion string, firstRow, lastRow int64) string {
	sum := sha256.Sum256([]byte(checksum + "|" + pipelineVersion + "|" + strconv.FormatInt(firstRow, 10) + "-" + strconv.FormatInt(lastRow, 10)))
	return hex.EncodeToString(sum[:16])
}

func (b BatchLineage) Validate() error {
	if b.TenantID == "" || b.JobID == "" || b.SourceFileID == "" || b.SourceChecksum == "" || b.PipelineVersion == "" || b.BatchID == "" ||
		b.FirstRow < 1 || b.LastRow < b.FirstRow {
		return ErrLineage
	}
	if b.Destination != ParquetS3 && b.Destination != ClickHouseBulk {
		return fmt.Errorf("%w: destination", ErrLineage)
	}
	if !sha256Hex.MatchString(b.SourceChecksum) {
		return fmt.Errorf("%w: source checksum", ErrLineage)
	}
	if b.Valid < 0 || b.Invalid < 0 || b.Valid+b.Invalid != int(b.LastRow-b.FirstRow+1) {
		return fmt.Errorf("%w: row counts", ErrLineage)
	}
	return nil
}

// LineageRecorder persists batch lineage. Record must be idempotent on (TenantID, BatchID): a crash between the
// sink write and the checkpoint replays the batch.
type LineageRecorder interface {
	Record(ctx context.Context, b BatchLineage) error
}

// PGLineage persists batch lineage in PostgreSQL. It uses the same tenant RLS
// session setting as the job store and upserts by the deterministic batch ID,
// making sink/checkpoint crash retries idempotent.
type PGLineage struct{ DB DB }

const sqlLineage = `INSERT INTO file_batch_lineage
	(tenant_id, batch_id, job_id, source_file_id, source_checksum, pipeline_version, destination,
	 first_row, last_row, valid_rows, invalid_rows, output_key)
	VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5, $6, $7, $8, $9, $10, $11, $12)
	ON CONFLICT (tenant_id, batch_id) DO UPDATE SET
	 job_id = EXCLUDED.job_id, source_file_id = EXCLUDED.source_file_id,
	 source_checksum = EXCLUDED.source_checksum, pipeline_version = EXCLUDED.pipeline_version,
	 destination = EXCLUDED.destination, first_row = EXCLUDED.first_row, last_row = EXCLUDED.last_row,
	 valid_rows = EXCLUDED.valid_rows, invalid_rows = EXCLUDED.invalid_rows, output_key = EXCLUDED.output_key`

func (l *PGLineage) Record(ctx context.Context, b BatchLineage) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if l == nil || l.DB == nil {
		return errors.New("jobs: nil PostgreSQL lineage DB")
	}
	tx, err := l.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, b.TenantID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, sqlLineage, b.TenantID, b.BatchID, b.JobID, b.SourceFileID, b.SourceChecksum,
		b.PipelineVersion, string(b.Destination), b.FirstRow, b.LastRow, b.Valid, b.Invalid, b.OutputKey); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// LogLineage writes one JSON line per batch (for log shipping). It is not a durable store.
type LogLineage struct {
	W  io.Writer
	mu sync.Mutex
}

func (l *LogLineage) Record(_ context.Context, b BatchLineage) error {
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err = l.W.Write(append(raw, '\n'))
	return err
}

// MemLineage keeps lineage in memory keyed by (tenant, batch id); used by tests and as a reference implementation.
type MemLineage struct {
	mu sync.Mutex
	m  map[string]BatchLineage
}

func (l *MemLineage) Record(_ context.Context, b BatchLineage) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m == nil {
		l.m = map[string]BatchLineage{}
	}
	l.m[b.TenantID+"/"+b.BatchID] = b
	return nil
}

// All returns the recorded batches in no particular order.
func (l *MemLineage) All() []BatchLineage {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]BatchLineage, 0, len(l.m))
	for _, b := range l.m {
		out = append(out, b)
	}
	return out
}
