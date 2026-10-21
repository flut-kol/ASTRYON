// Package parquetout writes accepted batches as normalized Parquet part files (dedup_key, row_number, payload JSON),
// one object per worker batch, so memory stays at one batch. Keys are deterministic per (file, pipeline, first row):
// a replayed batch overwrites its own part, making the sink idempotent without a manifest.
//
// Library choice: github.com/parquet-go/parquet-go (Apache-2.0) is pure Go (no cgo, no Arrow/C++), supports
// ReaderAt-based footer-first reads and row-group iteration, and its transitive deps are small (bitpack, jsonlite,
// klauspost/compress already used here). apache/arrow-go is far heavier; xitongsys/parquet-go is unmaintained.
package parquetout

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/worker"
)

// Putter stores one object (s3.Client satisfies it via PutterFunc / adapter in cmd).
type Putter interface {
	Put(ctx context.Context, key string, body []byte, contentType string) error
}

// ContentType of the written parts.
const ContentType = "application/vnd.apache.parquet"

// Row is the normalized output schema.
type Row struct {
	DedupKey  string `parquet:"dedup_key"`
	RowNumber int64  `parquet:"row_number"`
	Payload   string `parquet:"payload"` // the record as a JSON object
}

// Sink writes valid records of each batch as a Parquet part, then forwards the batch to Inner (if set).
// Invalid records are only forwarded: quarantine stays Inner's job.
type Sink struct {
	Put      Putter
	Prefix   string // e.g. "normalized"; tenant and file ids are appended
	Pipeline string // pipeline version, part of the key
	Inner    worker.Sink
}

var _ worker.Sink = (*Sink)(nil)
var _ worker.Outputter = (*Sink)(nil)

// OutputKey is <prefix>/tenants/<tenant>/<file object>/<pipeline>/part-<first valid row>.parquet ("" when the batch has no valid row).
func (s *Sink) OutputKey(j jobs.Job, valid []worker.Item) string {
	if len(valid) == 0 {
		return ""
	}
	firstRow := int64(valid[0].Record.Number)
	return path.Join(strings.Trim(s.Prefix, "/"), "tenants", j.TenantID, j.FileObjectID, s.Pipeline, fmt.Sprintf("part-%012d.parquet", firstRow))
}

func (s *Sink) Accept(ctx context.Context, j jobs.Job, valid, invalid []worker.Item) error {
	if s.Put == nil || s.Pipeline == "" {
		return errors.New("parquetout: Put and Pipeline are required")
	}
	if len(valid) > 0 {
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[Row](&buf, parquet.Compression(&zstd.Codec{}),
			parquet.KeyValueMetadata("astryon.pipeline_version", s.Pipeline),
			parquet.KeyValueMetadata("astryon.source_file_id", j.FileObjectID),
			parquet.KeyValueMetadata("astryon.job_id", j.ID))
		rows := make([]Row, len(valid))
		for i, v := range valid {
			rows[i] = Row{DedupKey: v.DedupKey, RowNumber: int64(v.Record.Number), Payload: string(v.Record.Raw)}
		}
		if _, err := w.Write(rows); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		if err := s.Put.Put(ctx, s.OutputKey(j, valid), buf.Bytes(), ContentType); err != nil {
			return err
		}
	}
	if s.Inner != nil {
		return s.Inner.Accept(ctx, j, valid, invalid)
	}
	return nil
}
