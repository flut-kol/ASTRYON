// Package editsave orchestrates the spreadsheet-prep flow for an already-uploaded, committed CSV file: preview
// an ops pipeline against a bounded sample, or apply it to the whole file and save the result as a new, versioned
// S3 object. It never enqueues a jobs row and never touches the ClickHouse sink path.
package editsave

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/parse"
	"github.com/astryon/file-ingestion/internal/profile"
	"github.com/astryon/file-ingestion/internal/transform"
)

var (
	ErrNotCommitted           = errors.New("editsave: file is not committed")
	ErrUnsupportedContentType = errors.New("editsave: only text/csv files can be edited")
	ErrEditTooLarge           = errors.New("editsave: edited output exceeds the configured size limit")
	ErrVersionConflict        = errors.New("editsave: version number was claimed by a concurrent save; retry")
)

// FormatOptions controls how the source is read and the result is written.
type FormatOptions struct {
	InputHeaderRow  bool
	OutputHeaderRow bool
	OutputDelimiter byte // defaults to ',' when zero
}

func (f FormatOptions) delimiter() rune {
	if f.OutputDelimiter == 0 {
		return ','
	}
	return rune(f.OutputDelimiter)
}

type Warning struct {
	RowNumber int    `json:"row_number"`
	Message   string `json:"message"`
}

type PreviewResult struct {
	Header    []string
	Rows      [][]string
	Truncated bool
	Warnings  []Warning
}

// ObjectWriter is the S3 subset needed to save an edited file and hand back a download link (satisfied by *s3.Client).
type ObjectWriter interface {
	Put(ctx context.Context, key string, body []byte, contentType string) error
	PresignGet(key string, ttl time.Duration) (string, error)
}

// Service wires the pieces together. MaxBytes bounds the buffered edited output (a deliberate MVP simplification:
// large-file streaming saves are not supported yet). MaxRecordBytes bounds one parsed CSV record; zero uses 1 MiB,
// the same default as internal/worker.
type Service struct {
	Files          objects.Store
	Objs           objects.Objects
	Writer         ObjectWriter
	Versions       Store
	MaxBytes       int64
	MaxRecordBytes int
	Now            func() time.Time
}

func (s Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s Service) maxRecordBytes() int {
	if s.MaxRecordBytes > 0 {
		return s.MaxRecordBytes
	}
	return 1 << 20
}

var errStopEarly = errors.New("editsave: preview limit reached")

// DownloadTTL is how long a presigned download link from PresignDownload stays valid.
const DownloadTTL = 15 * time.Minute

// PresignDownload returns a short-lived download URL for an edit version's object key.
func (s Service) PresignDownload(key string) (url string, expiresAt time.Time, err error) {
	url, err = s.Writer.PresignGet(key, DownloadTTL)
	if err != nil {
		return "", time.Time{}, err
	}
	return url, s.now().Add(DownloadTTL), nil
}

// open validates the source file object and returns it ready to stream.
func (s Service) open(ctx context.Context, tenant, fileObjectID string) (objects.File, io.ReadCloser, error) {
	f, err := s.Files.Get(ctx, tenant, fileObjectID)
	if err != nil {
		return objects.File{}, nil, err
	}
	if !f.Committed {
		return objects.File{}, nil, ErrNotCommitted
	}
	if f.ContentType != "text/csv" {
		return objects.File{}, nil, ErrUnsupportedContentType
	}
	rc, err := s.Objs.Get(ctx, f.Key, 0)
	if err != nil {
		return objects.File{}, nil, err
	}
	return f, rc, nil
}

// Preview applies ops to up to limit kept rows (1..500, default 50) without writing anything.
func (s Service) Preview(ctx context.Context, tenant, fileObjectID string, ops []transform.OpSpec, fmtOpts FormatOptions, limit int) (PreviewResult, error) {
	switch {
	case limit <= 0:
		limit = 50
	case limit > 500:
		limit = 500
	}
	f, rc, err := s.open(ctx, tenant, fileObjectID)
	if err != nil {
		return PreviewResult{}, err
	}
	defer rc.Close()

	var (
		res      PreviewResult
		pipeline *transform.Pipeline
	)
	maxRecord := profile.Select(f.SourceType).MaxRecord(s.maxRecordBytes())
	err = parse.CSVFrom(rc, maxRecord, 0, 1, func(rec parse.CSVRecord) error {
		row, isData, perr := s.headerOrRow(rec, fmtOpts, &res.Header, &pipeline, ops, &res.Warnings)
		if perr != nil {
			return perr
		}
		if !isData {
			return nil
		}
		out, keep, warn := pipeline.Apply(row)
		if warn != nil {
			res.Warnings = append(res.Warnings, Warning{RowNumber: rec.Number, Message: warn.Error()})
		}
		if !keep {
			return nil
		}
		res.Rows = append(res.Rows, out)
		if len(res.Rows) >= limit {
			res.Truncated = true
			return errStopEarly
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopEarly) {
		return PreviewResult{}, err
	}
	if pipeline != nil {
		res.Header = pipeline.Header()
	} else if res.Header, err = s.emptyHeader(ops); err != nil {
		return PreviewResult{}, err
	}
	return res, nil
}

// Save applies ops to the whole file and writes the result as a new file_edit_versions row.
func (s Service) Save(ctx context.Context, tenant, fileObjectID string, ops []transform.OpSpec, fmtOpts FormatOptions, filename, requestedBy string) (Version, error) {
	f, rc, err := s.open(ctx, tenant, fileObjectID)
	if err != nil {
		return Version{}, err
	}
	defer rc.Close()

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Comma = fmtOpts.delimiter()

	var (
		header    []string
		pipeline  *transform.Pipeline
		warnings  []Warning
		rowCount  int64
		wroteHead bool
	)
	maxRecord := profile.Select(f.SourceType).MaxRecord(s.maxRecordBytes())
	err = parse.CSVFrom(rc, maxRecord, 0, 1, func(rec parse.CSVRecord) error {
		row, isData, perr := s.headerOrRow(rec, fmtOpts, &header, &pipeline, ops, &warnings)
		if perr != nil {
			return perr
		}
		if !wroteHead {
			wroteHead = true
			if fmtOpts.OutputHeaderRow {
				if err := w.Write(pipeline.Header()); err != nil {
					return err
				}
			}
		}
		if !isData {
			return nil
		}
		out, keep, warn := pipeline.Apply(row)
		if warn != nil {
			warnings = append(warnings, Warning{RowNumber: rec.Number, Message: warn.Error()})
		}
		if !keep {
			return nil
		}
		if err := w.Write(out); err != nil {
			return err
		}
		rowCount++
		if s.MaxBytes > 0 && int64(buf.Len()) > s.MaxBytes {
			return ErrEditTooLarge
		}
		return nil
	})
	if err != nil {
		return Version{}, err
	}
	if pipeline == nil {
		// empty source file: still produce a validated, possibly-empty header.
		h, herr := s.emptyHeader(ops)
		if herr != nil {
			return Version{}, herr
		}
		header = h
		if fmtOpts.OutputHeaderRow {
			if err := w.Write(header); err != nil {
				return Version{}, err
			}
		}
	} else {
		header = pipeline.Header()
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return Version{}, err
	}
	if s.MaxBytes > 0 && int64(buf.Len()) > s.MaxBytes {
		return Version{}, ErrEditTooLarge
	}

	body := buf.Bytes()
	sum := sha256.Sum256(body)
	checksum := hex.EncodeToString(sum[:])
	name := safeFilename(filename, f.Key)

	// The object key names the version number, which Postgres only allocates on a successful insert, so the
	// number is proposed, written to S3 under that key, then inserted; a losing race on the uniqueness
	// constraint (another Save for the same file landed first) re-proposes the next number and retries. The
	// S3 object from a losing attempt is simply an orphan at an unreferenced key, acceptable for this
	// low-concurrency admin path.
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		n, err := s.Versions.NextVersionNumber(ctx, tenant, fileObjectID)
		if err != nil {
			return Version{}, err
		}
		key := editKey(tenant, fileObjectID, n, name)
		if err := s.Writer.Put(ctx, key, body, "text/csv"); err != nil {
			return Version{}, err
		}
		v, err := s.Versions.Insert(ctx, tenant, fileObjectID, NewVersion{
			VersionNumber: n, Ops: ops, InputHeaderRow: fmtOpts.InputHeaderRow, OutputHeaderRow: fmtOpts.OutputHeaderRow,
			OutputDelimiter: string(fmtOpts.delimiter()), Header: header, Bucket: f.Bucket, ObjectKey: key,
			ChecksumSHA256: checksum, SizeBytes: int64(len(body)), RowCount: rowCount, RequestedBy: requestedBy,
		})
		if errors.Is(err, ErrVersionConflict) {
			lastErr = err
			continue
		}
		if err != nil {
			return Version{}, err
		}
		return v, nil
	}
	return Version{}, lastErr
}

// headerOrRow establishes the header from the first record (either consuming it as a header row, or
// synthesizing col_1..col_N and treating it as data), compiling the pipeline exactly once, then reports
// whether rec is itself a data row to apply.
func (s Service) headerOrRow(rec parse.CSVRecord, fmtOpts FormatOptions, header *[]string, pipeline **transform.Pipeline, ops []transform.OpSpec, warnings *[]Warning) ([]string, bool, error) {
	if rec.Err != nil {
		*warnings = append(*warnings, Warning{RowNumber: rec.Number, Message: "malformed CSV record, skipped"})
		return nil, false, nil
	}
	if *header == nil {
		if fmtOpts.InputHeaderRow {
			*header = dedupeHeader(rec.Fields)
			p, err := transform.Compile(*header, ops)
			if err != nil {
				return nil, false, err
			}
			*pipeline = p
			return nil, false, nil
		}
		*header = syntheticHeader(len(rec.Fields))
		p, err := transform.Compile(*header, ops)
		if err != nil {
			return nil, false, err
		}
		*pipeline = p
	}
	return rec.Fields, true, nil
}

func (s Service) emptyHeader(ops []transform.OpSpec) ([]string, error) {
	p, err := transform.Compile(nil, ops)
	if err != nil {
		return nil, err
	}
	return p.Header(), nil
}

func syntheticHeader(n int) []string {
	h := make([]string, n)
	for i := range h {
		h[i] = "col" + strconv.Itoa(i+1)
	}
	return h
}

// dedupeHeader strips a UTF-8 BOM from the first cell and turns header cells into unique, non-empty names:
// blank names become col<N>, repeats get _<N> (mirrors internal/worker/format.go's headerKeys, kept separate
// since that function is private to the ingestion worker).
func dedupeHeader(h []string) []string {
	out := make([]string, len(h))
	copy(out, h)
	if len(out) > 0 {
		out[0] = strings.TrimPrefix(out[0], "\uFEFF")
	}
	seen := make(map[string]bool, len(out))
	for i, n := range out {
		if n == "" {
			n = "col" + strconv.Itoa(i+1)
		}
		base := n
		for k := 2; seen[n]; k++ {
			n = base + "_" + strconv.Itoa(k)
		}
		seen[n] = true
		out[i] = n
	}
	return out
}

func safeFilename(requested, originalKey string) string {
	if requested != "" && path.Base(requested) == requested && !strings.Contains(requested, "..") {
		return requested
	}
	return path.Base(originalKey)
}

func editKey(tenant, fileObjectID string, versionNumber int, filename string) string {
	return fmt.Sprintf("%s/edits/%s/v%d/%s", tenant, fileObjectID, versionNumber, filename)
}
