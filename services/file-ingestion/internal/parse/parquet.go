package parse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

var (
	// ErrParquet marks a file that is not readable Parquet (bad footer, unsupported layout). It is permanent.
	ErrParquet = errors.New("invalid parquet file")
	// ErrParquetRowGroupTooLarge means a row group's uncompressed size exceeds the configured bound.
	ErrParquetRowGroupTooLarge = errors.New("parquet row group exceeds configured maximum")
)

// ParquetLimits bound what one file can make the reader hold in memory.
type ParquetLimits struct {
	MaxRecordBytes  int   // JSON size of one converted row; larger rows are emitted as ErrRecordTooLarge
	MaxColumns      int   // leaf columns, default 1024
	MaxRowGroupSize int64 // uncompressed bytes per row group (from footer metadata), default 256 MiB
	ReadBufferSize  int   // ReaderAt read granularity, default 1 MiB
}

// ParquetRecord is one row. Record.Offset is the 0-based row ordinal in the file and Record.Number = Offset+1, so
// Checkpoint.Advance and Record.DedupKey work unchanged (the ordinal is stable for an immutable object).
// Raw is the row as a JSON object keyed by column name; Err is set (Raw nil) for rows that cannot be converted.
// GroupEnd is true on the last row of a row group: the only safe checkpoint boundary.
type ParquetRecord struct {
	Record
	Group    int
	GroupEnd bool
}

// ParquetGroups describes the row groups of a file without reading row data (footer only).
func ParquetGroups(ra io.ReaderAt, size int64, lim ParquetLimits) (groups int, rows int64, err error) {
	f, err := openParquet(ra, size, lim)
	if err != nil {
		return 0, 0, err
	}
	return len(f.RowGroups()), f.NumRows(), nil
}

func openParquet(ra io.ReaderAt, size int64, lim ParquetLimits) (*parquet.File, error) {
	if lim.MaxColumns <= 0 {
		lim.MaxColumns = 1024
	}
	if lim.MaxRowGroupSize <= 0 {
		lim.MaxRowGroupSize = 256 << 20
	}
	if lim.ReadBufferSize <= 0 {
		lim.ReadBufferSize = 1 << 20
	}
	f, err := openGuarded(ra, size, lim)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParquet, err)
	}
	if n := len(f.Schema().Columns()); n == 0 || n > lim.MaxColumns {
		return nil, fmt.Errorf("%w: %d columns", ErrParquet, n)
	}
	for _, g := range f.Metadata().RowGroups {
		if g.TotalByteSize > lim.MaxRowGroupSize {
			return nil, ErrParquetRowGroupTooLarge
		}
	}
	return f, nil
}

// openGuarded turns a decoder panic on hostile input into an error.
func openGuarded(ra io.ReaderAt, size int64, lim ParquetLimits) (f *parquet.File, err error) {
	defer func() {
		if r := recover(); r != nil {
			f, err = nil, fmt.Errorf("decoder panic: %v", r)
		}
	}()
	return parquet.OpenFile(ra, size, parquet.ReadBufferSize(lim.ReadBufferSize), parquet.SkipBloomFilters(true), parquet.SkipPageIndex(true))
}

// ParquetFrom streams rows from startGroup on. startRow must equal the number of rows in the groups before
// startGroup (the checkpoint's NextOffset); a mismatch means the file changed and yields ErrCheckpoint.
// Memory is bounded by the footer, one row group's decoded pages and one row.
func ParquetFrom(ra io.ReaderAt, size int64, lim ParquetLimits, startGroup int, startRow int64, emit func(ParquetRecord) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: decoder panic: %v", ErrParquet, r)
		}
	}()
	if lim.MaxRecordBytes < 1 {
		return ErrRecordTooLarge
	}
	if startGroup < 0 || startRow < 0 {
		return ErrInvalidPosition
	}
	f, err := openParquet(ra, size, lim)
	if err != nil {
		return err
	}
	groups := f.RowGroups()
	if startGroup > len(groups) {
		return ErrCheckpoint
	}
	var before int64
	for _, g := range groups[:startGroup] {
		before += g.NumRows()
	}
	if before != startRow {
		return ErrCheckpoint
	}
	schema := f.Schema()
	row := startRow
	for gi := startGroup; gi < len(groups); gi++ {
		n := groups[gi].NumRows()
		if n == 0 {
			continue
		}
		rows := groups[gi].Rows()
		buf := make([]parquet.Row, 1)
		for i := int64(0); i < n; i++ {
			c, rerr := rows.ReadRows(buf)
			if c == 0 {
				rows.Close()
				if rerr == nil || errors.Is(rerr, io.EOF) {
					rerr = io.ErrUnexpectedEOF
				}
				return fmt.Errorf("%w: row group %d: %v", ErrParquet, gi, rerr)
			}
			rec := ParquetRecord{Record: Record{Number: int(row) + 1, Offset: row, NextOffset: row + 1}, Group: gi, GroupEnd: i == n-1}
			m := map[string]any{}
			if cerr := schema.Reconstruct(&m, buf[0]); cerr != nil {
				rec.Err = cerr
			} else if raw, jerr := json.Marshal(m); jerr != nil {
				rec.Err = jerr
			} else if len(raw) > lim.MaxRecordBytes {
				rec.Err = ErrRecordTooLarge
			} else {
				rec.Raw = raw
			}
			if eerr := emit(rec); eerr != nil {
				rows.Close()
				return eerr
			}
			row++
		}
		rows.Close()
	}
	return nil
}
