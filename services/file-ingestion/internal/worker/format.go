package worker

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/parse"
)

// maxCSVColumns bounds the header (and so the per-row map built for the sink and the checkpoint size).
const maxCSVColumns = 1024

type format int

const (
	formatNDJSON format = iota
	formatCSV
)

// formatOf chooses the parser from the upload record's content type (already allow-listed at upload creation).
// Anything but text/csv is NDJSON, the original behaviour.
func formatOf(j jobs.Job) format {
	if mt, _, _ := strings.Cut(j.ContentType, ";"); strings.EqualFold(strings.TrimSpace(mt), "text/csv") {
		return formatCSV
	}
	return formatNDJSON
}

// row is one parsed record. For CSV, Rec.Raw is the data row as a JSON object keyed by the header (so profiles, the
// sink and dedup treat both formats alike) and Fields is the original cell list for classification. The header
// record itself is emitted with Header set: it advances the checkpoint but is neither valid nor invalid data.
type row struct {
	Rec    parse.Record
	Fields []string
	Header bool
}

// dataNumber is the 1-based number of a data record (the CSV header line does not count towards row limits).
func (f format) dataNumber(r parse.Record) int64 {
	if f == formatCSV {
		return int64(r.Number) - 1
	}
	return int64(r.Number)
}

// stream parses rc, which starts at a committed record boundary (off, num). header carries the CSV column names:
// set from the checkpoint on resume, filled in when record 1 is read. Memory is bounded by one record plus the header.
type stream struct {
	f      format
	header []string // original column names (as labelled)
	keys   []string // unique JSON keys, one per column
}

func (s *stream) setHeader(h []string) {
	s.header = h
	s.keys = headerKeys(h)
}

func (s *stream) run(rc io.Reader, maxRecord int, off int64, num int, emit func(row) error) error {
	if s.f != formatCSV {
		return parse.NDJSONFrom(rc, maxRecord, off, num, func(r parse.Record) error { return emit(row{Rec: r}) })
	}
	return parse.CSVFrom(rc, maxRecord, off, num, func(c parse.CSVRecord) error {
		rec := parse.Record{Number: c.Number, Offset: c.Offset, NextOffset: c.NextOffset, Err: c.Err}
		if c.Number == 1 {
			if c.Err != nil || len(c.Fields) == 0 || len(c.Fields) > maxCSVColumns {
				return Permanent("csv_header_invalid")
			}
			c.Fields[0] = strings.TrimPrefix(c.Fields[0], "\ufeff")
			s.setHeader(append([]string(nil), c.Fields...))
			return emit(row{Rec: rec, Header: true})
		}
		if s.keys == nil {
			return Permanent("bad_checkpoint") // resumed past the header without one in the checkpoint
		}
		if rec.Err == nil {
			if len(c.Fields) != len(s.keys) {
				rec.Err = parse.ErrCSVMalformed
			} else {
				obj := make(map[string]string, len(s.keys))
				for i, v := range c.Fields {
					obj[s.keys[i]] = v
				}
				rec.Raw, _ = json.Marshal(obj)
			}
		}
		return emit(row{Rec: rec, Fields: c.Fields})
	})
}

// scan classifies one valid row; CSV cells are named by the header (labels carry column names, never values).
func (s *stream) scan(sc *classify.Scanner, r row) {
	if s.f == formatCSV {
		sc.ScanCSV(s.header, r.Fields)
		return
	}
	sc.ScanJSON(r.Rec.Raw)
}

// headerKeys turns header cells into unique, non-empty JSON keys: blank names become col<N>, repeats get _<N>.
func headerKeys(h []string) []string {
	keys := make([]string, len(h))
	seen := make(map[string]bool, len(h))
	for i, n := range h {
		if n == "" {
			n = "col" + strconv.Itoa(i+1)
		}
		for k := 2; seen[n]; k++ {
			n = h[i] + "_" + strconv.Itoa(k)
		}
		seen[n] = true
		keys[i] = n
	}
	return keys
}
