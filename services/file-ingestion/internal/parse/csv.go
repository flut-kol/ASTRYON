package parse

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"io"
)

// CSVRecord is one RFC 4180 CSV record. Raw excludes its terminating newline.
// Fields is set only when Err is nil.
type CSVRecord struct {
	Number     int
	Offset     int64 // byte offset of the record start in the source object
	NextOffset int64 // offset of the next record (after the terminating newline)
	Raw        []byte
	Fields     []string
	Err        error
}

// CSV reads RFC 4180 records incrementally. It never retains more than one
// record (up to maxRecordBytes) and the reader's small buffer. Oversized and
// malformed records are emitted as quarantine candidates; following records
// remain readable. A newline within a quoted field belongs to that record.
func CSV(r io.Reader, maxRecordBytes int, emit func(CSVRecord) error) error {
	return CSVFrom(r, maxRecordBytes, 0, 1, emit)
}

// ErrCSVMalformed is the class of a CSV record that is not valid RFC 4180 (or whose shape does not match the header).
var ErrCSVMalformed = errors.New("csv record is malformed")

// CSVFrom parses a reader that starts at a previously committed record boundary (see NDJSONFrom). Offsets are
// absolute positions in the source object, so Record boundaries are valid range-read restart points. Quoted newlines
// belong to their record, hence a checkpoint never falls inside a quoted field.
func CSVFrom(r io.Reader, maxRecordBytes int, startOffset int64, startNumber int, emit func(CSVRecord) error) error {
	if maxRecordBytes < 1 {
		return ErrRecordTooLarge
	}
	if startOffset < 0 || startNumber < 1 {
		return ErrInvalidPosition
	}
	reader := &countReader{Reader: bufio.NewReaderSize(r, min(maxRecordBytes, 64<<10))}
	offset := startOffset
	for number := startNumber; ; number++ {
		before := reader.n
		raw, err := readCSVRecord(reader, maxRecordBytes)
		if errors.Is(err, io.EOF) && len(raw) == 0 && reader.n == before {
			return nil
		}
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, ErrRecordTooLarge) {
			return err
		}
		consumed := reader.n - before
		record := CSVRecord{Number: number, Offset: offset, NextOffset: offset + consumed, Raw: raw}
		offset += consumed
		if errors.Is(err, ErrRecordTooLarge) {
			record.Err = err
		} else if parsed, parseErr := csv.NewReader(bytes.NewReader(raw)).Read(); parseErr != nil {
			record.Err = ErrCSVMalformed // never wrap parseErr: callers must not see content-derived text
		} else {
			record.Fields = parsed
		}
		if emitErr := emit(record); emitErr != nil {
			return emitErr
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

// readCSVRecord finds the record boundary per RFC 4180: a quote opens a quoted field only at the start of a field,
// doubled quotes inside a quoted field are literal, and a newline inside quotes belongs to the record. A bare quote in
// an unquoted field does not swallow following lines (the csv reader then flags that record as malformed). At most
// max bytes are retained; the rest of an oversized record is consumed and dropped so the next call starts at a record
// boundary.
func readCSVRecord(r *countReader, max int) ([]byte, error) {
	var record []byte
	over := false
	add := func(b byte) {
		if len(record) < max {
			record = append(record, b)
		} else {
			over = true
		}
	}
	inQuotes, fieldStart := false, true
	for {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && over {
				return record, ErrRecordTooLarge
			}
			return record, err
		}
		add(b)
		switch {
		case b == '"' && inQuotes:
			if next, perr := r.Peek(1); perr == nil && next[0] == '"' {
				b2, _ := r.ReadByte()
				add(b2)
			} else {
				inQuotes = false
			}
			fieldStart = false
		case b == '"' && fieldStart:
			inQuotes, fieldStart = true, false
		case b == '\n' && !inQuotes:
			if over {
				return record, ErrRecordTooLarge
			}
			record = bytes.TrimSuffix(record, []byte("\n"))
			record = bytes.TrimSuffix(record, []byte("\r"))
			return record, nil
		case b == ',' && !inQuotes:
			fieldStart = true
		default:
			fieldStart = false
		}
	}
}

type countReader struct {
	*bufio.Reader
	n int64
}

func (c *countReader) ReadByte() (byte, error) {
	b, err := c.Reader.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}
