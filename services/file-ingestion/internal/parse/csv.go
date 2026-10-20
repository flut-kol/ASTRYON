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
	Number int
	Raw    []byte
	Fields []string
	Err    error
}

// CSV reads RFC 4180 records incrementally. It never retains more than one
// record (up to maxRecordBytes) and the reader's small buffer. Oversized and
// malformed records are emitted as quarantine candidates; following records
// remain readable. A newline within a quoted field belongs to that record.
func CSV(r io.Reader, maxRecordBytes int, emit func(CSVRecord) error) error {
	if maxRecordBytes < 1 {
		return ErrRecordTooLarge
	}
	reader := bufio.NewReaderSize(r, min(maxRecordBytes, 64<<10))
	for number := 1; ; number++ {
		raw, err := readCSVRecord(reader, maxRecordBytes)
		if errors.Is(err, io.EOF) && len(raw) == 0 {
			return nil
		}
		record := CSVRecord{Number: number, Raw: raw}
		if err != nil {
			record.Err = err
		} else {
			parsed, parseErr := csv.NewReader(bytes.NewReader(raw)).Read()
			if parseErr != nil {
				record.Err = parseErr
			} else {
				record.Fields = parsed
			}
		}
		if emitErr := emit(record); emitErr != nil {
			return emitErr
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

// readCSVRecord accepts doubled quotes per RFC 4180 and discards an oversized
// record before returning, so the next invocation starts at a record boundary.
func readCSVRecord(r *bufio.Reader, max int) ([]byte, error) {
	var record []byte
	inQuotes := false
	for {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && len(record) > 0 {
				return record, io.EOF
			}
			return record, err
		}
		if len(record) < max {
			record = append(record, b)
		}
		if b == '"' {
			if inQuotes {
				next, peekErr := r.Peek(1)
				if peekErr == nil && next[0] == '"' {
					b, err = r.ReadByte()
					if err != nil {
						return record, err
					}
					if len(record) < max {
						record = append(record, b)
					}
					continue
				}
			}
			inQuotes = !inQuotes
		}
		if b == '\n' && !inQuotes {
			record = bytes.TrimSuffix(record, []byte("\n"))
			record = bytes.TrimSuffix(record, []byte("\r"))
			if len(record) >= max {
				return record, ErrRecordTooLarge
			}
			return record, nil
		}
		if len(record) >= max {
			for {
				b, err = r.ReadByte()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return record, ErrRecordTooLarge
					}
					return record, err
				}
				if b == '"' {
					if inQuotes {
						next, peekErr := r.Peek(1)
						if peekErr == nil && next[0] == '"' {
							_, _ = r.ReadByte()
							continue
						}
					}
					inQuotes = !inQuotes
				}
				if b == '\n' && !inQuotes {
					return record, ErrRecordTooLarge
				}
			}
		}
	}
}
