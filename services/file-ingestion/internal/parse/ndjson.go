// Package parse provides bounded-memory file readers. Invalid records are
// reported independently so one bad record cannot discard an entire upload.
package parse

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

var (
	ErrRecordTooLarge  = errors.New("record exceeds configured maximum")
	ErrInvalidPosition = errors.New("invalid parser position")
	ErrInvalidEncoding = errors.New("record is not valid UTF-8")
)

type Record struct {
	Number     int
	Offset     int64
	NextOffset int64
	Raw        []byte
	Err        error
}

// NDJSON reads one JSON value per line. It keeps at most maxRecordBytes plus
// reader bookkeeping in memory and continues after malformed or oversized
// JSON records.
func NDJSON(r io.Reader, maxRecordBytes int, emit func(Record) error) error {
	return NDJSONFrom(r, maxRecordBytes, 0, 1, emit)
}

// NDJSONFrom parses a reader that starts at a previously committed record
// boundary. startOffset is the byte offset in the immutable source object and
// startNumber is the one-based number of the first record in r. A caller can
// persist Record.NextOffset and Number+1 only after its sink/quarantine write
// succeeds, then issue an object-store range read at that offset after restart.
func NDJSONFrom(r io.Reader, maxRecordBytes int, startOffset int64, startNumber int, emit func(Record) error) error {
	if maxRecordBytes < 1 {
		return ErrRecordTooLarge
	}
	if startOffset < 0 || startNumber < 1 {
		return ErrInvalidPosition
	}

	reader := bufio.NewReaderSize(r, min(maxRecordBytes+1, 64<<10))
	offset := startOffset
	for number := startNumber; ; number++ {
		raw, consumed, oversized, err := readNDJSONRecord(reader, maxRecordBytes)
		if errors.Is(err, io.EOF) && consumed == 0 {
			return nil
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		record := Record{
			Number:     number,
			Offset:     offset,
			NextOffset: offset + consumed,
			Raw:        raw,
		}
		if oversized {
			record.Err = ErrRecordTooLarge
		} else if !utf8.Valid(raw) {
			record.Err = ErrInvalidEncoding
		} else {
			var value any
			if jsonErr := json.Unmarshal(raw, &value); jsonErr != nil {
				record.Err = jsonErr
			}
		}
		if emitErr := emit(record); emitErr != nil {
			return emitErr
		}
		offset += consumed
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

// DedupKey is stable across retries and process restarts. sourceFingerprint
// must identify immutable source contents (for example, the verified SHA-256),
// not merely an object name that can be overwritten.
func (r Record) DedupKey(sourceFingerprint string) (string, error) {
	if sourceFingerprint == "" || r.Offset < 0 {
		return "", ErrInvalidPosition
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", sourceFingerprint, r.Offset)))
	return fmt.Sprintf("%x", sum[:]), nil
}

// readNDJSONRecord retains only max+1 bytes while consuming the complete line.
// This lets the caller quarantine an oversized line and continue at the next
// record without allocating memory proportional to attacker-controlled input.
func readNDJSONRecord(r *bufio.Reader, max int) ([]byte, int64, bool, error) {
	stored := make([]byte, 0, min(max+1, 64<<10))
	var consumed int64
	var contentBytes int64
	var lastContentByte byte

	for {
		fragment, err := r.ReadSlice('\n')
		if len(fragment) > 0 {
			consumed += int64(len(fragment))
			content := fragment
			if content[len(content)-1] == '\n' {
				content = content[:len(content)-1]
			}
			if len(content) > 0 {
				contentBytes += int64(len(content))
				lastContentByte = content[len(content)-1]
				if remaining := max + 1 - len(stored); remaining > 0 {
					if len(content) > remaining {
						content = content[:remaining]
					}
					stored = append(stored, content...)
				}
			}
		}

		hasNewline := len(fragment) > 0 && fragment[len(fragment)-1] == '\n'
		if hasNewline || errors.Is(err, io.EOF) {
			if consumed == 0 {
				return nil, 0, false, io.EOF
			}
			logicalBytes := contentBytes
			if logicalBytes > 0 && lastContentByte == '\r' {
				logicalBytes--
				if int64(len(stored)) == contentBytes {
					stored = bytes.TrimSuffix(stored, []byte{'\r'})
				}
			}
			tookTooMuch := logicalBytes > int64(max)
			if tookTooMuch && len(stored) > max {
				stored = stored[:max]
			}
			if errors.Is(err, io.EOF) {
				return stored, consumed, tookTooMuch, io.EOF
			}
			return stored, consumed, tookTooMuch, nil
		}
		if err != nil && !errors.Is(err, bufio.ErrBufferFull) {
			return nil, consumed, false, err
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
