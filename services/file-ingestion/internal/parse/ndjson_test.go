package parse

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestNDJSONQuarantinesBadRecordAndContinues(t *testing.T) {
	var got []Record
	err := NDJSON(strings.NewReader("{\"ok\":1}\nnope\n{\"ok\":2}\n"), 64, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil || len(got) != 3 || got[1].Err == nil || got[2].Number != 3 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestNDJSONRejectsInvalidUTF8(t *testing.T) {
	data := append([]byte("{\"ok\":1}\n"), []byte{'{', '"', 'x', '"', ':', 0xff, '}'}...)
	var got []Record
	if err := NDJSON(bytes.NewReader(data), 64, func(r Record) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !errors.Is(got[1].Err, ErrInvalidEncoding) {
		t.Fatalf("records=%+v", got)
	}
}

func TestNDJSONBoundsRecordMemory(t *testing.T) {
	input := "{\"a\":1}\n" + strings.Repeat("x", 128) + "\n{\"b\":2}\n"
	var got []Record
	err := NDJSON(strings.NewReader(input), 16, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || !errors.Is(got[1].Err, ErrRecordTooLarge) || got[2].Err != nil {
		t.Fatalf("records=%+v", got)
	}
	if len(got[1].Raw) > 16 {
		t.Fatalf("oversized record retained %d bytes", len(got[1].Raw))
	}
	if got[2].Offset != int64(len("{\"a\":1}\n")+128+1) || got[2].NextOffset != int64(len(input)) {
		t.Fatalf("wrong offsets: %+v", got[2])
	}
}

func TestNDJSONResumeFromCommittedCheckpoint(t *testing.T) {
	data := []byte("{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n")
	checkpoint, err := NewCheckpoint("sha256:immutable")
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("worker stopped")
	err = NDJSON(bytes.NewReader(data), 64, func(record Record) error {
		checkpoint, err = checkpoint.Advance(record)
		if err != nil {
			return err
		}
		if record.Number == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("err=%v", err)
	}

	var resumed []Record
	err = NDJSONFrom(bytes.NewReader(data[checkpoint.NextOffset:]), 64, checkpoint.NextOffset, checkpoint.NextRecordNumber, func(record Record) error {
		resumed = append(resumed, record)
		checkpoint, err = checkpoint.Advance(record)
		return err
	})
	if err != nil || len(resumed) != 1 || resumed[0].Number != 3 || checkpoint.NextOffset != int64(len(data)) {
		t.Fatalf("resumed=%+v checkpoint=%+v err=%v", resumed, checkpoint, err)
	}
}

func TestCheckpointRejectsChangedSourceAndSkippedRecord(t *testing.T) {
	checkpoint, err := NewCheckpoint("sha-a")
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(checkpoint.ValidateSource("sha-b"), ErrCheckpoint) {
		t.Fatal("changed source accepted")
	}
	_, err = checkpoint.Advance(Record{Number: 2, Offset: 10, NextOffset: 20})
	if !errors.Is(err, ErrCheckpoint) {
		t.Fatalf("skipped record err=%v", err)
	}
}

func TestDedupKeyIsStableAcrossReplay(t *testing.T) {
	record := Record{Number: 9, Offset: 42, NextOffset: 50}
	first, err := record.DedupKey("sha256:immutable")
	if err != nil {
		t.Fatal(err)
	}
	replayed := Record{Number: 9, Offset: 42, NextOffset: 50}
	second, err := replayed.DedupKey("sha256:immutable")
	if err != nil || first != second {
		t.Fatalf("first=%q second=%q err=%v", first, second, err)
	}
}
