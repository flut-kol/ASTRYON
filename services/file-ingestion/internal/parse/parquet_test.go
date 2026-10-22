package parse

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type testRow struct {
	ID   int64  `parquet:"id"`
	Name string `parquet:"name,optional"`
}

func buildParquet(t *testing.T, n, perGroup int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[testRow](&buf, parquet.MaxRowsPerRowGroup(int64(perGroup)))
	rows := make([]testRow, n)
	for i := range rows {
		rows[i] = testRow{ID: int64(i + 1), Name: "n" + string(rune('a'+i%26))}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func collect(t *testing.T, data []byte, lim ParquetLimits, g int, row int64) ([]ParquetRecord, error) {
	t.Helper()
	var out []ParquetRecord
	err := ParquetFrom(bytes.NewReader(data), int64(len(data)), lim, g, row, func(r ParquetRecord) error {
		out = append(out, r)
		return nil
	})
	return out, err
}

func TestParquetStreamsRowsWithGroupBoundaries(t *testing.T) {
	data := buildParquet(t, 7, 3) // groups: 3,3,1
	recs, err := collect(t, data, ParquetLimits{MaxRecordBytes: 1 << 10}, 0, 0)
	if err != nil || len(recs) != 7 {
		t.Fatalf("%d %v", len(recs), err)
	}
	var ends []int
	for i, r := range recs {
		if r.Number != i+1 || r.Offset != int64(i) || r.NextOffset != int64(i)+1 {
			t.Fatalf("%d %+v", i, r)
		}
		if r.GroupEnd {
			ends = append(ends, i)
		}
	}
	if len(ends) != 3 || ends[0] != 2 || ends[1] != 5 || ends[2] != 6 {
		t.Fatalf("%v", ends)
	}
	var m map[string]any
	if err := json.Unmarshal(recs[0].Raw, &m); err != nil || m["id"] != float64(1) || m["name"] != "na" {
		t.Fatalf("%v %v", m, err)
	}
	g, n, err := ParquetGroups(bytes.NewReader(data), int64(len(data)), ParquetLimits{})
	if err != nil || g != 3 || n != 7 {
		t.Fatal(g, n, err)
	}
}

func TestParquetResumesAtGroupAndCheckpointAdvances(t *testing.T) {
	data := buildParquet(t, 7, 3)
	recs, err := collect(t, data, ParquetLimits{MaxRecordBytes: 1 << 10}, 1, 3)
	if err != nil || len(recs) != 4 || recs[0].Number != 4 || recs[0].Group != 1 {
		t.Fatalf("%d %v", len(recs), err)
	}
	cp, _ := NewCheckpoint("fp")
	cp.NextOffset, cp.NextRecordNumber, cp.NextGroup = 3, 4, 1
	for _, r := range recs {
		var aerr error
		if cp, aerr = cp.Advance(r.Record); aerr != nil {
			t.Fatal(aerr)
		}
	}
	if cp.NextOffset != 7 || cp.NextRecordNumber != 8 {
		t.Fatalf("%+v", cp)
	}
	// a checkpoint that does not match the file's group sizes is rejected (file changed)
	if _, err := collect(t, data, ParquetLimits{MaxRecordBytes: 1 << 10}, 1, 2); !errors.Is(err, ErrCheckpoint) {
		t.Fatalf("%v", err)
	}
	if _, err := collect(t, data, ParquetLimits{MaxRecordBytes: 1 << 10}, 9, 0); !errors.Is(err, ErrCheckpoint) {
		t.Fatalf("%v", err)
	}
}

func TestParquetRejectsGarbageAndOversizedGroupsAndRows(t *testing.T) {
	lim := ParquetLimits{MaxRecordBytes: 1 << 10}
	if _, err := collect(t, []byte("this is definitely not parquet data"), lim, 0, 0); !errors.Is(err, ErrParquet) {
		t.Fatalf("%v", err)
	}
	data := buildParquet(t, 4, 2)
	lim.MaxRowGroupSize = 1
	if _, err := collect(t, data, lim, 0, 0); !errors.Is(err, ErrParquetRowGroupTooLarge) {
		t.Fatalf("%v", err)
	}
	recs, err := collect(t, data, ParquetLimits{MaxRecordBytes: 5}, 0, 0)
	if err != nil || len(recs) != 4 || !errors.Is(recs[0].Err, ErrRecordTooLarge) || recs[0].Raw != nil {
		t.Fatalf("%v %+v", err, recs)
	}
	if _, err := collect(t, data, ParquetLimits{}, 0, 0); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("%v", err)
	}
	// truncated file: footer missing
	if _, err := collect(t, data[:len(data)/2], ParquetLimits{MaxRecordBytes: 1 << 10}, 0, 0); !errors.Is(err, ErrParquet) {
		t.Fatalf("%v", err)
	}
}

func TestParquetEmitErrorStopsAndPropagates(t *testing.T) {
	data := buildParquet(t, 4, 2)
	boom := errors.New("boom")
	n := 0
	err := ParquetFrom(bytes.NewReader(data), int64(len(data)), ParquetLimits{MaxRecordBytes: 1 << 10}, 0, 0, func(ParquetRecord) error {
		n++
		return boom
	})
	if !errors.Is(err, boom) || n != 1 {
		t.Fatal(err, n)
	}
}
