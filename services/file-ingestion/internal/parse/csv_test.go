package parse

import (
	"errors"
	"strings"
	"testing"
)

func TestCSVParsesQuotedMultilineRecord(t *testing.T) {
	var got []CSVRecord
	err := CSV(strings.NewReader("name,note\r\nAda,\"first\nsecond\"\r\n"), 128, func(record CSVRecord) error {
		got = append(got, record)
		return nil
	})
	if err != nil || len(got) != 2 || got[1].Err != nil || got[1].Fields[1] != "first\nsecond" {
		t.Fatalf("records=%+v err=%v", got, err)
	}
}

func TestCSVQuarantinesMalformedAndContinues(t *testing.T) {
	var got []CSVRecord
	err := CSV(strings.NewReader("ok,1\n\"bad\"quote,2\nnext,3\n"), 128, func(record CSVRecord) error {
		got = append(got, record)
		return nil
	})
	if err != nil || len(got) != 3 || got[1].Err == nil || got[2].Fields[0] != "next" {
		t.Fatalf("records=%+v err=%v", got, err)
	}
}

func TestCSVOversizedRecordIsQuarantinedAndFollowingRecordContinues(t *testing.T) {
	var got []CSVRecord
	err := CSV(strings.NewReader("toolong,field\nok,2\n"), 8, func(record CSVRecord) error {
		got = append(got, record)
		return nil
	})
	if err != nil || len(got) != 2 || !errors.Is(got[0].Err, ErrRecordTooLarge) || got[1].Fields[0] != "ok" {
		t.Fatalf("records=%+v err=%v", got, err)
	}
}

func TestCSVFromOffsetsAreRestartPoints(t *testing.T) {
	data := "h1,h2\r\na,\"x\ny\"\nbad\"q,1\nlast,9" // quoted newline, malformed row, final row without newline
	var all []CSVRecord
	if err := CSV(strings.NewReader(data), 64, func(r CSVRecord) error { all = append(all, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 || all[0].Offset != 0 || all[3].NextOffset != int64(len(data)) || all[3].Err != nil || all[3].Fields[1] != "9" {
		t.Fatalf("%+v", all)
	}
	if !errors.Is(all[2].Err, ErrCSVMalformed) || all[1].Fields[1] != "x\ny" {
		t.Fatalf("%+v", all)
	}
	for i := 1; i < len(all); i++ {
		if all[i].Offset != all[i-1].NextOffset {
			t.Fatalf("gap at %d: %+v", i, all)
		}
		var resumed []CSVRecord
		err := CSVFrom(strings.NewReader(data[all[i].Offset:]), 64, all[i].Offset, all[i].Number, func(r CSVRecord) error { resumed = append(resumed, r); return nil })
		if err != nil || len(resumed) != len(all)-i || resumed[0].Number != all[i].Number || resumed[0].NextOffset != all[i].NextOffset {
			t.Fatalf("resume at %d: %+v err=%v", i, resumed, err)
		}
	}
}

func TestCSVOversizedRecordConsumesWholeRecord(t *testing.T) {
	data := "aaaaaaaaaaaaaaaaaaaa,\"b\nb\"\nok,2\n"
	var got []CSVRecord
	if err := CSV(strings.NewReader(data), 8, func(r CSVRecord) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !errors.Is(got[0].Err, ErrRecordTooLarge) || got[1].Offset != got[0].NextOffset || got[1].Fields[0] != "ok" {
		t.Fatalf("%+v", got)
	}
}
