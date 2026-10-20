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
