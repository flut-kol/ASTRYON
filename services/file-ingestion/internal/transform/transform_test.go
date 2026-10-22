package transform

import (
	"encoding/csv"
	"errors"
	"strings"
	"testing"
)

func header(names ...string) []string { return names }

func TestSplitColumnPadsShortValues(t *testing.T) {
	p, err := Compile(header("name", "full"), []OpSpec{
		{Type: SplitColumn, Column: "full", Delimiter: " ", NewColumns: []string{"first", "last", "suffix"}, DropOriginal: true},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, keep, warn := p.Apply([]string{"a", "Jane"})
	if !keep {
		t.Fatal("expected row kept")
	}
	if warn == nil {
		t.Fatal("expected a padding warning")
	}
	want := []string{"a", "Jane", "", ""}
	if !equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
	if !equal(p.Header(), []string{"name", "first", "last", "suffix"}) {
		t.Fatalf("header = %v", p.Header())
	}
}

func TestSplitColumnKeepsRemainderOnExcessDelimiters(t *testing.T) {
	p, err := Compile(header("full"), []OpSpec{
		{Type: SplitColumn, Column: "full", Delimiter: " ", NewColumns: []string{"first", "rest"}, DropOriginal: true},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, keep, warn := p.Apply([]string{"Jane Q Public"})
	if !keep || warn != nil {
		t.Fatalf("keep=%v warn=%v", keep, warn)
	}
	if !equal(out, []string{"Jane", "Q Public"}) {
		t.Fatalf("got %v", out)
	}
}

func TestFindReplaceRegexCaseSensitivity(t *testing.T) {
	p, err := Compile(header("v"), []OpSpec{
		{Type: FindReplace, Column: "v", Find: "^foo$", Replace: "bar", Regex: true, CaseSensitive: true},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if out, _, _ := p.Apply([]string{"foo"}); !equal(out, []string{"bar"}) {
		t.Fatalf("got %v", out)
	}
	if out, _, _ := p.Apply([]string{"FOO"}); !equal(out, []string{"FOO"}) {
		t.Fatalf("case-sensitive match should not have fired: got %v", out)
	}

	p2, err := Compile(header("v"), []OpSpec{
		{Type: FindReplace, Column: "v", Find: "^foo$", Replace: "bar", Regex: true, CaseSensitive: false},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if out, _, _ := p2.Apply([]string{"FOO"}); !equal(out, []string{"bar"}) {
		t.Fatalf("case-insensitive match should have fired: got %v", out)
	}
}

func TestFindReplaceRejectsBadRegex(t *testing.T) {
	_, err := Compile(header("v"), []OpSpec{
		{Type: FindReplace, Column: "v", Find: "(unclosed", Replace: "x", Regex: true},
	})
	if !errors.Is(err, ErrOpsInvalid) {
		t.Fatalf("expected ErrOpsInvalid, got %v", err)
	}
}

func TestRenameColumnCollisionRejected(t *testing.T) {
	_, err := Compile(header("a", "b"), []OpSpec{
		{Type: RenameColumn, Column: "a", NewName: "b"},
	})
	if !errors.Is(err, ErrOpsInvalid) {
		t.Fatalf("expected ErrOpsInvalid, got %v", err)
	}
}

func TestReorderColumnsValidAndInvalid(t *testing.T) {
	p, err := Compile(header("a", "b", "c"), []OpSpec{
		{Type: ReorderColumns, Order: []string{"c", "a", "b"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, _, _ := p.Apply([]string{"1", "2", "3"})
	if !equal(out, []string{"3", "1", "2"}) {
		t.Fatalf("got %v", out)
	}

	if _, err := Compile(header("a", "b", "c"), []OpSpec{
		{Type: ReorderColumns, Order: []string{"a", "b"}},
	}); !errors.Is(err, ErrOpsInvalid) {
		t.Fatalf("expected ErrOpsInvalid for short order, got %v", err)
	}
	if _, err := Compile(header("a", "b", "c"), []OpSpec{
		{Type: ReorderColumns, Order: []string{"a", "b", "z"}},
	}); !errors.Is(err, ErrOpsInvalid) {
		t.Fatalf("expected ErrOpsInvalid for unknown column, got %v", err)
	}
}

func TestTrimWhitespaceColumnVsAll(t *testing.T) {
	p, err := Compile(header("a", "b"), []OpSpec{{Type: TrimWhitespace, Column: "a"}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, _, _ := p.Apply([]string{"  x  ", "  y  "})
	if !equal(out, []string{"x", "  y  "}) {
		t.Fatalf("got %v", out)
	}

	pAll, _ := Compile(header("a", "b"), []OpSpec{{Type: TrimWhitespace, AllColumns: true}})
	outAll, _, _ := pAll.Apply([]string{"  x  ", "  y  "})
	if !equal(outAll, []string{"x", "y"}) {
		t.Fatalf("got %v", outAll)
	}
	// internal whitespace must survive
	outInner, _, _ := pAll.Apply([]string{"a  b", "c"})
	if !equal(outInner, []string{"a  b", "c"}) {
		t.Fatalf("internal whitespace altered: %v", outInner)
	}
}

func TestDropEmptyRows(t *testing.T) {
	p, err := Compile(header("a", "b"), []OpSpec{{Type: DropEmptyRows}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, keep, _ := p.Apply([]string{"", "  "}); keep {
		t.Fatal("expected all-blank row dropped")
	}
	if _, keep, _ := p.Apply([]string{"", "x"}); !keep {
		t.Fatal("expected row with one non-blank cell kept")
	}
}

func TestDelimiterChangeEndToEndThroughCSVWriter(t *testing.T) {
	p, err := Compile(header("a", "b"), nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, _, _ := p.Apply([]string{"x\ty", "z"})
	var buf strings.Builder
	w := csv.NewWriter(&buf)
	w.Comma = '\t'
	if err := w.Write(out); err != nil {
		t.Fatalf("write: %v", err)
	}
	w.Flush()
	// a cell containing the output delimiter must be quoted by encoding/csv
	if !strings.Contains(buf.String(), `"x`) {
		t.Fatalf("expected quoting of a cell containing the delimiter, got %q", buf.String())
	}
}

func TestMergeColumnsDropOriginal(t *testing.T) {
	p, err := Compile(header("first", "last", "age"), []OpSpec{
		{Type: MergeColumns, Columns: []string{"first", "last"}, NewColumn: "name", Separator: " ", DropOriginal: true},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !equal(p.Header(), []string{"name", "age"}) {
		t.Fatalf("header = %v", p.Header())
	}
	out, _, _ := p.Apply([]string{"Jane", "Doe", "30"})
	if !equal(out, []string{"Jane Doe", "30"}) {
		t.Fatalf("got %v", out)
	}
}

func TestDeleteColumn(t *testing.T) {
	p, err := Compile(header("a", "b", "c"), []OpSpec{{Type: DeleteColumn, Column: "b"}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !equal(p.Header(), []string{"a", "c"}) {
		t.Fatalf("header = %v", p.Header())
	}
	out, _, _ := p.Apply([]string{"1", "2", "3"})
	if !equal(out, []string{"1", "3"}) {
		t.Fatalf("got %v", out)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
