// Package transform compiles a user-authored, ordered list of spreadsheet-prep operations (rename, split,
// merge, find/replace, delete, reorder, trim, drop-empty-rows) against a CSV header, then replays the compiled
// pipeline against each data row. It is pure: no I/O, no ClickHouse, no job-queue awareness.
package transform

import "errors"

type OpType string

const (
	RenameColumn   OpType = "rename_column"
	SplitColumn    OpType = "split_column"
	MergeColumns   OpType = "merge_columns"
	FindReplace    OpType = "find_replace"
	DeleteColumn   OpType = "delete_column"
	ReorderColumns OpType = "reorder_columns"
	TrimWhitespace OpType = "trim_whitespace"
	DropEmptyRows  OpType = "drop_empty_rows"
)

// ErrOpsInvalid wraps every validation failure raised while compiling an op pipeline.
var ErrOpsInvalid = errors.New("transform: ops invalid")

// OpSpec is the JSON wire shape for one pipeline step (see docs/api/file-ingestion.openapi.yaml).
type OpSpec struct {
	Type OpType `json:"type"`

	Column  string   `json:"column,omitempty"`  // rename_column, split_column, find_replace (scoped), trim_whitespace (scoped)
	Columns []string `json:"columns,omitempty"` // merge_columns, in join order

	NewName   string `json:"new_name,omitempty"`   // rename_column
	NewColumn string `json:"new_column,omitempty"` // merge_columns: name of the resulting column
	Separator string `json:"separator,omitempty"`  // merge_columns: join separator

	Delimiter    string   `json:"delimiter,omitempty"`     // split_column: literal substring, never a regex
	NewColumns   []string `json:"new_columns,omitempty"`   // split_column: result column names, len >= 2
	DropOriginal bool     `json:"drop_original,omitempty"` // split_column, merge_columns

	AllColumns    bool   `json:"all_columns,omitempty"` // find_replace, trim_whitespace: scope to every column
	Find          string `json:"find,omitempty"`
	Replace       string `json:"replace,omitempty"`
	Regex         bool   `json:"regex,omitempty"`
	CaseSensitive bool   `json:"case_sensitive,omitempty"`

	Order []string `json:"order,omitempty"` // reorder_columns: full target permutation of the current header
}
