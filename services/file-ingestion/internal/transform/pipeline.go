package transform

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Pipeline is a compiled, ordered list of row transformations plus the header they produce. Compile resolves
// every column reference against the header state at the point the op was declared, so a later op can refer to
// a column introduced earlier (e.g. rename a column that a previous split just created).
type Pipeline struct {
	header     []string
	steps      []step
	startWidth int // width of the row passed into Apply, before any step runs (len of the initial header)
}

type step struct {
	// apply transforms one row (already padded/truncated to the current header width) and reports whether the
	// row survives (drop_empty_rows is the only op that can say no) plus a non-fatal, row-independent warning
	// (e.g. "split produced fewer parts than the declared columns"); the caller attaches row context to it.
	apply func(row []string) (out []string, keep bool, warn error)
}

func (p *Pipeline) Header() []string { return slices.Clone(p.header) }

// Apply replays the compiled steps against one data row, normalizing its width to the pipeline's starting
// header length first (short rows are zero-padded, long rows truncated) so index-based steps never panic.
func (p *Pipeline) Apply(row []string) (out []string, keep bool, warn error) {
	row = normalize(row, p.startWidth)
	keep = true
	for _, st := range p.steps {
		var k bool
		var w error
		row, k, w = st.apply(row)
		if !k {
			keep = false
		}
		if w != nil && warn == nil {
			warn = w
		}
	}
	return row, keep, warn
}

func normalize(row []string, width int) []string {
	if len(row) == width {
		return row
	}
	out := make([]string, width)
	copy(out, row)
	return out
}

// Compile validates specs against initialHeader and builds a Pipeline. All column references are resolved
// eagerly against the header as it stands at that point in the list, so every error is caught up front —
// Apply itself never fails.
func Compile(initialHeader []string, specs []OpSpec) (*Pipeline, error) {
	header := slices.Clone(initialHeader)
	p := &Pipeline{startWidth: len(header)}
	for i, spec := range specs {
		var (
			st     step
			newHdr []string
			err    error
		)
		switch spec.Type {
		case RenameColumn:
			st, newHdr, err = compileRename(header, spec)
		case SplitColumn:
			st, newHdr, err = compileSplit(header, spec)
		case MergeColumns:
			st, newHdr, err = compileMerge(header, spec)
		case FindReplace:
			st, newHdr, err = compileFindReplace(header, spec)
		case DeleteColumn:
			st, newHdr, err = compileDelete(header, spec)
		case ReorderColumns:
			st, newHdr, err = compileReorder(header, spec)
		case TrimWhitespace:
			st, newHdr, err = compileTrim(header, spec)
		case DropEmptyRows:
			st, newHdr, err = compileDropEmpty(header, spec)
		default:
			err = fmt.Errorf("%w: op %d: unknown type %q", ErrOpsInvalid, i, spec.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("op %d: %w", i, err)
		}
		header = newHdr
		p.steps = append(p.steps, st)
	}
	p.header = header
	return p, nil
}

func indexOf(header []string, name string) (int, error) {
	idx := slices.Index(header, name)
	if idx < 0 {
		return 0, fmt.Errorf("%w: unknown column %q", ErrOpsInvalid, name)
	}
	return idx, nil
}

// collides reports whether name already appears in header at a position other than skip (-1 = no exception).
func collides(header []string, name string, skip int) bool {
	for i, h := range header {
		if i != skip && h == name {
			return true
		}
	}
	return false
}

func compileRename(header []string, spec OpSpec) (step, []string, error) {
	idx, err := indexOf(header, spec.Column)
	if err != nil {
		return step{}, nil, err
	}
	if spec.NewName == "" {
		return step{}, nil, fmt.Errorf("%w: rename_column requires new_name", ErrOpsInvalid)
	}
	if collides(header, spec.NewName, idx) {
		return step{}, nil, fmt.Errorf("%w: rename_column: %q already names another column", ErrOpsInvalid, spec.NewName)
	}
	newHdr := slices.Clone(header)
	newHdr[idx] = spec.NewName
	return step{apply: func(row []string) ([]string, bool, error) { return row, true, nil }}, newHdr, nil
}

func compileSplit(header []string, spec OpSpec) (step, []string, error) {
	idx, err := indexOf(header, spec.Column)
	if err != nil {
		return step{}, nil, err
	}
	if spec.Delimiter == "" {
		return step{}, nil, fmt.Errorf("%w: split_column requires a non-empty delimiter", ErrOpsInvalid)
	}
	n := len(spec.NewColumns)
	if n < 2 {
		return step{}, nil, fmt.Errorf("%w: split_column requires at least 2 new_columns", ErrOpsInvalid)
	}
	rest := without(header, idx)
	if spec.DropOriginal {
		for _, nc := range spec.NewColumns {
			if slices.Contains(rest, nc) {
				return step{}, nil, fmt.Errorf("%w: split_column: new column %q collides with an existing column", ErrOpsInvalid, nc)
			}
		}
	} else if collidesAny(header, spec.NewColumns) {
		return step{}, nil, fmt.Errorf("%w: split_column: a new column name collides with an existing column", ErrOpsInvalid)
	}
	newHdr := make([]string, 0, len(header)+n)
	newHdr = append(newHdr, header[:idx]...)
	if !spec.DropOriginal {
		newHdr = append(newHdr, header[idx])
	}
	newHdr = append(newHdr, spec.NewColumns...)
	newHdr = append(newHdr, header[idx+1:]...)

	delim, drop := spec.Delimiter, spec.DropOriginal
	apply := func(row []string) ([]string, bool, error) {
		val := row[idx]
		parts := strings.SplitN(val, delim, n)
		var warn error
		if len(parts) < n {
			warn = fmt.Errorf("split_column on %q: value had fewer delimiter occurrences than new_columns; missing parts are empty", spec.Column)
			padded := make([]string, n)
			copy(padded, parts)
			parts = padded
		}
		out := make([]string, 0, len(row)+n)
		out = append(out, row[:idx]...)
		if !drop {
			out = append(out, val)
		}
		out = append(out, parts...)
		out = append(out, row[idx+1:]...)
		return out, true, warn
	}
	return step{apply: apply}, newHdr, nil
}

func compileMerge(header []string, spec OpSpec) (step, []string, error) {
	if len(spec.Columns) < 2 {
		return step{}, nil, fmt.Errorf("%w: merge_columns requires at least 2 columns", ErrOpsInvalid)
	}
	if spec.NewColumn == "" {
		return step{}, nil, fmt.Errorf("%w: merge_columns requires new_column", ErrOpsInvalid)
	}
	idxs := make([]int, len(spec.Columns))
	seenCol := make(map[string]bool, len(spec.Columns))
	for i, c := range spec.Columns {
		if seenCol[c] {
			return step{}, nil, fmt.Errorf("%w: merge_columns: %q listed more than once", ErrOpsInvalid, c)
		}
		seenCol[c] = true
		idx, err := indexOf(header, c)
		if err != nil {
			return step{}, nil, err
		}
		idxs[i] = idx
	}
	drop := spec.DropOriginal
	first := slices.Min(idxs)
	if drop {
		rest := header
		for _, idx := range sortedDesc(idxs) {
			rest = without(rest, idx)
		}
		if slices.Contains(rest, spec.NewColumn) {
			return step{}, nil, fmt.Errorf("%w: merge_columns: new_column %q collides with a surviving column", ErrOpsInvalid, spec.NewColumn)
		}
	} else if collides(header, spec.NewColumn, -1) {
		return step{}, nil, fmt.Errorf("%w: merge_columns: new_column %q collides with an existing column", ErrOpsInvalid, spec.NewColumn)
	}

	var newHdr []string
	if drop {
		newHdr = make([]string, 0, len(header)-len(idxs)+1)
		skip := make(map[int]bool, len(idxs))
		for _, idx := range idxs {
			skip[idx] = true
		}
		inserted := false
		for i, h := range header {
			if skip[i] {
				if i == first && !inserted {
					newHdr = append(newHdr, spec.NewColumn)
					inserted = true
				}
				continue
			}
			newHdr = append(newHdr, h)
		}
		if !inserted {
			newHdr = append(newHdr, spec.NewColumn)
		}
	} else {
		newHdr = append(slices.Clone(header), spec.NewColumn)
	}

	sep := spec.Separator
	apply := func(row []string) ([]string, bool, error) {
		vals := make([]string, len(idxs))
		for i, idx := range idxs {
			vals[i] = row[idx]
		}
		merged := strings.Join(vals, sep)
		if !drop {
			out := make([]string, 0, len(row)+1)
			out = append(out, row...)
			out = append(out, merged)
			return out, true, nil
		}
		skip := make(map[int]bool, len(idxs))
		for _, idx := range idxs {
			skip[idx] = true
		}
		out := make([]string, 0, len(row)-len(idxs)+1)
		inserted := false
		for i, v := range row {
			if skip[i] {
				if i == first && !inserted {
					out = append(out, merged)
					inserted = true
				}
				continue
			}
			out = append(out, v)
		}
		if !inserted {
			out = append(out, merged)
		}
		return out, true, nil
	}
	return step{apply: apply}, newHdr, nil
}

func compileFindReplace(header []string, spec OpSpec) (step, []string, error) {
	if !spec.AllColumns && spec.Column == "" {
		return step{}, nil, fmt.Errorf("%w: find_replace requires column or all_columns", ErrOpsInvalid)
	}
	if spec.Find == "" {
		return step{}, nil, fmt.Errorf("%w: find_replace requires a non-empty find value", ErrOpsInvalid)
	}
	idx := -1
	if !spec.AllColumns {
		i, err := indexOf(header, spec.Column)
		if err != nil {
			return step{}, nil, err
		}
		idx = i
	}
	pattern := spec.Find
	if !spec.Regex {
		pattern = regexp.QuoteMeta(pattern)
	}
	if !spec.CaseSensitive {
		pattern = "(?i)" + pattern
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return step{}, nil, fmt.Errorf("%w: find_replace: invalid pattern: %v", ErrOpsInvalid, err)
	}
	replace := spec.Replace
	apply := func(row []string) ([]string, bool, error) {
		out := slices.Clone(row)
		if idx >= 0 {
			out[idx] = re.ReplaceAllString(out[idx], replace)
		} else {
			for i, v := range out {
				out[i] = re.ReplaceAllString(v, replace)
			}
		}
		return out, true, nil
	}
	return step{apply: apply}, slices.Clone(header), nil
}

func compileDelete(header []string, spec OpSpec) (step, []string, error) {
	idx, err := indexOf(header, spec.Column)
	if err != nil {
		return step{}, nil, err
	}
	newHdr := without(header, idx)
	apply := func(row []string) ([]string, bool, error) { return without(row, idx), true, nil }
	return step{apply: apply}, newHdr, nil
}

func compileReorder(header []string, spec OpSpec) (step, []string, error) {
	if len(spec.Order) != len(header) {
		return step{}, nil, fmt.Errorf("%w: reorder_columns: order must name exactly the current %d columns", ErrOpsInvalid, len(header))
	}
	mapping := make([]int, len(spec.Order))
	seen := make(map[string]bool, len(header))
	for i, name := range spec.Order {
		idx, err := indexOf(header, name)
		if err != nil {
			return step{}, nil, err
		}
		if seen[name] {
			return step{}, nil, fmt.Errorf("%w: reorder_columns: %q listed more than once", ErrOpsInvalid, name)
		}
		seen[name] = true
		mapping[i] = idx
	}
	apply := func(row []string) ([]string, bool, error) {
		out := make([]string, len(mapping))
		for i, src := range mapping {
			out[i] = row[src]
		}
		return out, true, nil
	}
	return step{apply: apply}, slices.Clone(spec.Order), nil
}

func compileTrim(header []string, spec OpSpec) (step, []string, error) {
	if !spec.AllColumns && spec.Column == "" {
		return step{}, nil, fmt.Errorf("%w: trim_whitespace requires column or all_columns", ErrOpsInvalid)
	}
	idx := -1
	if !spec.AllColumns {
		i, err := indexOf(header, spec.Column)
		if err != nil {
			return step{}, nil, err
		}
		idx = i
	}
	apply := func(row []string) ([]string, bool, error) {
		out := slices.Clone(row)
		if idx >= 0 {
			out[idx] = strings.TrimSpace(out[idx])
		} else {
			for i, v := range out {
				out[i] = strings.TrimSpace(v)
			}
		}
		return out, true, nil
	}
	return step{apply: apply}, slices.Clone(header), nil
}

func compileDropEmpty(header []string, _ OpSpec) (step, []string, error) {
	apply := func(row []string) ([]string, bool, error) {
		for _, v := range row {
			if strings.TrimSpace(v) != "" {
				return row, true, nil
			}
		}
		return row, false, nil
	}
	return step{apply: apply}, slices.Clone(header), nil
}

func without(s []string, idx int) []string {
	out := make([]string, 0, len(s)-1)
	out = append(out, s[:idx]...)
	out = append(out, s[idx+1:]...)
	return out
}

func collidesAny(header []string, names []string) bool {
	for _, n := range names {
		if slices.Contains(header, n) {
			return true
		}
	}
	return false
}

func sortedDesc(idxs []int) []int {
	out := slices.Clone(idxs)
	slices.Sort(out)
	slices.Reverse(out)
	return out
}
