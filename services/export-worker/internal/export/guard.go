package export

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

const exceptionJSON = `{"exception":`

var (
	ErrRowCap    = errors.New("export exceeds max_rows")
	ErrByteCap   = errors.New("export exceeds byte cap")
	ErrTruncated = errors.New("clickhouse stream truncated or errored mid-stream")
	// ErrClickHouse is a ClickHouse failure before the stream started (connect error, non-2xx status).
	ErrClickHouse = errors.New("clickhouse request failed")
)

// guard wraps the ClickHouse body: counts rows/bytes, enforces caps and detects a mid-stream server error
// (ClickHouse cannot change the HTTP status once streaming began; it appends "Code: N. DB::Exception ..."
// to the body). Memory use is O(1).
type guard struct {
	r        io.Reader
	csv      bool
	maxRows  int64
	maxBytes int64

	Rows, Bytes int64
	inQuote     bool
	lineStart   bool
	pos         int // position within current line (for prefix check)
	prefix      [13]byte
	excAt       int // index in the scanned chunk where the exception line starts
	skipHeader  bool
	last        byte
	err         error
}

func newGuard(r io.Reader, csv bool, maxRows, maxBytes int64) *guard {
	return &guard{r: r, csv: csv, maxRows: maxRows, maxBytes: maxBytes, lineStart: true, skipHeader: csv}
}

func (g *guard) Read(p []byte) (int, error) {
	if g.err != nil {
		return 0, g.err
	}
	n, err := g.r.Read(p)
	if n > 0 {
		if serr := g.scan(p[:n]); serr != nil {
			if errors.Is(serr, ErrTruncated) { // keep the exception text ("Code: 394. DB::Exception: ...") for the audit
				serr = fmt.Errorf("%w: %s", ErrTruncated, g.excerpt(p[:n]))
			}
			g.err = serr
			return 0, serr
		}
	}
	if err == io.EOF {
		if g.Bytes > 0 && g.last != '\n' {
			g.err = ErrTruncated
			return n, g.err
		}
		return n, io.EOF
	}
	if err != nil {
		g.err = fmt.Errorf("%w: %v", ErrTruncated, err)
		return n, g.err
	}
	return n, nil
}

func (g *guard) scan(b []byte) error {
	g.Bytes += int64(len(b))
	if g.Bytes > g.maxBytes {
		return ErrByteCap
	}
	for i, c := range b {
		if g.pos < len(g.prefix) {
			g.prefix[g.pos] = c
		}
		if g.pos == 0 && !g.csv && c != '{' {
			g.excAt = i
			return ErrTruncated
		}
		// ClickHouse 24.x ends a failed or KILLed JSON stream with a line {"exception": "Code: 394. ..."} that is
		// itself valid JSON and would otherwise be counted as a row of a "successful" export.
		if !g.csv && g.pos == len(exceptionJSON)-1 && string(g.prefix[:g.pos+1]) == exceptionJSON {
			g.excAt = max(0, i-g.pos)
			return ErrTruncated
		}
		if g.csv && g.pos == 5 && !g.inQuote && string(g.prefix[:6]) == "Code: " {
			g.excAt = max(0, i-5)
			return ErrTruncated
		}
		g.pos++
		g.last = c
		if c == '"' && g.csv {
			g.inQuote = !g.inQuote
		}
		if c == '\n' && !g.inQuote {
			g.pos = 0
			if g.skipHeader {
				g.skipHeader = false
				continue
			}
			g.Rows++
			if g.Rows > g.maxRows {
				return ErrRowCap
			}
		}
	}
	return nil
}

// excerpt returns the ClickHouse exception text of the chunk that tripped the guard (best effort; reads a little
// further when the text continues in the next chunk).
func (g *guard) excerpt(b []byte) string {
	if g.excAt < 0 || g.excAt > len(b) {
		return "malformed row"
	}
	t := append([]byte(nil), b[g.excAt:]...)
	for tries := 0; len(t) < 400 && tries < 4; tries++ {
		more := make([]byte, 400-len(t))
		n, err := g.r.Read(more)
		t = append(t, more[:n]...)
		if err != nil {
			break
		}
	}
	s := string(t)
	if i := strings.Index(s, "Code: "); i >= 0 {
		s = s[i:]
	}
	if len(s) > 300 {
		s = s[:300]
	}
	return strings.TrimSpace(s)
}
