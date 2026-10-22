// Package archive inspects archive metadata without extracting files to disk.
// It is intentionally not part of the default ingestion path: callers must
// opt in and provide limits. No archive entry is ever executed or persisted.
package archive

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

type Format string

const (
	FormatUnknown Format = "unknown"
	FormatZIP     Format = "zip"
	FormatTAR     Format = "tar"
	FormatGZIP    Format = "gzip"
)

var (
	ErrLimit       = errors.New("archive: safety limit exceeded")
	ErrUnsafePath  = errors.New("archive: unsafe entry path")
	ErrEncrypted   = errors.New("archive: encrypted entry")
	ErrUnsupported = errors.New("archive: unsupported or invalid format")
)

// Limits are hard bounds. Zero values are replaced by SafeLimits.
type Limits struct {
	MaxDepth            int
	MaxFiles            int
	MaxExpandedBytes    int64
	MaxCompressedBytes  int64
	MaxCompressionRatio int64
}

func SafeLimits() Limits {
	return Limits{MaxDepth: 2, MaxFiles: 10000, MaxExpandedBytes: 1 << 30, MaxCompressedBytes: 64 << 20, MaxCompressionRatio: 100}
}

type Entry struct {
	Name            string `json:"name"`
	Format          Format `json:"format"`
	CompressedBytes uint64 `json:"compressed_bytes"`
	ExpandedBytes   uint64 `json:"expanded_bytes"`
	Encrypted       bool   `json:"encrypted,omitempty"`
	Symlink         bool   `json:"symlink,omitempty"`
	UnsafePath      bool   `json:"unsafe_path,omitempty"`
}

type Report struct {
	Format          Format  `json:"format"`
	Files           int     `json:"files"`
	ExpandedBytes   uint64  `json:"expanded_bytes"`
	CompressedBytes uint64  `json:"compressed_bytes"`
	Nested          bool    `json:"nested,omitempty"`
	Encrypted       bool    `json:"encrypted,omitempty"`
	UnsafePaths     bool    `json:"unsafe_paths,omitempty"`
	LimitExceeded   bool    `json:"limit_exceeded,omitempty"`
	ReasonCode      string  `json:"reason_code,omitempty"`
	Entries         []Entry `json:"entries,omitempty"`
}

func normalizeLimits(l Limits) Limits {
	d := SafeLimits()
	if l.MaxDepth <= 0 {
		l.MaxDepth = d.MaxDepth
	}
	if l.MaxFiles <= 0 {
		l.MaxFiles = d.MaxFiles
	}
	if l.MaxExpandedBytes <= 0 {
		l.MaxExpandedBytes = d.MaxExpandedBytes
	}
	if l.MaxCompressedBytes <= 0 {
		l.MaxCompressedBytes = d.MaxCompressedBytes
	}
	if l.MaxCompressionRatio <= 0 {
		l.MaxCompressionRatio = d.MaxCompressionRatio
	}
	return l
}

// Inspect reads at most MaxCompressedBytes and inspects archive headers. It
// returns a report even when a safety violation is found, together with an
// error suitable for fail-closed callers.
func Inspect(r io.Reader, limits Limits) (Report, error) {
	return InspectContext(context.Background(), r, limits)
}

// InspectContext is cancellable. ZIP metadata is bounded in memory because
// archive/zip requires ReaderAt; streaming formats are processed from that
// bounded buffer and no archive entry is written to disk.
func InspectContext(ctx context.Context, r io.Reader, limits Limits) (Report, error) {
	limits = normalizeLimits(limits)
	b, err := readBounded(ctx, r, limits.MaxCompressedBytes)
	if err != nil {
		return Report{}, err
	}
	if int64(len(b)) > limits.MaxCompressedBytes {
		return Report{LimitExceeded: true, ReasonCode: "compressed_bytes"}, ErrLimit
	}
	return inspectBytes(b, limits, 0)
}

func readBounded(ctx context.Context, r io.Reader, max int64) ([]byte, error) {
	var out bytes.Buffer
	buf := make([]byte, 32<<10)
	for int64(out.Len()) <= max {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		want := int64(len(buf))
		if rem := max + 1 - int64(out.Len()); want > rem {
			want = rem
		}
		if want <= 0 {
			break
		}
		n, err := r.Read(buf[:want])
		if n > 0 {
			_, _ = out.Write(buf[:n])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
	}
	return out.Bytes(), nil
}

func inspectBytes(b []byte, limits Limits, depth int) (Report, error) {
	if depth > limits.MaxDepth {
		return Report{LimitExceeded: true, ReasonCode: "nesting_depth"}, ErrLimit
	}
	f := detect(b)
	switch f {
	case FormatZIP:
		return inspectZIP(b, limits, depth)
	case FormatGZIP:
		return inspectGZIP(b, limits, depth)
	case FormatTAR:
		return inspectTAR(b, limits, depth)
	default:
		return Report{Format: FormatUnknown}, ErrUnsupported
	}
}

func detect(b []byte) Format {
	if len(b) >= 4 && (bytes.Equal(b[:4], []byte("PK\x03\x04")) || bytes.Equal(b[:4], []byte("PK\x05\x06")) || bytes.Equal(b[:4], []byte("PK\x07\x08"))) {
		return FormatZIP
	}
	if len(b) >= 2 && b[0] == 0x1f && b[1] == 0x8b {
		return FormatGZIP
	}
	if len(b) >= 262 && string(b[257:262]) == "ustar" {
		return FormatTAR
	}
	return FormatUnknown
}

func inspectZIP(b []byte, l Limits, depth int) (Report, error) {
	z, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return Report{Format: FormatZIP}, fmt.Errorf("%w: %v", ErrUnsupported, err)
	}
	r := Report{Format: FormatZIP, CompressedBytes: uint64(len(b))}
	for _, f := range z.File {
		if f.FileInfo().IsDir() {
			continue
		}
		r.Files++
		if r.Files > l.MaxFiles {
			r.LimitExceeded = true
			r.ReasonCode = "file_count"
			return r, ErrLimit
		}
		name := f.Name
		e := Entry{Name: name, Format: FormatZIP, CompressedBytes: f.CompressedSize64, ExpandedBytes: f.UncompressedSize64}
		if unsafeName(name) {
			e.UnsafePath, r.UnsafePaths = true, true
			r.ReasonCode = "unsafe_path"
			r.Entries = append(r.Entries, e)
			return r, ErrUnsafePath
		}
		if f.Flags&1 != 0 {
			e.Encrypted, r.Encrypted = true, true
			r.ReasonCode = "encrypted"
			r.Entries = append(r.Entries, e)
			return r, ErrEncrypted
		}
		if f.Mode()&0o170000 == 0o120000 {
			e.Symlink = true
			r.ReasonCode = "symlink"
			r.Entries = append(r.Entries, e)
			return r, ErrUnsafePath
		}
		if f.UncompressedSize64 > uint64(l.MaxExpandedBytes) || (f.CompressedSize64 > 0 && f.UncompressedSize64/f.CompressedSize64 > uint64(l.MaxCompressionRatio)) {
			r.LimitExceeded = true
			r.ReasonCode = "compression_ratio"
			r.Entries = append(r.Entries, e)
			return r, ErrLimit
		}
		r.ExpandedBytes += f.UncompressedSize64
		if r.ExpandedBytes > uint64(l.MaxExpandedBytes) {
			r.LimitExceeded = true
			r.ReasonCode = "expanded_bytes"
			r.Entries = append(r.Entries, e)
			return r, ErrLimit
		}
		if nested, nerr := zipNested(f, l, depth); nerr != nil {
			return r, nerr
		} else if nested {
			r.Nested = true
		}
		r.Entries = append(r.Entries, e)
	}
	return r, nil
}

func zipNested(f *zip.File, l Limits, depth int) (bool, error) {
	rc, err := f.Open()
	if err != nil {
		return false, err
	}
	defer rc.Close()
	prefix, err := io.ReadAll(io.LimitReader(rc, 512))
	if err != nil {
		return false, err
	}
	if detect(prefix) == FormatUnknown {
		return false, nil
	}
	if depth+1 > l.MaxDepth {
		return false, ErrLimit
	}
	return true, nil // nested archive is reported; caller may inspect it explicitly under its own budget.
}

func inspectGZIP(b []byte, l Limits, depth int) (Report, error) {
	g, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return Report{Format: FormatGZIP}, fmt.Errorf("%w: %v", ErrUnsupported, err)
	}
	defer g.Close()
	var out bytes.Buffer
	n, err := io.CopyN(&out, g, l.MaxExpandedBytes+1)
	if err != nil && !errors.Is(err, io.EOF) {
		return Report{Format: FormatGZIP}, err
	}
	r := Report{Format: FormatGZIP, Files: 1, CompressedBytes: uint64(len(b)), ExpandedBytes: uint64(n)}
	if n > l.MaxExpandedBytes || (len(b) > 0 && n/int64(len(b)) > l.MaxCompressionRatio) {
		r.LimitExceeded = true
		r.ReasonCode = "compression_ratio"
		return r, ErrLimit
	}
	if f := detect(out.Bytes()); f != FormatUnknown {
		r.Nested = true
		if depth+1 > l.MaxDepth {
			r.LimitExceeded = true
			return r, ErrLimit
		}
	}
	return r, nil
}

func inspectTAR(b []byte, l Limits, depth int) (Report, error) {
	t := tar.NewReader(bytes.NewReader(b))
	r := Report{Format: FormatTAR, CompressedBytes: uint64(len(b))}
	for {
		h, err := t.Next()
		if errors.Is(err, io.EOF) {
			return r, nil
		}
		if err != nil {
			return r, fmt.Errorf("%w: %v", ErrUnsupported, err)
		}
		if h.Typeflag == tar.TypeDir {
			continue
		}
		r.Files++
		if r.Files > l.MaxFiles {
			r.LimitExceeded = true
			r.ReasonCode = "file_count"
			return r, ErrLimit
		}
		e := Entry{Name: h.Name, Format: FormatTAR, CompressedBytes: uint64(h.Size), ExpandedBytes: uint64(h.Size)}
		if unsafeName(h.Name) {
			e.UnsafePath, r.UnsafePaths = true, true
			r.ReasonCode = "unsafe_path"
			return r, ErrUnsafePath
		}
		if h.Typeflag == tar.TypeSymlink || h.Typeflag == tar.TypeLink {
			e.Symlink = true
			r.ReasonCode = "symlink"
			return r, ErrUnsafePath
		}
		r.ExpandedBytes += uint64(h.Size)
		if r.ExpandedBytes > uint64(l.MaxExpandedBytes) {
			r.LimitExceeded = true
			r.ReasonCode = "expanded_bytes"
			return r, ErrLimit
		}
		r.Entries = append(r.Entries, e)
	}
}

func unsafeName(name string) bool {
	n := strings.ReplaceAll(name, "\\", "/")
	return n == "" || strings.ContainsRune(n, 0) || strings.HasPrefix(n, "/") || strings.HasPrefix(n, "//") || strings.HasPrefix(n, "../") || strings.Contains(n, "/../") || path.Clean(n) == ".." || strings.HasPrefix(n, "../") || (len(n) >= 2 && n[1] == ':')
}
