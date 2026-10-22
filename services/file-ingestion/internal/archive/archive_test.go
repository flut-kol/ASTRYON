package archive

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"strings"
	"testing"
)

func makeZIP(t *testing.T, name, body string) []byte {
	t.Helper()
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	f, err := z.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte(body))
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestInspectZIPAndNested(t *testing.T) {
	inner := makeZIP(t, "inner.txt", "ok")
	outer := makeZIP(t, "inner.zip", string(inner))
	r, err := Inspect(bytes.NewReader(outer), Limits{})
	if err != nil || r.Format != FormatZIP || !r.Nested || r.Files != 1 || r.ExpandedBytes == 0 {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestInspectRejectsTraversalAndBombRatio(t *testing.T) {
	for _, name := range []string{"../x", "/x", `..\\x`, `C:\\x`, "a/../../x"} {
		_, err := Inspect(bytes.NewReader(makeZIP(t, name, "x")), Limits{})
		if !errors.Is(err, ErrUnsafePath) {
			t.Errorf("%q: %v", name, err)
		}
	}
	bomb := makeZIP(t, "x", strings.Repeat("a", 1000))
	r, err := Inspect(bytes.NewReader(bomb), Limits{MaxCompressionRatio: 2})
	if !errors.Is(err, ErrLimit) || r.ReasonCode != "compression_ratio" {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestInspectTarLinksAndGzip(t *testing.T) {
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	if err := tw.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Inspect(bytes.NewReader(b.Bytes()), Limits{})
	if !errors.Is(err, ErrUnsafePath) || r.ReasonCode != "symlink" {
		t.Fatalf("%+v %v", r, err)
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write(makeZIP(t, "x", "y"))
	_ = zw.Close()
	r, err = Inspect(bytes.NewReader(gz.Bytes()), Limits{})
	if err != nil || r.Format != FormatGZIP || !r.Nested {
		t.Fatalf("%+v %v", r, err)
	}
}

func FuzzUnsafeNamesNeverPass(f *testing.F) {
	for _, seed := range []string{"../x", "/x", `..\\x`, "C:/x", "safe/name"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if strings.Contains(name, "\x00") {
			return
		}
		b := makeZIP(t, name, "x")
		r, err := Inspect(bytes.NewReader(b), Limits{})
		if strings.HasPrefix(strings.ReplaceAll(name, "\\", "/"), "../") || strings.HasPrefix(name, "/") || strings.Contains(strings.ReplaceAll(name, "\\", "/"), "/../") || (len(name) > 1 && name[1] == ':') {
			if !errors.Is(err, ErrUnsafePath) || !r.UnsafePaths {
				t.Fatalf("unsafe name accepted: %q %+v %v", name, r, err)
			}
		}
	})
}
