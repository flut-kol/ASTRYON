package scanner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
)

var ErrStageTooLarge = errors.New("scanner: staged file exceeds limit")

// Stage copies a bounded object into a generated, exclusive file. The caller
// must call Cleanup; the generated name never contains a user filename.
func Stage(ctx context.Context, dir string, r io.Reader, maxBytes int64) (File, func() error, error) {
	if maxBytes <= 0 {
		maxBytes = 1 << 30
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return File{}, nil, err
	}
	name := ".astryon-scan-" + hex.EncodeToString(token[:])
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return File{}, nil, err
	}
	cleanup := func() error { return os.Remove(path) }
	buf := make([]byte, 32<<10)
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			_ = f.Close()
			_ = cleanup()
			return File{}, nil, err
		}
		readN, readErr := r.Read(buf)
		if readN > 0 {
			if n+int64(readN) > maxBytes {
				_ = f.Close()
				_ = cleanup()
				return File{}, nil, ErrStageTooLarge
			}
			if _, err := f.Write(buf[:readN]); err != nil {
				_ = f.Close()
				_ = cleanup()
				return File{}, nil, err
			}
			n += int64(readN)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = f.Close()
			_ = cleanup()
			return File{}, nil, readErr
		}
	}
	if err := f.Close(); err != nil {
		_ = cleanup()
		return File{}, nil, err
	}
	return File{Path: path, Name: name, Size: n}, cleanup, nil
}
