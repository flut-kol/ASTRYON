package secrets

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// EnvProvider reads secrets from environment variables.
type EnvProvider struct{}

func (EnvProvider) Get(_ context.Context, name string) (Secret, error) {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return Secret{}, fmt.Errorf("secrets: env %s: %w", name, ErrNotFound)
	}
	return New(v), nil
}

// FileProvider reads secrets from files on every Get, so rotated files (for
// example mounted Kubernetes secrets) are picked up without restart.
type FileProvider struct{ log *slog.Logger }

// NewFileProvider returns a FileProvider; log may be nil (slog.Default).
func NewFileProvider(log *slog.Logger) *FileProvider {
	if log == nil {
		log = slog.Default()
	}
	return &FileProvider{log: log}
}

func (p *FileProvider) Get(_ context.Context, path string) (Secret, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return Secret{}, fmt.Errorf("secrets: file %s: %w", path, ErrNotFound)
	}
	if fi.Mode().Perm()&0o004 != 0 {
		p.log.Warn("secret file is world-readable", "path", path, "mode", fi.Mode().Perm().String())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Secret{}, fmt.Errorf("secrets: file %s: read failed", path)
	}
	v := strings.TrimRight(string(b), "\r\n")
	if v == "" {
		return Secret{}, fmt.Errorf("secrets: file %s: empty: %w", path, ErrNotFound)
	}
	return New(v), nil
}
