// Package secrets resolves connector SecretRefs from the process environment or mounted files only.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

var nameRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,127}$`)

// Env implements runner.Resolver. A SecretRef "name" resolves to env ASTRYON_SECRET_<NAME> (value used as the
// Authorization header), or, if ASTRYON_SECRET_<NAME>_FILE is set, to the trimmed content of that file.
// Names are restricted so a ref in the database cannot address arbitrary env vars or paths.
type Env struct {
	Getenv   func(string) string
	ReadFile func(string) ([]byte, error)
}

func (e Env) Authorization(_ context.Context, ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	if !nameRE.MatchString(ref) {
		return "", errors.New("secrets: invalid secret ref")
	}
	getenv, read := e.Getenv, e.ReadFile
	if getenv == nil {
		getenv = os.Getenv
	}
	if read == nil {
		read = os.ReadFile
	}
	key := "ASTRYON_SECRET_" + strings.NewReplacer("-", "_", ".", "_").Replace(strings.ToUpper(ref))
	if f := getenv(key + "_FILE"); f != "" {
		b, err := read(f)
		if err != nil {
			return "", fmt.Errorf("secrets: read %s: %w", key+"_FILE", err) // path only, never content
		}
		if v := strings.TrimSpace(string(b)); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("secrets: %s is empty", key+"_FILE")
	}
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("secrets: %s is not set", key)
}
