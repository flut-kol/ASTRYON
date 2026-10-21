package secrets

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestEnvResolver(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "tok")
	if err := os.WriteFile(f, []byte("Bearer fromfile\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"ASTRYON_SECRET_PARTNER_A": " Bearer abc ", "ASTRYON_SECRET_B_FILE": f, "ASTRYON_SECRET_C_FILE": "/nonexistent"}
	r := Env{Getenv: func(k string) string { return env[k] }}
	ctx := context.Background()
	for ref, want := range map[string]string{"": "", "partner-a": "Bearer abc", "b": "Bearer fromfile"} {
		got, err := r.Authorization(ctx, ref)
		if err != nil || got != want {
			t.Errorf("%q: got %q, %v want %q", ref, got, err, want)
		}
	}
	for _, ref := range []string{"missing", "c", "../etc/passwd", "a b", "x$(y)"} {
		if _, err := r.Authorization(ctx, ref); err == nil {
			t.Errorf("%q: expected error", ref)
		}
	}
}
