package scanner

import (
	"os"
	"strings"
	"testing"
)

func clearScannerEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"FILE_SCANNER_MODE", "FILE_SCANNER_DEFENDER_URL", "FILE_SCANNER_CLAMAV_URL", "FILE_SCANNER_TIMEOUT", "FILE_SCANNER_STAGE_DIR", "FILE_SCANNER_MAX_STAGE_BYTES", "FILE_SCANNER_CIRCUIT_FAILURES", "FILE_SCANNER_CIRCUIT_COOLDOWN"} {
		t.Setenv(k, "")
	}
}

func TestLoadEnvDefaultsOff(t *testing.T) {
	clearScannerEnv(t)
	c, err := LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.Mode != ModeOff || c.Timeout.String() != "30s" || c.MaxStageBytes != 1<<30 {
		t.Fatalf("defaults: %+v", c)
	}
	if got := New(c); len(got.Providers) != 0 || got.Client != nil {
		t.Fatalf("scanner should be disabled: %+v", got)
	}
}

func TestLoadEnvBuildsConfiguredProvidersWithoutSecrets(t *testing.T) {
	clearScannerEnv(t)
	t.Setenv("FILE_SCANNER_MODE", "required")
	t.Setenv("FILE_SCANNER_DEFENDER_URL", "http://defender.internal")
	t.Setenv("FILE_SCANNER_CLAMAV_URL", "http://clamav.internal")
	t.Setenv("FILE_SCANNER_TIMEOUT", "2s")
	t.Setenv("FILE_SCANNER_MAX_STAGE_BYTES", "4096")
	c, err := LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	r := New(c)
	if len(r.Providers) != 2 || r.Client == nil || r.Client.Timeout.String() != "2s" {
		t.Fatalf("runtime: %+v", r)
	}
	if strings.Contains(c.Summary(), "internal") {
		t.Fatalf("summary leaked endpoint: %s", c.Summary())
	}
}

func TestLoadEnvRejectsRequiredWithoutProvider(t *testing.T) {
	clearScannerEnv(t)
	t.Setenv("FILE_SCANNER_MODE", "required")
	if _, err := LoadEnv(); err == nil {
		t.Fatal("expected missing provider error")
	}
}

func TestLoadEnvRejectsInvalidBounds(t *testing.T) {
	clearScannerEnv(t)
	t.Setenv("FILE_SCANNER_TIMEOUT", "1us")
	if _, err := LoadEnv(); err == nil {
		t.Fatal("expected timeout bounds error")
	}
	t.Setenv("FILE_SCANNER_TIMEOUT", "")
	t.Setenv("FILE_SCANNER_MAX_STAGE_BYTES", "0")
	if _, err := LoadEnv(); err == nil {
		t.Fatal("expected stage bounds error")
	}
}

func TestLoadEnvDoesNotReadCredentialLikeValues(t *testing.T) {
	clearScannerEnv(t)
	_ = os.Setenv("FILE_SCANNER_DEFENDER_URL", "http://user:secret@defender.internal")
	c, err := LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(c.Summary(), "secret") {
		t.Fatal("summary leaked credential")
	}
}
