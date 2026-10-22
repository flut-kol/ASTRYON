package scanner

// Environment-backed scanner wiring lives here so binaries do not duplicate
// provider URL, timeout, staging, and fail-closed policy parsing. The zero
// value is disabled; this is intentional so existing deployments are
// unchanged until an operator opts in.

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Mode string

const (
	ModeOff      Mode = "off"
	ModeOptional Mode = "optional"
	ModeRequired Mode = "required"
)

// Config is safe to log only through Summary. It contains no credentials.
type Config struct {
	Mode              Mode
	DefenderURL       string
	ClamAVURL         string
	Timeout           time.Duration
	StageDir          string
	MaxStageBytes     int64
	CircuitFailures   int
	CircuitCooldown   time.Duration
	MaxConcurrent     int
	RequestsPerSecond int
}

// Runtime is the dependency-injection bundle passed to the worker. Scanning
// remains a separate gate; an empty Runtime has no effect on ingestion.
type Runtime struct {
	Config    Config
	Providers []Provider
	Client    *http.Client
}

// Scan fans out to all configured engines and fails closed. A positive verdict
// wins over errors; otherwise any provider error prevents ingestion. Clean is
// returned only when every configured provider is clean.
func (r *Runtime) Scan(ctx context.Context, f File) Verdict {
	if r == nil || len(r.Providers) == 0 {
		return Verdict{Status: ScanError, Provider: "scanner", ReasonCode: "not_configured"}
	}
	var firstErr Verdict
	for _, p := range r.Providers {
		v := p.Scan(ctx, f)
		if v.Status == Infected || v.Status == Encrypted {
			return v
		}
		if v.Status == ScanError && firstErr.Status == "" {
			firstErr = v
		}
	}
	if firstErr.Status == ScanError {
		return firstErr
	}
	return Verdict{Status: Clean, Provider: "combined"}
}

// LoadEnv parses FILE_SCANNER_* settings. Invalid operator configuration is a
// startup error rather than a silent fail-open. URLs are treated as opaque and
// never included in errors or logs.
func LoadEnv() (Config, error) {
	get := os.Getenv
	mode := Mode(strings.ToLower(strings.TrimSpace(get("FILE_SCANNER_MODE"))))
	if mode == "" {
		mode = ModeOff
	}
	if mode != ModeOff && mode != ModeOptional && mode != ModeRequired {
		return Config{}, fmt.Errorf("FILE_SCANNER_MODE must be off, optional, or required")
	}
	c := Config{Mode: mode, DefenderURL: strings.TrimSpace(get("FILE_SCANNER_DEFENDER_URL")), ClamAVURL: strings.TrimSpace(get("FILE_SCANNER_CLAMAV_URL")), StageDir: strings.TrimSpace(get("FILE_SCANNER_STAGE_DIR"))}
	var err error
	if c.Timeout, err = durationEnv("FILE_SCANNER_TIMEOUT", get("FILE_SCANNER_TIMEOUT"), 30*time.Second, time.Millisecond, 5*time.Minute); err != nil {
		return Config{}, err
	}
	if c.MaxStageBytes, err = int64Env("FILE_SCANNER_MAX_STAGE_BYTES", get("FILE_SCANNER_MAX_STAGE_BYTES"), 1<<30, 1, 10<<30); err != nil {
		return Config{}, err
	}
	var failures int64
	if failures, err = int64Env("FILE_SCANNER_CIRCUIT_FAILURES", get("FILE_SCANNER_CIRCUIT_FAILURES"), 3, 1, 100); err != nil {
		return Config{}, err
	}
	c.CircuitFailures = int(failures)
	if c.CircuitCooldown, err = durationEnv("FILE_SCANNER_CIRCUIT_COOLDOWN", get("FILE_SCANNER_CIRCUIT_COOLDOWN"), 30*time.Second, time.Millisecond, time.Hour); err != nil {
		return Config{}, err
	}
	var n int64
	if n, err = int64Env("FILE_SCANNER_MAX_CONCURRENT", get("FILE_SCANNER_MAX_CONCURRENT"), 4, 1, 64); err != nil {
		return Config{}, err
	}
	c.MaxConcurrent = int(n)
	if n, err = int64Env("FILE_SCANNER_RATE_PER_SECOND", get("FILE_SCANNER_RATE_PER_SECOND"), 10, 1, 1000); err != nil {
		return Config{}, err
	}
	c.RequestsPerSecond = int(n)
	if c.Mode != ModeOff && c.DefenderURL == "" && c.ClamAVURL == "" {
		return Config{}, fmt.Errorf("FILE_SCANNER_MODE requires FILE_SCANNER_DEFENDER_URL or FILE_SCANNER_CLAMAV_URL")
	}
	return c, nil
}

func durationEnv(name, raw string, def, min, max time.Duration) (time.Duration, error) {
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || d < min || d > max {
		return 0, fmt.Errorf("%s must be a duration between %s and %s", name, min, max)
	}
	return d, nil
}

func int64Env(name, raw string, def, min, max int64) (int64, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", name, min, max)
	}
	return n, nil
}

// New builds provider adapters and a shared bounded HTTP client. In off mode
// it returns an empty provider set. Required mode is validated by LoadEnv.
func New(c Config) Runtime {
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
	if c.MaxStageBytes <= 0 {
		c.MaxStageBytes = 1 << 30
	}
	if c.CircuitFailures <= 0 {
		c.CircuitFailures = 3
	}
	if c.CircuitCooldown <= 0 {
		c.CircuitCooldown = 30 * time.Second
	}
	if c.MaxConcurrent <= 0 {
		c.MaxConcurrent = 4
	}
	if c.RequestsPerSecond <= 0 {
		c.RequestsPerSecond = 10
	}
	r := Runtime{Config: c}
	if c.Mode == ModeOff {
		return r
	}
	r.Client = &http.Client{Timeout: c.Timeout}
	if c.DefenderURL != "" {
		r.Providers = append(r.Providers, &RateLimited{Provider: &CircuitBreaker{Provider: Defender{BaseURL: c.DefenderURL, Client: r.Client, Timeout: c.Timeout}, Failures: c.CircuitFailures, Cooldown: c.CircuitCooldown}, MaxConcurrent: c.MaxConcurrent, RequestsPerSecond: c.RequestsPerSecond})
	}
	if c.ClamAVURL != "" {
		r.Providers = append(r.Providers, &RateLimited{Provider: &CircuitBreaker{Provider: ClamAV{BaseURL: c.ClamAVURL, Client: r.Client, Timeout: c.Timeout}, Failures: c.CircuitFailures, Cooldown: c.CircuitCooldown}, MaxConcurrent: c.MaxConcurrent, RequestsPerSecond: c.RequestsPerSecond})
	}
	return r
}

func (c Config) Summary() string {
	providers := 0
	if c.DefenderURL != "" {
		providers++
	}
	if c.ClamAVURL != "" {
		providers++
	}
	return fmt.Sprintf("mode=%s providers=%d timeout=%s stage_limit=%d max_concurrent=%d rate_per_second=%d", c.Mode, providers, c.Timeout, c.MaxStageBytes, c.MaxConcurrent, c.RequestsPerSecond)
}
