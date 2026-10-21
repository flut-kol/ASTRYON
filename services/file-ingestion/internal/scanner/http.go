package scanner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxScannerResponse = 64 << 10

type Defender struct {
	BaseURL string
	Client  *http.Client
	Timeout time.Duration
}

func (d Defender) Name() string { return "defender" }
func (d Defender) Scan(ctx context.Context, f File) (v Verdict) {
	start := time.Now()
	v = Verdict{Provider: d.Name()}
	defer func() { v.Duration = time.Since(start) }()
	if f.Path == "" || d.BaseURL == "" {
		v.Status, v.ReasonCode = ScanError, "invalid_request"
		return v
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout())
	defer cancel()
	body, err := json.Marshal(struct {
		TraceID  string `json:"TraceId,omitempty"`
		FileName string `json:"FileName"`
	}{FileName: f.Path})
	if err != nil {
		v.Status, v.ReasonCode = ScanError, "request"
		return v
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint(d.BaseURL, "/scan"), bytes.NewReader(body))
	if err != nil {
		v.Status, v.ReasonCode = ScanError, "request"
		return v
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := clientOrDefault(d.Client, d.Timeout).Do(req)
	if err != nil {
		v.Status, v.ReasonCode, v.Retryable = ScanError, "transport", true
		return v
	}
	defer resp.Body.Close()
	if unavailable(resp.StatusCode) {
		v.Status, v.ReasonCode, v.Retryable = ScanError, "provider_unavailable", true
		return v
	}
	var out struct {
		Output string `json:"Output"`
		Error  string `json:"Error"`
	}
	if decodeResponse(resp.Body, &out) != nil {
		v.Status, v.ReasonCode, v.Retryable = ScanError, "malformed_response", true
		return v
	}
	if resp.StatusCode >= 400 || out.Error != "" {
		v.Status, v.ReasonCode, v.Retryable = ScanError, "provider_error", false
		return v
	}
	if defenderThreat(out.Output) {
		v.Status, v.ReasonCode = Infected, "malware_detected"
	} else {
		v.Status = Clean
	}
	return v
}
func (d Defender) timeout() time.Duration {
	if d.Timeout > 0 {
		return d.Timeout
	}
	return 10 * time.Second
}

type ClamAV struct {
	BaseURL string
	Client  *http.Client
	Timeout time.Duration
}

func (c ClamAV) Name() string { return "clamav" }
func (c ClamAV) Scan(ctx context.Context, f File) (v Verdict) {
	start := time.Now()
	v = Verdict{Provider: c.Name()}
	defer func() { v.Duration = time.Since(start) }()
	if f.Path == "" || c.BaseURL == "" {
		v.Status, v.ReasonCode = ScanError, "invalid_request"
		return v
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	body, err := json.Marshal(struct {
		FileName string `json:"FileName"`
	}{f.Path})
	if err != nil {
		v.Status, v.ReasonCode = ScanError, "request"
		return v
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint(c.BaseURL, "/clamav/scanfile"), bytes.NewReader(body))
	if err != nil {
		v.Status, v.ReasonCode = ScanError, "request"
		return v
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := clientOrDefault(c.Client, c.Timeout).Do(req)
	if err != nil {
		v.Status, v.ReasonCode, v.Retryable = ScanError, "transport", true
		return v
	}
	defer resp.Body.Close()
	var out struct {
		IsInfected  bool `json:"isInfected"`
		IsEncrypted bool `json:"isEncrypted"`
	}
	if decodeResponse(resp.Body, &out) != nil {
		v.Status, v.ReasonCode, v.Retryable = ScanError, "malformed_response", true
		return v
	}
	if out.IsInfected {
		v.Status, v.ReasonCode = Infected, "malware_detected"
	} else if out.IsEncrypted {
		v.Status, v.ReasonCode = Encrypted, "encrypted"
	} else if unavailable(resp.StatusCode) {
		v.Status, v.ReasonCode, v.Retryable = ScanError, "provider_unavailable", true
	} else if resp.StatusCode >= 400 {
		v.Status, v.ReasonCode, v.Retryable = ScanError, "provider_error", false
	} else {
		v.Status = Clean
	}
	return v
}
func (c ClamAV) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return 10 * time.Second
}

// Healthy checks readiness without exposing the provider response body.
func (d Defender) Healthy(ctx context.Context) error {
	return health(ctx, d.Client, d.timeout(), endpoint(d.BaseURL, "/ping"), func(body []byte) bool {
		var out struct {
			IsHealthy any `json:"IsHealthy"`
		}
		if json.Unmarshal(body, &out) != nil {
			return false
		}
		switch x := out.IsHealthy.(type) {
		case bool:
			return x
		case string:
			return strings.EqualFold(strings.TrimSpace(x), "true")
		default:
			return false
		}
	})
}

// Healthy checks the ClamAV adapter readiness endpoint.
func (c ClamAV) Healthy(ctx context.Context) error {
	return health(ctx, c.Client, c.timeout(), endpoint(c.BaseURL, "/health"), func(body []byte) bool {
		var out struct {
			Message string `json:"message"`
		}
		return json.Unmarshal(body, &out) == nil && strings.TrimSpace(out.Message) != ""
	})
}

func endpoint(base, path string) string { return strings.TrimRight(base, "/") + path }

func unavailable(status int) bool {
	return status >= 500 || status == http.StatusTooManyRequests || status == http.StatusRequestTimeout
}

func decodeResponse(r io.Reader, dst any) error {
	return json.NewDecoder(io.LimitReader(r, maxScannerResponse)).Decode(dst)
}

func transportReason(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timeout"
	}
	return "transport"
}

func defenderThreat(output string) bool {
	s := strings.ToLower(strings.Join(strings.Fields(output), " "))
	if s == "" || strings.Contains(s, "no threats found") || strings.Contains(s, "no threat found") {
		return false
	}
	// Defender emits "Threat ... identified". A bare mention of "threat"
	// (for example in a diagnostic sentence) is not a positive verdict.
	return strings.Contains(s, "threat") && (strings.Contains(s, "identified") || strings.Contains(s, "detected"))
}

func health(ctx context.Context, client *http.Client, timeout time.Duration, url string, valid func([]byte) bool) error {
	if strings.TrimSpace(url) == "" || !strings.Contains(url, "://") {
		return fmt.Errorf("scanner: invalid health endpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := clientOrDefault(client, timeout).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxScannerResponse))
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 || !valid(body) {
		return fmt.Errorf("scanner: unhealthy")
	}
	return nil
}
