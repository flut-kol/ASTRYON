package scanner

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDefenderNormalizesAndDoesNotLeakOutput(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Output":"Threat SecretToken identified","Error":null}`))
	}))
	defer ts.Close()
	v := (Defender{BaseURL: ts.URL}).Scan(context.Background(), File{Path: "/tmp/generated"})
	if v.Status != Infected || v.ReasonCode != "malware_detected" || strings.Contains(v.ReasonCode, "Secret") || v.Duration <= 0 {
		t.Fatalf("%+v", v)
	}
}

func TestDefenderDoesNotTreatDiagnosticThreatTextAsDetection(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"Output":"No threats found","Error":null}`))
	}))
	defer ts.Close()
	v := (Defender{BaseURL: ts.URL}).Scan(context.Background(), File{Path: "/tmp/generated"})
	if v.Status != Clean {
		t.Fatalf("%+v", v)
	}
}

func TestClamInfectionCanUseLegacyBadRequest(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"isInfected":true,"isEncrypted":false,"error":"engine result"}`))
	}))
	defer ts.Close()
	v := (ClamAV{BaseURL: ts.URL}).Scan(context.Background(), File{Path: "/tmp/generated"})
	if v.Status != Infected || v.ReasonCode != "malware_detected" {
		t.Fatalf("%+v", v)
	}
}

func TestScannerHealthEndpoints(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ping":
			_, _ = w.Write([]byte(`{"IsHealthy":"true"}`))
		case "/health":
			_, _ = w.Write([]byte(`{"message":"clamd is running"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	if err := (Defender{BaseURL: ts.URL}).Healthy(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := (ClamAV{BaseURL: ts.URL}).Healthy(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClamTimeoutIsRetryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(100 * time.Millisecond) }))
	defer ts.Close()
	v := (ClamAV{BaseURL: ts.URL, Timeout: 5 * time.Millisecond}).Scan(context.Background(), File{Path: "/tmp/generated"})
	if v.Status != ScanError || !v.Retryable || v.ReasonCode != "transport" {
		t.Fatalf("%+v", v)
	}
}

func TestDefenderUnavailableIsRetryableAndDoesNotBecomeClean(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"Output":"","Error":"temporarily unavailable"}`))
	}))
	defer ts.Close()
	v := (Defender{BaseURL: ts.URL}).Scan(context.Background(), File{Path: "/tmp/staged"})
	if v.Status != ScanError || !v.Retryable || v.ReasonCode != "provider_unavailable" {
		t.Fatalf("unavailable scanner must fail closed with retryable verdict: %+v", v)
	}
	if v.Status == Clean {
		t.Fatal("unavailable scanner must never be treated as clean")
	}
}

func TestClamMalformedResponseIsRetryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"isInfected":`))
	}))
	defer ts.Close()
	v := (ClamAV{BaseURL: ts.URL}).Scan(context.Background(), File{Path: "/tmp/staged"})
	if v.Status != ScanError || !v.Retryable || v.ReasonCode != "malformed_response" {
		t.Fatalf("malformed scanner response must be retryable and fail closed: %+v", v)
	}
}

func TestStageBoundsAndCleans(t *testing.T) {
	d := t.TempDir()
	f, cleanup, err := Stage(context.Background(), d, bytes.NewBufferString("abc"), 3)
	if err != nil || f.Size != 3 {
		t.Fatalf("%+v %v", f, err)
	}
	if _, err := os.Stat(f.Path); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.Path); !os.IsNotExist(err) {
		t.Fatalf("not cleaned: %v", err)
	}
	_, _, err = Stage(context.Background(), d, bytes.NewBufferString("abcd"), 3)
	if err != ErrStageTooLarge {
		t.Fatalf("%v", err)
	}
}

func TestCircuitOpensAfterFailures(t *testing.T) {
	p := fakeProvider{v: Verdict{Status: ScanError, Provider: "fake", Retryable: true, ReasonCode: "transport"}}
	c := CircuitBreaker{Provider: p, Failures: 2, Cooldown: time.Minute}
	_ = c.Scan(context.Background(), File{})
	_ = c.Scan(context.Background(), File{})
	v := c.Scan(context.Background(), File{})
	if v.ReasonCode != "circuit_open" || !v.Retryable {
		t.Fatalf("%+v", v)
	}
}

func TestCircuitMissingProviderFailsClosed(t *testing.T) {
	v := (&CircuitBreaker{}).Scan(context.Background(), File{Path: "/tmp/staged"})
	if v.Status != ScanError || v.Retryable || v.ReasonCode != "not_configured" {
		t.Fatalf("missing scanner must not silently pass: %+v", v)
	}
}

func TestCircuitOpenDoesNotCallProviderAndRecoversAfterCooldown(t *testing.T) {
	p := &countingProvider{verdict: Verdict{Status: ScanError, Provider: "fake", Retryable: true, ReasonCode: "transport"}}
	c := CircuitBreaker{Provider: p, Failures: 1, Cooldown: 10 * time.Millisecond}
	first := c.Scan(context.Background(), File{})
	if first.Status != ScanError || p.calls != 1 {
		t.Fatalf("first provider error: verdict=%+v calls=%d", first, p.calls)
	}
	open := c.Scan(context.Background(), File{})
	if open.ReasonCode != "circuit_open" || !open.Retryable || p.calls != 1 {
		t.Fatalf("open circuit must avoid provider call: verdict=%+v calls=%d", open, p.calls)
	}
	time.Sleep(15 * time.Millisecond)
	p.verdict = Verdict{Status: Clean, Provider: "fake"}
	recovered := c.Scan(context.Background(), File{})
	if recovered.Status != Clean || p.calls != 2 {
		t.Fatalf("circuit should recover after cooldown: verdict=%+v calls=%d", recovered, p.calls)
	}
}

func TestPasswordCandidatesAreCappedAndNeverReturned(t *testing.T) {
	p := passwordProvider{candidates: []string{"one", "two", "three"}}
	_, err := TryPasswordCandidates(context.Background(), p, "tenant", "campaign", 2, time.Second, func(candidate string) (bool, error) {
		if candidate == "two" {
			t.Fatal("candidate should not be reached after cap")
		}
		return false, nil
	})
	if err != ErrPasswordLimit {
		t.Fatalf("%v", err)
	}
	p.candidates = []string{"secret"}
	var seen string
	result, err := TryPasswordCandidates(context.Background(), p, "tenant", "campaign", 2, time.Second, func(candidate string) (bool, error) { seen = candidate; return true, nil })
	if err != nil || result != PasswordMatched || seen != "secret" {
		t.Fatalf("%v %v", result, err)
	}
}

func TestPasswordAttemptsRequireExplicitTags(t *testing.T) {
	for name, tags := range map[string]map[string]string{
		"nil":              nil,
		"missing campaign": {PasswordEnableTag: "enabled"},
		"disabled":         {PasswordEnableTag: "false", PasswordCampaignTag: "c1"},
		"blank campaign":   {PasswordEnableTag: "true", PasswordCampaignTag: "  "},
	} {
		if enabled, campaign := PasswordAttemptsEnabled(tags); enabled || campaign != "" {
			t.Errorf("%s: enabled=%v campaign=%q", name, enabled, campaign)
		}
	}
	for _, value := range []string{"enabled", "true", "1", " TRUE "} {
		if enabled, campaign := PasswordAttemptsEnabled(map[string]string{PasswordEnableTag: value, PasswordCampaignTag: " campaign-7 "}); !enabled || campaign != "campaign-7" {
			t.Fatalf("%q: enabled=%v campaign=%q", value, enabled, campaign)
		}
	}
}

func TestTaggedPasswordCandidatesDoNotConsultProviderWhenDisabled(t *testing.T) {
	called := false
	p := passwordProvider{candidates: []string{"secret"}}
	result, err := TryTaggedPasswordCandidates(context.Background(), providerFunc(func(context.Context, string, string) ([]string, error) {
		called = true
		return p.candidates, nil
	}), "tenant", map[string]string{}, 4, time.Second, func(string) (bool, error) {
		t.Fatal("password callback must not run without the opt-in tag")
		return false, nil
	})
	if err != nil || result != PasswordDisabled || called {
		t.Fatalf("result=%v err=%v provider_called=%v", result, err, called)
	}
}

func TestTaggedPasswordCandidatesUseOnlyCampaignAndDeduplicate(t *testing.T) {
	var gotTenant, gotCampaign string
	var attempts []string
	result, err := TryTaggedPasswordCandidates(context.Background(), providerFunc(func(_ context.Context, tenant, campaign string) ([]string, error) {
		gotTenant, gotCampaign = tenant, campaign
		return []string{"one", "one", "", "two"}, nil
	}), "tenant-7", map[string]string{PasswordEnableTag: "enabled", PasswordCampaignTag: " campaign-7 "}, 4, time.Second, func(candidate string) (bool, error) {
		attempts = append(attempts, candidate)
		return false, nil
	})
	if err != nil || result != PasswordNoMatch || gotTenant != "tenant-7" || gotCampaign != "campaign-7" {
		t.Fatalf("result=%v err=%v tenant=%q campaign=%q", result, err, gotTenant, gotCampaign)
	}
	if strings.Join(attempts, ",") != "one,two" {
		t.Fatalf("attempts=%v", attempts)
	}
}

func TestTaggedPasswordCandidatesRejectInvalidCampaign(t *testing.T) {
	called := false
	result, err := TryTaggedPasswordCandidates(context.Background(), providerFunc(func(context.Context, string, string) ([]string, error) {
		called = true
		return nil, nil
	}), "tenant", map[string]string{PasswordEnableTag: "true", PasswordCampaignTag: "../secret"}, 4, time.Second, func(string) (bool, error) { return false, nil })
	if result != PasswordNoMatch || err != ErrInvalidCampaign || called {
		t.Fatalf("result=%v err=%v provider_called=%v", result, err, called)
	}
}

func TestRateLimitedStopsCanceledWaitWithoutCallingProvider(t *testing.T) {
	p := &countingProvider{verdict: Verdict{Status: Clean, Provider: "fake"}}
	r := &RateLimited{Provider: p, MaxConcurrent: 1, RequestsPerSecond: 1}
	if got := r.Scan(context.Background(), File{Path: "/tmp/a"}); got.Status != Clean {
		t.Fatalf("first scan = %+v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := r.Scan(ctx, File{Path: "/tmp/b"})
	if got.ReasonCode != "rate_limited" || !got.Retryable || p.calls != 1 {
		t.Fatalf("canceled rate wait = %+v calls=%d", got, p.calls)
	}
}

type passwordProvider struct{ candidates []string }

func (p passwordProvider) Candidates(context.Context, string, string) ([]string, error) {
	return p.candidates, nil
}

type providerFunc func(context.Context, string, string) ([]string, error)

func (f providerFunc) Candidates(ctx context.Context, tenant, campaign string) ([]string, error) {
	return f(ctx, tenant, campaign)
}

type fakeProvider struct{ v Verdict }

func (f fakeProvider) Name() string                       { return "fake" }
func (f fakeProvider) Scan(context.Context, File) Verdict { return f.v }

type countingProvider struct {
	verdict Verdict
	calls   int
}

func (p *countingProvider) Name() string { return "fake" }

func (p *countingProvider) Scan(context.Context, File) Verdict {
	p.calls++
	return p.verdict
}
