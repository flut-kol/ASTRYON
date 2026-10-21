package scanner

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"
)

const (
	PasswordEnableTag   = "archive_passwords"
	PasswordCampaignTag = "archive_password_campaign"
)

// PasswordAttemptsEnabled is an explicit opt-in gate. Candidate lookup must
// not happen unless the archive carries both the enable tag and a campaign ID.
// The campaign identifier is returned trimmed; plaintext candidates are never
// represented by tags or this package.
func PasswordAttemptsEnabled(tags map[string]string) (bool, string) {
	if tags == nil {
		return false, ""
	}
	enabled := strings.ToLower(strings.TrimSpace(tags[PasswordEnableTag]))
	if enabled != "enabled" && enabled != "true" && enabled != "1" {
		return false, ""
	}
	campaign := strings.TrimSpace(tags[PasswordCampaignTag])
	if campaign == "" {
		return false, ""
	}
	return true, campaign
}

// PasswordProvider resolves an externally managed, tenant/campaign-scoped
// candidate list. Implementations must not persist or log plaintext values.
type PasswordProvider interface {
	Candidates(ctx context.Context, tenantID, campaignID string) ([]string, error)
}

type PasswordResult string

const (
	// PasswordDisabled is returned when the upload has not explicitly opted
	// into a password campaign. In this state the provider is not consulted.
	PasswordDisabled PasswordResult = "disabled"
	PasswordMatched  PasswordResult = "matched"
	PasswordNoMatch  PasswordResult = "no_match"
	PasswordTimedOut PasswordResult = "timeout"
)

var (
	ErrPasswordLimit    = errors.New("scanner: password candidate limit exceeded")
	ErrInvalidCampaign  = errors.New("scanner: invalid password campaign")
	ErrPasswordCallback = errors.New("scanner: password callback is nil")
)

var campaignIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// TryTaggedPasswordCandidates is the only supported entry point for archive
// password campaigns. It deliberately requires both the explicit enable tag
// and a campaign tag. If either is absent/invalid, the provider is not called
// and no password attempt is made. Candidates are supplied by an external,
// administrator-managed campaign; this function never generates guesses or
// dictionaries.
func TryTaggedPasswordCandidates(ctx context.Context, provider PasswordProvider, tenantID string, tags map[string]string, maxCandidates int, timeout time.Duration, try func(string) (bool, error)) (PasswordResult, error) {
	enabled, campaign := PasswordAttemptsEnabled(tags)
	if !enabled {
		return PasswordDisabled, nil
	}
	if !campaignIDPattern.MatchString(campaign) {
		return PasswordNoMatch, ErrInvalidCampaign
	}
	if provider == nil {
		return PasswordNoMatch, errors.New("scanner: password provider is nil")
	}
	if try == nil {
		return PasswordNoMatch, ErrPasswordCallback
	}
	return TryPasswordCandidates(ctx, provider, tenantID, campaign, maxCandidates, timeout, try)
}

// TryPasswordCandidates is deliberately callback-based: archive engines stay
// outside this package, and candidates never enter Verdicts, audit records or
// logs. It performs no guessing or dictionary generation.
func TryPasswordCandidates(ctx context.Context, provider PasswordProvider, tenantID, campaignID string, maxCandidates int, timeout time.Duration, try func(string) (bool, error)) (PasswordResult, error) {
	if provider == nil {
		return PasswordNoMatch, errors.New("scanner: password provider is nil")
	}
	if try == nil {
		return PasswordNoMatch, ErrPasswordCallback
	}
	if maxCandidates <= 0 {
		maxCandidates = 8
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cs, err := provider.Candidates(ctx, tenantID, campaignID)
	if err != nil {
		return PasswordNoMatch, err
	}
	if len(cs) > maxCandidates {
		return PasswordNoMatch, ErrPasswordLimit
	}
	seen := make(map[string]struct{}, len(cs))
	for _, candidate := range cs {
		if err := ctx.Err(); err != nil {
			return PasswordTimedOut, err
		}
		// Empty and duplicate candidates do not constitute useful attempts.
		// Do not trim non-empty candidates: spaces can be part of a password.
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		matched, err := try(candidate)
		if err != nil {
			return PasswordNoMatch, err
		}
		if matched {
			return PasswordMatched, nil
		}
	}
	return PasswordNoMatch, nil
}
