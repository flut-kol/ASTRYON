package scanner

import (
	"context"
	"strings"
)

// Combined runs all configured engines for one staged file. A positive
// verdict wins over an engine error; otherwise any error is returned so the
// worker retries instead of allowing an unverified object through.
type Combined struct {
	Providers []Provider
}

func (c Combined) Name() string {
	var names []string
	for _, p := range c.Providers {
		if p != nil {
			names = append(names, p.Name())
		}
	}
	return strings.Join(names, "+")
}

func (c Combined) Scan(ctx context.Context, f File) Verdict {
	if len(c.Providers) == 0 {
		return Verdict{Status: ScanError, Provider: "none", ReasonCode: "not_configured"}
	}
	var firstErr Verdict
	for _, p := range c.Providers {
		if p == nil {
			continue
		}
		v := p.Scan(ctx, f)
		if v.Status == Infected {
			return Verdict{Status: Infected, Provider: p.Name(), ReasonCode: v.ReasonCode}
		}
		if v.Status == Encrypted {
			return Verdict{Status: Encrypted, Provider: p.Name(), ReasonCode: v.ReasonCode}
		}
		if v.Status == ScanError && firstErr.Status == "" {
			firstErr = v
		}
	}
	if firstErr.Status == ScanError {
		return firstErr
	}
	return Verdict{Status: Clean, Provider: c.Name()}
}
