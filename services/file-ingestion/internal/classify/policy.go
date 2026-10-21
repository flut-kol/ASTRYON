package classify

import (
	"fmt"
	"strings"
)

// Policy is what to do with a file whose content contains credentials.
type Policy string

const (
	Quarantine Policy = "quarantine" // job ends QUARANTINED, nothing reaches the sink, sensitivity raised to restricted
	Block      Policy = "block"      // job FAILED with CREDENTIALS_DETECTED
	Allow      Policy = "allow"      // tag only
	Tag        Policy = "tag"        // synonym of allow: label the job, nothing else
)

// CodeCredentials is the catalog error code recorded when the Block policy fails a job.
const CodeCredentials = "CREDENTIALS_DETECTED"

// ParsePolicy accepts quarantine|block|allow ("" = quarantine).
func ParsePolicy(s string) (Policy, error) {
	switch p := Policy(strings.ToLower(strings.TrimSpace(s))); p {
	case "":
		return Quarantine, nil
	case Quarantine, Block, Allow, Tag:
		return p, nil
	}
	return "", fmt.Errorf("classify: unknown policy %q (want quarantine|block|allow)", s)
}

// PolicySet resolves the policy per source, then per tenant, then the default.
type PolicySet struct {
	Default Policy
	// Overrides keys: "<tenant_id>/<source_id>" or "<tenant_id>".
	Overrides map[string]Policy
}

// ParseOverrides parses "tenant/source=block,tenant=allow".
func ParseOverrides(s string) (map[string]Policy, error) {
	out := map[string]Policy{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(k) == "" {
			return nil, fmt.Errorf("classify: bad override %q (want tenant[/source]=policy)", part)
		}
		p, err := ParsePolicy(v)
		if err != nil {
			return nil, err
		}
		out[strings.TrimSpace(k)] = p
	}
	return out, nil
}

// For returns the effective policy; the zero PolicySet quarantines.
func (ps PolicySet) For(tenant, source string) Policy {
	if p, ok := ps.Overrides[tenant+"/"+source]; ok {
		return p
	}
	if p, ok := ps.Overrides[tenant]; ok {
		return p
	}
	if ps.Default == "" {
		return Quarantine
	}
	return ps.Default
}

// Explicit returns a tenant/source or tenant override if one is configured.
func (ps PolicySet) Explicit(tenant, source string) (Policy, bool) {
	if p, ok := ps.Overrides[tenant+"/"+source]; ok {
		return p, true
	}
	p, ok := ps.Overrides[tenant]
	return p, ok
}
