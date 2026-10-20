package replay

import "errors"

var ErrForbidden = errors.New("replay request forbidden")

const ReplayScope = "astryon:quarantine:replay"

type Principal struct {
	TenantID string
	Scopes   map[string]bool
}
type Request struct {
	TenantID, Source, FromOffset, ToOffset, PipelineVersion string
	DryRun                                                  bool
}

// Validate requires an explicit bounded source range and tenant-scoped,
// privileged intent. Callers must execute dry-run before a real replay policy.
func Validate(p Principal, r Request) error {
	if p.TenantID == "" || p.TenantID != r.TenantID || !p.Scopes[ReplayScope] ||
		r.Source == "" || r.FromOffset == "" || r.ToOffset == "" || r.PipelineVersion == "" {
		return ErrForbidden
	}
	return nil
}
