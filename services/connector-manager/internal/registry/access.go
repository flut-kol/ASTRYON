package registry

import "errors"

var ErrForbidden = errors.New("connector access forbidden")

type Principal struct {
	TenantID string
	Scopes   map[string]bool
}

const ManageScope = "astryon:connectors:manage"

// Authorize rejects cross-tenant connector access before probing a remote
// endpoint, preventing both IDOR and accidental credential use.
func Authorize(p Principal, c Connector) error {
	if p.TenantID == "" || p.TenantID != c.TenantID || !p.Scopes[ManageScope] {
		return ErrForbidden
	}
	return nil
}
