package replay

import "errors"

var ErrIdentity = errors.New("invalid replay identity")

// Identity is the idempotency domain for replayed business results. Storage
// must unique-key these fields before applying an aggregate mutation.
type Identity struct {
	TenantID, EventID, PipelineVersion string
}

func (i Identity) Validate() error {
	if i.TenantID == "" || i.EventID == "" || i.PipelineVersion == "" {
		return ErrIdentity
	}
	return nil
}

func SameBusinessResult(a, b Identity) bool {
	return a.TenantID == b.TenantID && a.EventID == b.EventID && a.PipelineVersion == b.PipelineVersion
}
