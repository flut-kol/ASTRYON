package registry

import (
	"context"
	"errors"
	"strings"
)

var ErrInvalid = errors.New("invalid connector configuration")

type Kind string

const (
	PostgresCDC Kind = "POSTGRES_CDC"
	MySQLCDC    Kind = "MYSQL_CDC"
	RESTPull    Kind = "REST_PULL"
)

type Connector struct {
	ID, TenantID, Name, SecretRef string
	Kind                          Kind
	Config                        map[string]string
}

// Validate receives tenant ID from trusted auth middleware, never connector input.
func (c Connector) Validate(trustedTenant string) error {
	if trustedTenant == "" || c.TenantID != trustedTenant || c.ID == "" || c.Name == "" ||
		!strings.Contains(c.SecretRef, "://") || c.Config == nil {
		return ErrInvalid
	}
	switch c.Kind {
	case PostgresCDC, MySQLCDC, RESTPull:
		return nil
	default:
		return ErrInvalid
	}
}

type Prober interface {
	Probe(context.Context, Connector) error
}
