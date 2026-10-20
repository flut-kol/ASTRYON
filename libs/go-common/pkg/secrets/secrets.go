// Package secrets resolves secret references (env:NAME, file:/path,
// vault:mount/path#key) into values. Values are wrapped in Secret, which
// redacts itself in String, fmt, JSON and slog output; errors never contain
// values or upstream response bodies.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// ErrNotFound is returned when a reference resolves to nothing.
var ErrNotFound = errors.New("secrets: not found")

const redacted = "[REDACTED]"

// Secret holds a sensitive value. Use Reveal to read it deliberately.
type Secret struct{ v string }

// New wraps a value.
func New(v string) Secret { return Secret{v: v} }

// Reveal returns the raw value.
func (s Secret) Reveal() string { return s.v }

// Empty reports whether the secret has no value.
func (s Secret) Empty() bool { return s.v == "" }

func (Secret) String() string               { return redacted }
func (Secret) GoString() string             { return redacted }
func (Secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }
func (Secret) LogValue() slog.Value         { return slog.StringValue(redacted) }
func (s Secret) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(redacted)) }

// Provider fetches a secret. ref is the provider-specific part of a reference
// (without the scheme prefix).
type Provider interface {
	Get(ctx context.Context, ref string) (Secret, error)
}

// Resolver dispatches full references ("scheme:ref") to providers.
type Resolver struct{ providers map[string]Provider }

// NewResolver builds a Resolver from scheme -> Provider.
func NewResolver(p map[string]Provider) *Resolver {
	m := make(map[string]Provider, len(p))
	for k, v := range p {
		m[k] = v
	}
	return &Resolver{providers: m}
}

// DefaultResolver supports env: and file: (plus vault: when v is non-nil).
func DefaultResolver(v Provider) *Resolver {
	m := map[string]Provider{"env": EnvProvider{}, "file": NewFileProvider(nil)}
	if v != nil {
		m["vault"] = v
	}
	return NewResolver(m)
}

// Resolve resolves a full reference such as "env:PG_PASSWORD".
func (r *Resolver) Resolve(ctx context.Context, ref string) (Secret, error) {
	scheme, rest, ok := strings.Cut(ref, ":")
	if !ok || rest == "" {
		return Secret{}, fmt.Errorf("secrets: invalid reference %q (want scheme:ref)", ref)
	}
	p, ok := r.providers[scheme]
	if !ok {
		return Secret{}, fmt.Errorf("secrets: unknown scheme %q", scheme)
	}
	return p.Get(ctx, rest)
}

// Resolve is a convenience wrapper over r.Resolve.
func Resolve(ctx context.Context, r *Resolver, ref string) (Secret, error) {
	return r.Resolve(ctx, ref)
}
