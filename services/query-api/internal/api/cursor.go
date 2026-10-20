package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// ErrBadCursor is returned for any malformed, tampered, expired or
// mismatched cursor. Callers must not distinguish the reasons to clients.
var ErrBadCursor = errors.New("invalid cursor")

// cursorBody is the signed payload. It binds the cursor to a tenant, a
// resource kind and a hash of the query's filters, so a cursor cannot be
// replayed across tenants or against a different query.
type cursorBody struct {
	Kind   string `json:"k"`
	Tenant string `json:"t"`
	Hash   string `json:"h"`
	A      int64  `json:"a"` // keyset timestamp, unix micros
	ID     string `json:"i"` // keyset tie-breaker
	Exp    int64  `json:"e"` // unix seconds
}

// Cursors signs and verifies opaque pagination cursors with HMAC-SHA256.
type Cursors struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

func NewCursors(key []byte, ttl time.Duration) *Cursors {
	return &Cursors{key: key, ttl: ttl, now: time.Now}
}

func (c *Cursors) mac(b []byte) []byte {
	m := hmac.New(sha256.New, c.key)
	m.Write(b)
	return m.Sum(nil)
}

func (c *Cursors) Encode(kind, tenant, hash string, at time.Time, id string) string {
	b, _ := json.Marshal(cursorBody{Kind: kind, Tenant: tenant, Hash: hash, A: at.UnixMicro(), ID: id, Exp: c.now().Add(c.ttl).Unix()})
	enc := base64.RawURLEncoding
	return enc.EncodeToString(b) + "." + enc.EncodeToString(c.mac(b))
}

// Decode verifies signature, expiry, kind, tenant and filter hash.
func (c *Cursors) Decode(s, kind, tenant, hash string) (time.Time, string, error) {
	enc := base64.RawURLEncoding
	parts := strings.Split(s, ".")
	if len(parts) != 2 || len(s) > 1024 {
		return time.Time{}, "", ErrBadCursor
	}
	b, err1 := enc.DecodeString(parts[0])
	sig, err2 := enc.DecodeString(parts[1])
	if err1 != nil || err2 != nil || !hmac.Equal(sig, c.mac(b)) {
		return time.Time{}, "", ErrBadCursor
	}
	var body cursorBody
	if json.Unmarshal(b, &body) != nil {
		return time.Time{}, "", ErrBadCursor
	}
	if body.Kind != kind || body.Tenant != tenant || body.Hash != hash || c.now().Unix() > body.Exp {
		return time.Time{}, "", ErrBadCursor
	}
	return time.UnixMicro(body.A).UTC(), body.ID, nil
}

// hashOf returns a stable hash of v's JSON form (struct field order is fixed).
func hashOf(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return base64.RawURLEncoding.EncodeToString(h[:12])
}
