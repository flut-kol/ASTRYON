package bff

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrFull is returned by a store that refuses a new entry because its capacity is exhausted.
var ErrFull = errors.New("bff: store is full")

// Pending is the state of a login in progress (single use).
type Pending struct {
	Verifier, Nonce string
	Exp             time.Time
}

// Record is one server-side session. The browser only ever sees the opaque id it is stored under.
type Record struct {
	Tok     Tokens
	Sub     string
	SID     string   // AAS session id (ID token "sid"), the key of OIDC back-channel logout
	Tenant  string   // tenant_id from the validated ID token
	Client  string   // client_id/azp from the validated ID token
	ACR     string   // authentication context from the validated ID token
	AMR     []string // authentication methods from the validated ID token; retained for future AAS-defined step-up policy
	Created time.Time
}

// Store keeps login state, sessions and back-channel logout replay marks. The in-memory implementation is the
// default (one replica); the Redis implementation lets several replicas share state.
type Store interface {
	SavePending(ctx context.Context, state string, p Pending, ttl time.Duration) error
	TakePending(ctx context.Context, state string) (Pending, bool, error) // atomic: a state is returned once
	CreateSession(ctx context.Context, id string, r Record, ttl time.Duration) error
	GetSession(ctx context.Context, id string) (Record, bool, error)
	UpdateSession(ctx context.Context, id string, r Record, ttl time.Duration) error
	DeleteSession(ctx context.Context, id string) error
	DeleteBySID(ctx context.Context, sid string) ([]Record, error)
	DeleteBySub(ctx context.Context, sub string) ([]Record, error)
	SeenJTI(ctx context.Context, jti string, ttl time.Duration) (first bool, err error)
}

type memStore struct {
	mu       sync.Mutex
	now      func() time.Time
	max      int
	pend     map[string]Pending
	sessions map[string]Record
	jti      map[string]time.Time
}

// NewMemoryStore returns the process-local store; max bounds sessions and logins in progress.
func NewMemoryStore(max int, now func() time.Time) Store {
	if now == nil {
		now = time.Now
	}
	if max <= 0 {
		max = 10000
	}
	return &memStore{now: now, max: max, pend: map[string]Pending{}, sessions: map[string]Record{}, jti: map[string]time.Time{}}
}

func (m *memStore) SavePending(_ context.Context, state string, p Pending, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for k, v := range m.pend { // bounded: drop expired, refuse when still full
		if now.After(v.Exp) {
			delete(m.pend, k)
		}
	}
	if len(m.pend) >= m.max {
		return ErrFull
	}
	m.pend[state] = p
	return nil
}

func (m *memStore) TakePending(_ context.Context, state string) (Pending, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.pend[state]
	delete(m.pend, state)
	return p, ok, nil
}

func (m *memStore) CreateSession(_ context.Context, id string, r Record, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sessions) >= m.max {
		var oldest string
		var t time.Time
		for k, v := range m.sessions {
			if oldest == "" || v.Created.Before(t) {
				oldest, t = k, v.Created
			}
		}
		delete(m.sessions, oldest)
	}
	m.sessions[id] = r
	return nil
}

func (m *memStore) GetSession(_ context.Context, id string) (Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.sessions[id]
	return r, ok, nil
}

func (m *memStore) UpdateSession(_ context.Context, id string, r Record, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[id]; ok { // never resurrect a session that was ended meanwhile
		m.sessions[id] = r
	}
	return nil
}

func (m *memStore) DeleteSession(_ context.Context, id string) error {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
	return nil
}

func (m *memStore) deleteWhere(f func(Record) bool) []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	var deleted []Record
	for k, v := range m.sessions {
		if f(v) {
			delete(m.sessions, k)
			deleted = append(deleted, v)
		}
	}
	return deleted
}

func (m *memStore) DeleteBySID(_ context.Context, sid string) ([]Record, error) {
	return m.deleteWhere(func(r Record) bool { return r.SID == sid }), nil
}

func (m *memStore) DeleteBySub(_ context.Context, sub string) ([]Record, error) {
	return m.deleteWhere(func(r Record) bool { return r.Sub == sub }), nil
}

func (m *memStore) SeenJTI(_ context.Context, jti string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for k, exp := range m.jti {
		if now.After(exp) {
			delete(m.jti, k)
		}
	}
	if _, dup := m.jti[jti]; dup {
		return false, nil
	}
	if len(m.jti) >= 10*m.max {
		return false, ErrFull
	}
	m.jti[jti] = now.Add(ttl)
	return true, nil
}
