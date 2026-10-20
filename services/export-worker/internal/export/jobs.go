package export

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Job statuses. They describe one export job, not events.
const (
	StatusQueued    = "QUEUED"
	StatusRunning   = "RUNNING"
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
)

// Job is one export job. The download URL is never stored; it is signed on request.
type Job struct {
	ID         string
	TenantID   string
	Subject    string
	Spec       ExportSpec
	Status     string
	Rows       int64
	Bytes      int64
	ObjectKey  string
	ErrorCode  string
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	Attempts   int // lost claims so far (PG queue); 0 for in-memory jobs
}

func (j Job) Terminal() bool { return j.Status == StatusSucceeded || j.Status == StatusFailed }

var ErrJobNotFound = errors.New("export job not found")

// JobStore persists jobs. Every access is scoped by tenant: another tenant's id is "not found".
// PGJobStore (pg.go) is the durable, replica-safe implementation.
type JobStore interface {
	Create(j Job) error
	Get(tenantID, id string) (Job, error)
	// Update applies fn atomically; fn must not change a terminal job (the server guards that).
	Update(tenantID, id string, fn func(*Job)) (Job, error)
	// FailUnfinished marks every QUEUED/RUNNING job FAILED with code and returns how many.
	FailUnfinished(code string, at time.Time) int
}

// ReservingCreator is implemented by a durable QuotaChecker (PGQuota): it checks the limits and creates the job
// atomically, returning ErrQuotaExceeded / ErrTooManyConcurrent. The Server then skips Acquire + Jobs.Create.
type ReservingCreator interface {
	CreateReserved(j Job) error
}

// MemoryJobStore is the in-memory JobStore.
type MemoryJobStore struct {
	mu   sync.Mutex
	jobs map[string]Job // key tenant/id
}

func jkey(t, id string) string { return t + "/" + id }

func (m *MemoryJobStore) Create(j Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.jobs == nil {
		m.jobs = map[string]Job{}
	}
	k := jkey(j.TenantID, j.ID)
	if _, ok := m.jobs[k]; ok {
		return errors.New("job exists")
	}
	m.jobs[k] = j
	return nil
}

func (m *MemoryJobStore) Get(tenantID, id string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jkey(tenantID, id)]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	return j, nil
}

func (m *MemoryJobStore) Update(tenantID, id string, fn func(*Job)) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := jkey(tenantID, id)
	j, ok := m.jobs[k]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	fn(&j)
	j.ID, j.TenantID = id, tenantID
	m.jobs[k] = j
	return j, nil
}

func (m *MemoryJobStore) FailUnfinished(code string, at time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k, j := range m.jobs {
		if !j.Terminal() {
			j.Status, j.ErrorCode, j.FinishedAt = StatusFailed, code, at
			m.jobs[k] = j
			n++
		}
	}
	return n
}

// NewExportID returns a random UUID v4 (matches exportIDRe).
func NewExportID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
