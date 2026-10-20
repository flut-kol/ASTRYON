package export

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

const (
	OutcomeSuccess       = "success"
	OutcomeFailed        = "failed"
	OutcomeInvalid       = "invalid_spec"
	OutcomeQuotaExceeded = "quota_exceeded"
	OutcomeRowCap        = "row_cap_exceeded"
	OutcomeByteCap       = "byte_cap_exceeded"
)

// AuditRecord is one audit entry per export attempt (success or failure).
type AuditRecord struct {
	Time     time.Time `json:"time"`
	TenantID string    `json:"tenant"`
	Subject  string    `json:"subject,omitempty"`
	ExportID string    `json:"export_id"`
	SpecHash string    `json:"spec_hash"`
	Rows     int64     `json:"rows"`
	Bytes    int64     `json:"bytes"`
	Outcome  string    `json:"outcome"`
	Error    string    `json:"error,omitempty"`
}

type AuditSink interface{ Emit(AuditRecord) error }

// MemoryAudit is an in-memory fake.
type MemoryAudit struct {
	mu   sync.Mutex
	Recs []AuditRecord
}

func (m *MemoryAudit) Emit(r AuditRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Recs = append(m.Recs, r)
	return nil
}

func (m *MemoryAudit) Records() []AuditRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]AuditRecord(nil), m.Recs...)
}

// JSONLinesAudit writes one JSON object per line.
type JSONLinesAudit struct {
	mu sync.Mutex
	W  io.Writer
}

func (j *JSONLinesAudit) Emit(r AuditRecord) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	_, err = j.W.Write(append(b, '\n'))
	return err
}
