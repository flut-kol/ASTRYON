package replay

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// TopicAudit is the shared audit topic (infra/kafka/topics.yaml, libs/event-contracts TopicAudit).
const TopicAudit = "audit.events.v1"

// Audit actions and outcomes.
const (
	ActionPlan     = "replay.plan"     // dry run
	ActionStart    = "replay.started"  // version allocated; job about to be / was published
	ActionActivate = "replay.activate" // cutover
	ActionRollback = "replay.rollback"

	ActionAuthDenied = "auth.denied"

	// Phases of a two-phase audit (activation, start). Empty for single-shot events (plan, rejected).
	PhasePrepared  = "prepared"  // published BEFORE the state change (required mode); truth unknown until followed up
	PhaseCommitted = "committed" // the state change is durable (best-effort follow-up, or the only event in best_effort mode)
	PhaseAborted   = "aborted"   // the state change did not happen (COMMIT failed / job publish failed)

	OutcomeSuccess  = "success"
	OutcomeRejected = "rejected" // refused by a rule (404/409/400); nothing changed
	OutcomePending  = "pending"  // phase prepared
	OutcomeAborted  = "aborted"  // phase aborted
)

// AuditEvent is one audit record. It carries identifiers and versions only: never request payloads, event data or tokens.
type AuditEvent struct {
	EventID     string    `json:"event_id"` // deterministic per (tenant, replay, source, action, versions, request_id, outcome): retries do not double-audit
	Time        time.Time `json:"ts"`
	TenantID    string    `json:"tenant_id"`
	Subject     string    `json:"subject"`
	Action      string    `json:"action"`
	ReplayID    string    `json:"replay_id"` // activation: replay_requests.id; start: job id; plan: empty
	SourceID    string    `json:"source_id"`
	FromVersion *int32    `json:"from_version"`
	ToVersion   *int32    `json:"to_version"`
	Outcome     string    `json:"outcome"`
	Phase       string    `json:"phase,omitempty"` // prepared | committed | aborted (see Phase*)
	RequestID   string    `json:"request_id"`
}

// NewEventID derives the idempotent event id.
func NewEventID(e AuditEvent) string {
	f := func(v *int32) string {
		if v == nil {
			return "-"
		}
		return fmt.Sprint(*v)
	}
	h := sha256.Sum256([]byte(strings.Join([]string{e.TenantID, e.ReplayID, e.SourceID, e.Action, f(e.FromVersion), f(e.ToVersion), e.RequestID, e.Outcome, e.Phase}, "\x1f")))
	return hex.EncodeToString(h[:16])
}

// AuditSink receives audit events. Record returns nil only when the event is durable for that sink.
type AuditSink interface {
	Record(ctx context.Context, e AuditEvent) error
}

// MemAuditSink is the in-memory fake.
type MemAuditSink struct {
	mu     sync.Mutex
	Events []AuditEvent
	Denied []DeniedEvent
}

func (m *MemAuditSink) Record(_ context.Context, e AuditEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Events = append(m.Events, e)
	return nil
}

// Snapshot returns a copy of the recorded events.
func (m *MemAuditSink) Snapshot() []AuditEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]AuditEvent(nil), m.Events...)
}

// JSONAuditSink writes one JSON object per line (stdout in cmd/replay-controller when Kafka is not configured).
type JSONAuditSink struct {
	W  io.Writer
	mu sync.Mutex
}

func (s *JSONAuditSink) Record(_ context.Context, e AuditEvent) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.W.Write(append(b, '\n'))
	return err
}

// KafkaAuditSink publishes JSON events to audit.events.v1: acks=all, idempotent producer, key = tenant_id.
type KafkaAuditSink struct {
	p       kafkaProducer
	topic   string
	timeout time.Duration
}

// NewKafkaAuditSink uses the same TLS/SASL config as the job publisher. timeout bounds one Record (default 5s).
func NewKafkaAuditSink(c KafkaConfig, timeout time.Duration) (*KafkaAuditSink, error) {
	p, err := newFranzProducer(c)
	if err != nil {
		return nil, err
	}
	return newKafkaAuditSink(p, timeout), nil
}

func newKafkaAuditSink(p kafkaProducer, timeout time.Duration) *KafkaAuditSink {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &KafkaAuditSink{p: p, topic: TopicAudit, timeout: timeout}
}

func (k *KafkaAuditSink) Record(ctx context.Context, e AuditEvent) error {
	if e.TenantID == "" || e.Action == "" {
		return errors.New("audit event without tenant/action")
	}
	if e.EventID == "" {
		e.EventID = NewEventID(e)
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()
	if err := k.p.produce(ctx, k.topic, []byte(e.TenantID), b); err != nil {
		return fmt.Errorf("kafka audit publish to %s: %w", k.topic, err)
	}
	return nil
}

// Close releases the client.
func (k *KafkaAuditSink) Close() { k.p.close() }

// ParseAuditMode maps REPLAY_AUDIT_MODE to "activation requires a durable audit event". Unset: required when
// Kafka is configured, else best_effort. "required" without Kafka is an error (stdout is not a durable sink).
func ParseAuditMode(v string, kafkaConfigured bool) (required bool, err error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "":
		return kafkaConfigured, nil
	case "best_effort":
		return false, nil
	case "required":
		if !kafkaConfigured {
			return false, errors.New("REPLAY_AUDIT_MODE=required needs ASTRYON_KAFKA_BROKERS (stdout is not a durable audit sink)")
		}
		return true, nil
	}
	return false, fmt.Errorf("REPLAY_AUDIT_MODE must be required or best_effort, got %q", v)
}

// requestID returns the caller's X-Request-Id if it is short printable ASCII, else a fresh random id.
func requestID(h string) string {
	if n := len(h); n > 0 && n <= 128 {
		ok := true
		for _, c := range h {
			if c < 0x21 || c > 0x7e {
				ok = false
			}
		}
		if ok {
			return h
		}
	}
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
