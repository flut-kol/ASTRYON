package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Denial reasons of auth.denied events.
const (
	ReasonUnauthenticated = "unauthenticated" // 401
	ReasonMissingScope    = "missing_scope"   // 403 (token verified)
)

// DeniedEvent is the audit record of a 401/403. It never carries a token or any header. Subject is set only when the
// token verified (403). RemoteIP is the peer address truncated to /24 (IPv4) or /48 (IPv6); X-Forwarded-For is NOT
// trusted (spoofable).
type DeniedEvent struct {
	EventID   string    `json:"event_id"`
	Time      time.Time `json:"ts"`
	Action    string    `json:"action"` // auth.denied
	RequestID string    `json:"request_id"`
	Path      string    `json:"path"`
	Method    string    `json:"method"`
	Reason    string    `json:"reason"`
	RemoteIP  string    `json:"remote_ip"`
	Subject   string    `json:"subject,omitempty"`
}

// DeniedSink is implemented by sinks that can record auth.denied events (Kafka key = truncated remote_ip).
type DeniedSink interface {
	RecordDenied(ctx context.Context, e DeniedEvent) error
}

func (m *MemAuditSink) RecordDenied(_ context.Context, e DeniedEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Denied = append(m.Denied, e)
	return nil
}

// DeniedSnapshot returns a copy of the recorded denial events.
func (m *MemAuditSink) DeniedSnapshot() []DeniedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DeniedEvent(nil), m.Denied...)
}

func (s *JSONAuditSink) RecordDenied(_ context.Context, e DeniedEvent) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.W.Write(append(b, '\n'))
	return err
}

func (k *KafkaAuditSink) RecordDenied(ctx context.Context, e DeniedEvent) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()
	if err := k.p.produce(ctx, k.topic, []byte(e.RemoteIP), b); err != nil {
		return fmt.Errorf("kafka audit publish to %s: %w", k.topic, err)
	}
	return nil
}

// truncIP returns the peer IP of r.RemoteAddr truncated to a network prefix ("203.0.113.0/24"); "unknown" if unparsable.
func truncIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "unknown"
	}
	if v4 := ip.To4(); v4 != nil {
		return (&net.IPNet{IP: v4.Mask(net.CIDRMask(24, 32)), Mask: net.CIDRMask(24, 32)}).String()
	}
	return (&net.IPNet{IP: ip.Mask(net.CIDRMask(48, 128)), Mask: net.CIDRMask(48, 128)}).String()
}

// denyLimiter allows at most one event per key per window; the map is bounded (expired entries are swept when full,
// and when still full new keys are dropped, so memory stays bounded under an address-spraying attacker).
type denyLimiter struct {
	mu     sync.Mutex
	last   map[string]time.Time
	window time.Duration
	max    int
	now    func() time.Time
}

func newDenyLimiter(window time.Duration, max int) *denyLimiter {
	return &denyLimiter{last: map[string]time.Time{}, window: window, max: max, now: time.Now}
}

func (l *denyLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if t, ok := l.last[key]; ok && now.Sub(t) < l.window {
		return false
	}
	if _, ok := l.last[key]; !ok && len(l.last) >= l.max {
		for k, t := range l.last {
			if now.Sub(t) >= l.window {
				delete(l.last, k)
			}
		}
		if len(l.last) >= l.max {
			return false
		}
	}
	l.last[key] = now
	return true
}

func safeStr(s string, n int) string {
	var b strings.Builder
	for _, c := range s {
		if b.Len() >= n {
			break
		}
		if c < 0x20 || c > 0x7e {
			c = '?'
		}
		b.WriteRune(c)
	}
	return b.String()
}

const (
	denyWindow    = 10 * time.Second
	denyMaxKeys   = 4096
	denyMaxFlight = 8
)

// auditDenied records an auth.denied event best effort: rate limited per (remote_ip, reason), published asynchronously
// with bounded concurrency so a slow broker never delays or blocks the 401/403 response.
func (h *Handler) auditDenied(r *http.Request, reason, subject string) {
	ds, ok := h.Audit.(DeniedSink)
	if !ok {
		return
	}
	h.denyOnce.Do(func() {
		h.denyLim = newDenyLimiter(denyWindow, denyMaxKeys)
		h.denySem = make(chan struct{}, denyMaxFlight)
	})
	ip := truncIP(r.RemoteAddr)
	if !h.denyLim.allow(ip + "|" + reason) {
		return
	}
	e := DeniedEvent{Time: time.Now().UTC(), Action: ActionAuthDenied, RequestID: requestID(r.Header.Get("X-Request-Id")),
		Path: safeStr(r.URL.Path, 128), Method: safeStr(r.Method, 16), Reason: reason, RemoteIP: ip, Subject: safeStr(subject, 128)}
	hh := sha256.Sum256([]byte(strings.Join([]string{ActionAuthDenied, e.RequestID, e.Reason, e.RemoteIP, e.Path, e.Method}, "\x1f")))
	e.EventID = hex.EncodeToString(hh[:16])
	select {
	case h.denySem <- struct{}{}:
	default:
		return
	}
	go func() {
		defer func() { <-h.denySem }()
		if err := ds.RecordDenied(context.Background(), e); err != nil {
			log.Printf("replay audit: auth.denied not recorded: %v", err)
		}
	}()
}
