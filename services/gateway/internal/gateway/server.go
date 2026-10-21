// Package gateway implements the ASTRYON public ingestion API
// (DEVELOPMENT_PLAN §8: POST /v1/events, POST /v1/events/batch).
package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	ec "github.com/astryon/event-contracts"
)

const (
	maxIdemKeyLen    = 128
	maxRawQuarantine = 64 << 10
	// ErrEnvelopeMalformed is a gateway-local diagnostic for undecodable items.
	ErrEnvelopeMalformed ec.Code = "ERR_ENVELOPE_MALFORMED"
)

// Server is the HTTP gateway.
type Server struct {
	cfg  Config
	auth Authenticator
	pub  Publisher
	now  func() time.Time
	m    metrics
}

func NewServer(cfg Config, a Authenticator, p Publisher) *Server {
	return &Server{cfg: cfg, auth: a, pub: p, now: time.Now}
}

// Handler returns the routed handler with request-ID middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /metrics", s.m.handler)
	mux.HandleFunc("POST /v1/events", s.m.count(s.handleSingle))
	mux.HandleFunc("POST /v1/events/batch", s.m.count(s.handleBatch))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 || !utf8.ValidString(id) || hasCtl(id) {
			id = newUUID()
		}
		w.Header().Set("X-Request-ID", id)
		mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), reqIDKey, id)))
	})
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
	TraceID string `json:"trace_id"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, r *http.Request, status int, code, msg string, details any) {
	writeJSON(w, status, apiError{code, msg, details, requestID(r.Context())})
}

// prelude: authn, scope, idempotency key. ok=false means response written.
func (s *Server) prelude(w http.ResponseWriter, r *http.Request) (Principal, string, bool) {
	p, err := s.auth.Authenticate(r)
	if err != nil {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeErr(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required", nil)
		return p, "", false
	}
	if !ec.IsUUID(p.TenantID) || !p.HasScope(ScopeEventsWrite) {
		writeErr(w, r, http.StatusForbidden, "FORBIDDEN", "missing required scope "+ScopeEventsWrite, nil)
		return p, "", false
	}
	key := r.Header.Get("Idempotency-Key")
	if key != "" && (len(key) > maxIdemKeyLen || !utf8.ValidString(key) || hasCtl(key)) {
		writeErr(w, r, http.StatusBadRequest, "IDEMPOTENCY_KEY_INVALID", "Idempotency-Key must be printable, max 128 bytes", nil)
		return p, "", false
	}
	return p, key, true
}

func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, bool) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErr(w, r, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", fmt.Sprintf("body exceeds %d bytes", limit), nil)
		} else {
			writeErr(w, r, http.StatusBadRequest, "BODY_READ_ERROR", "cannot read body", nil)
		}
		return nil, false
	}
	return b, true
}

// item is one parsed event submission.
type item struct {
	raw      json.RawMessage
	env      ec.Envelope
	parseErr error
}

func parseItem(raw json.RawMessage) item {
	it := item{raw: raw}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&it.env); err != nil {
		it.parseErr = err
	} else if dec.More() {
		it.parseErr = errors.New("trailing data")
	}
	return it
}

// ItemResult is the per-event outcome in responses.
type ItemResult struct {
	Index   int      `json:"index"`
	EventID string   `json:"event_id,omitempty"`
	Status  string   `json:"status"` // accepted | quarantined | failed
	Code    string   `json:"code,omitempty"`
	Message string   `json:"message,omitempty"`
	Codes   []string `json:"codes,omitempty"`
}

func (s *Server) handleSingle(w http.ResponseWriter, r *http.Request) {
	p, idem, ok := s.prelude(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r, s.cfg.MaxBodyBytes)
	if !ok {
		return
	}
	it := parseItem(body)
	if it.parseErr == nil && it.env.TenantID != "" && it.env.TenantID != p.TenantID {
		writeErr(w, r, http.StatusForbidden, "TENANT_MISMATCH", "tenant_id in body does not match authenticated tenant", nil)
		return
	}
	res := s.process(r.Context(), p, it, idem, 0)
	switch res.Status {
	case "accepted":
		writeJSON(w, http.StatusAccepted, res)
	case "quarantined":
		writeErr(w, r, http.StatusUnprocessableEntity, "ENVELOPE_INVALID", "event failed validation and was quarantined", res)
	default:
		w.Header().Set("Retry-After", "1")
		writeErr(w, r, http.StatusServiceUnavailable, "PUBLISH_FAILED", "event was not durably accepted; retry with the same Idempotency-Key", nil)
	}
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	p, idem, ok := s.prelude(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r, s.cfg.MaxBatchBytes)
	if !ok {
		return
	}
	var req struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, r, http.StatusBadRequest, "BODY_INVALID", `body must be {"events":[...]}`, nil)
		return
	}
	if len(req.Events) == 0 {
		writeErr(w, r, http.StatusBadRequest, "BATCH_EMPTY", "events must not be empty", nil)
		return
	}
	if len(req.Events) > s.cfg.MaxBatchEvents {
		writeErr(w, r, http.StatusRequestEntityTooLarge, "BATCH_TOO_LARGE", fmt.Sprintf("at most %d events per batch", s.cfg.MaxBatchEvents), nil)
		return
	}
	items := make([]item, len(req.Events))
	for i, raw := range req.Events {
		items[i] = parseItem(raw)
		if items[i].parseErr == nil && items[i].env.TenantID != "" && items[i].env.TenantID != p.TenantID {
			writeErr(w, r, http.StatusForbidden, "TENANT_MISMATCH", "tenant_id in body does not match authenticated tenant", map[string]int{"index": i})
			return
		}
	}
	results := make([]ItemResult, len(items))
	counts := map[string]int{}
	for i, it := range items {
		results[i] = s.process(r.Context(), p, it, idem, i)
		counts[results[i].Status]++
	}
	status := http.StatusOK
	switch {
	case counts["failed"] == len(items):
		w.Header().Set("Retry-After", "1")
		status = http.StatusServiceUnavailable
	case counts["accepted"] != len(items):
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, map[string]any{
		"accepted": counts["accepted"], "quarantined": counts["quarantined"], "failed": counts["failed"],
		"results": results, "trace_id": requestID(r.Context()),
	})
}

// process validates and publishes one event. "accepted" is returned only
// after the publisher acked.
func (s *Server) process(ctx context.Context, p Principal, it item, idem string, idx int) ItemResult {
	res := ItemResult{Index: idx}
	reqID := requestID(ctx)
	if it.parseErr != nil {
		return s.quarantine(ctx, p, it, idx, res, []ec.Code{ErrEnvelopeMalformed}, it.parseErr.Error())
	}
	env := it.env
	env.TenantID = p.TenantID // always from the principal
	env.IngestedAt = s.now().UTC()
	if env.TraceID == "" {
		env.TraceID = reqID
	}
	if env.ProcessingVersion == 0 {
		env.ProcessingVersion = 1
	}
	if env.EventID == "" && idem != "" {
		env.EventID = deriveEventID(p.TenantID, env.SourceID, idem, idx)
	} else if env.EventID == "" {
		env.EventID = newUUID()
	}
	if id, ok := ec.CanonicalID(env.EventID); ok {
		env.EventID = id
	}
	res.EventID = env.EventID
	if err := env.ValidateAt(s.now()); err != nil {
		var codes []ec.Code
		var ve ec.Errors
		if errors.As(err, &ve) {
			codes = ve.Codes()
		}
		return s.quarantine(ctx, p, it, idx, res, codes, err.Error())
	}
	val, err := json.Marshal(env)
	if err != nil {
		res.Status, res.Code, res.Message = "failed", "INTERNAL", "encode failed"
		return res
	}
	if err := s.pub.Publish(ctx, ec.TopicRawEvents, env.PartitionKey(), val); err != nil {
		res.Status, res.Code, res.Message = "failed", "PUBLISH_FAILED", "not durably accepted"
		return res
	}
	res.Status = "accepted"
	return res
}

func (s *Server) quarantine(ctx context.Context, p Principal, it item, idx int, res ItemResult, codes []ec.Code, detail string) ItemResult {
	strCodes := make([]string, len(codes))
	for i, c := range codes {
		strCodes[i] = string(c)
	}
	rec := map[string]any{
		"tenant_id": p.TenantID, "request_id": requestID(ctx), "index": idx,
		"received_at": s.now().UTC(), "codes": strCodes, "detail": detail,
		"source_topic": ec.TopicRawEvents,
	}
	raw := it.raw
	if len(raw) > maxRawQuarantine {
		raw = raw[:maxRawQuarantine]
		rec["raw_truncated"] = true
	}
	rec["raw_b64"] = base64.StdEncoding.EncodeToString(raw)
	val, _ := json.Marshal(rec)
	key := ec.PartitionKey(p.TenantID, fmt.Sprintf("%s/%d", requestID(ctx), idx))
	res.Codes = strCodes
	if err := s.pub.Publish(ctx, ec.TopicQuarantine, key, val); err != nil {
		res.Status, res.Code, res.Message = "failed", "PUBLISH_FAILED", "quarantine not durably recorded"
		return res
	}
	res.Status, res.Code, res.Message = "quarantined", "ENVELOPE_INVALID", "validation failed"
	return res
}

// deriveEventID makes event_id a pure function of (tenant, source, key, index)
// so retries with the same Idempotency-Key never mint a new event_id (§5.3).
func deriveEventID(tenant, source, key string, idx int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("astryon/event-id/v1\x00%s\x00%s\x00%s\x00%d", tenant, source, key, idx)))
	b := h[:16]
	b[6] = b[6]&0x0f | 0x50
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func hasCtl(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}
