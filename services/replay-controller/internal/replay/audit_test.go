package replay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// kafkaFake is a recording / failing kafkaProducer.
type kafkaFake struct {
	mu     sync.Mutex
	topics []string
	keys   []string
	vals   [][]byte
	err    error
}

func (f *kafkaFake) produce(_ context.Context, topic string, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.topics, f.keys, f.vals = append(f.topics, topic), append(f.keys, string(key)), append(f.vals, value)
	return nil
}
func (f *kafkaFake) close() {}
func (f *kafkaFake) n() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.vals) }

func auditFixture(required bool, kf *kafkaFake) (actFixture, struct{}) {
	f := newActFixture()
	sink := newKafkaAuditSink(kf, 0)
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Subject: "alice", Scopes: []string{ScopeReplayWrite}}}
	h := (&Handler{Auth: auth, Planner: &Planner{}, Activator: f.act, Audit: sink, AuditRequired: required}).Routes()
	f.srv = h
	return f, struct{}{}
}

func newPlanFixtureWithAudit(kf *kafkaFake) (http.Handler, struct{}) {
	pl := &Planner{Alloc: NewMemAllocator(), Counter: FixedCounter{N: 42}, Publisher: &MemPublisher{}}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Subject: "alice", Scopes: []string{ScopeReplayWrite}}}
	return (&Handler{Auth: auth, Planner: pl, Audit: newKafkaAuditSink(kf, 0)}).Routes(), struct{}{}
}

func TestKafkaAuditSinkTopicKeyBody(t *testing.T) {
	kf := &kafkaFake{}
	f, _ := auditFixture(true, kf)
	if c, _ := f.post(rp1, "", "tok"); c != 200 {
		t.Fatal(c)
	}
	if len(kf.vals) != 2 || kf.topics[0] != "audit.events.v1" || kf.keys[0] != tenantA || kf.keys[1] != tenantA {
		t.Fatalf("%v %v", kf.topics, kf.keys)
	}
	var m, pm map[string]any
	if err := json.Unmarshal(kf.vals[0], &pm); err != nil {
		t.Fatal(err)
	}
	if pm["phase"] != "prepared" || pm["outcome"] != "pending" {
		t.Fatalf("%s", kf.vals[0])
	}
	kf.vals = kf.vals[1:] // the committed follow-up carries the final truth
	if err := json.Unmarshal(kf.vals[0], &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"ts", "tenant_id", "subject", "action", "replay_id", "source_id", "from_version", "to_version", "outcome", "request_id", "event_id"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("missing %s in %s", k, kf.vals[0])
		}
	}
	if m["action"] != "replay.activate" || m["subject"] != "alice" || m["tenant_id"] != tenantA || m["outcome"] != "success" || m["phase"] != "committed" || m["to_version"] != float64(1) {
		t.Fatalf("%s", kf.vals[0])
	}
	if strings.Contains(string(kf.vals[0]), "tok") && strings.Contains(string(kf.vals[0]), "Bearer") {
		t.Fatal("token leaked")
	}
}

func TestRequiredAuditFailureRefusesActivationBeforeCommit(t *testing.T) {
	kf := &kafkaFake{err: errors.New("broker down")}
	f, _ := auditFixture(true, kf)
	c, m := f.post(rp1, "", "tok")
	if c != 503 || m["code"] != "audit_unavailable" {
		t.Fatalf("%d %v", c, m)
	}
	if _, _, ok := f.act.Active(tenantA, srcX); ok {
		t.Fatal("activation committed without audit")
	}
	// broker back: the same request now succeeds
	kf.err = nil
	if c, _ := f.post(rp1, "", "tok"); c != 200 {
		t.Fatal(c)
	}
	if a, _, ok := f.act.Active(tenantA, srcX); !ok || a != 1 {
		t.Fatal("not active after retry")
	}
}

func TestBestEffortAuditFailureStillActivates(t *testing.T) {
	kf := &kafkaFake{err: errors.New("broker down")}
	f, _ := auditFixture(false, kf)
	if c, _ := f.post(rp1, "", "tok"); c != 200 {
		t.Fatal(c)
	}
	if a, _, ok := f.act.Active(tenantA, srcX); !ok || a != 1 {
		t.Fatal("not active")
	}
}

func TestEventIDIdempotentForSameRequestID(t *testing.T) {
	kf := &kafkaFake{}
	f, _ := auditFixture(true, kf)
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("POST", "/v1/replays/"+rp1+"/activate", nil)
		r.Header.Set("Authorization", "Bearer tok")
		r.Header.Set("X-Request-Id", "req-1")
		f.srv.ServeHTTP(httptest.NewRecorder(), r)
	}
	// second call is a noop (nothing audited); a retry after a refused commit reuses the same id:
	var e1 AuditEvent
	_ = json.Unmarshal(kf.vals[0], &e1)
	e2 := e1
	e2.EventID = ""
	if e1.RequestID != "req-1" || NewEventID(e2) != e1.EventID {
		t.Fatalf("%+v", e1)
	}
	e2.RequestID = "req-2"
	if NewEventID(e2) == e1.EventID {
		t.Fatal("request id not part of the event id")
	}
}

func TestPlanAndStartAudited(t *testing.T) {
	kf := &kafkaFake{}
	h, _ := newPlanFixtureWithAudit(kf)
	for _, dry := range []string{"true", "false"} {
		body := `{"source_id":"` + srcX + `","from":"2026-10-01T00:00:00Z","to":"2026-10-02T00:00:00Z","pipeline_version":1,"dry_run":` + dry + `}`
		r := httptest.NewRequest("POST", "/v1/replays", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer tok")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 && w.Code != 202 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if len(kf.vals) != 2 {
		t.Fatalf("events %d", len(kf.vals))
	}
	var a, b AuditEvent
	_ = json.Unmarshal(kf.vals[0], &a)
	_ = json.Unmarshal(kf.vals[1], &b)
	if a.Action != ActionPlan || b.Action != "replay.started" || b.Phase != PhaseCommitted || b.ReplayID == "" || a.TenantID != tenantA || b.Subject != "alice" ||
		strings.Contains(string(kf.vals[1]), "pipeline_version") {
		t.Fatalf("%+v %+v", a, b)
	}
}

func TestParseAuditMode(t *testing.T) {
	cases := []struct {
		v     string
		kafka bool
		req   bool
		err   bool
	}{{"", true, true, false}, {"", false, false, false}, {"best_effort", true, false, false},
		{"required", true, true, false}, {"required", false, false, true}, {"bogus", true, false, true}}
	for _, c := range cases {
		got, err := ParseAuditMode(c.v, c.kafka)
		if got != c.req || (err != nil) != c.err {
			t.Fatalf("%+v got %v %v", c, got, err)
		}
	}
}

func TestKafkaAuditSinkRejectsEmptyTenant(t *testing.T) {
	if err := newKafkaAuditSink(&kafkaFake{}, 0).Record(context.Background(), AuditEvent{Action: "x"}); err == nil {
		t.Fatal("expected error")
	}
}
