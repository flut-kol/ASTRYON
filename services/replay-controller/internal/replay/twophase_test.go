package replay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func decodeEvents(t *testing.T, kf *kafkaFake) []AuditEvent {
	t.Helper()
	var out []AuditEvent
	for _, v := range kf.vals {
		var e AuditEvent
		if err := json.Unmarshal(v, &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func TestCommitFailureEmitsPreparedThenAbortedNeverCommitted(t *testing.T) {
	kf := &kafkaFake{}
	f, _ := auditFixture(true, kf)
	f.act.CommitErr = errors.New("serialization failure")
	c, _ := f.post(rp1, "", "tok")
	if c != 502 {
		t.Fatal(c)
	}
	if _, _, ok := f.act.Active(tenantA, srcX); ok {
		t.Fatal("state changed")
	}
	ev := decodeEvents(t, kf)
	if len(ev) != 2 || ev[0].Phase != PhasePrepared || ev[1].Phase != PhaseAborted || ev[1].Outcome != OutcomeAborted ||
		ev[0].EventID == ev[1].EventID || ev[0].RequestID != ev[1].RequestID {
		t.Fatalf("%+v", ev)
	}
	for _, e := range ev {
		if e.Phase == PhaseCommitted {
			t.Fatal("committed event for a failed commit")
		}
	}
}

func TestBestEffortActivationEmitsOnlyCommitted(t *testing.T) {
	kf := &kafkaFake{}
	f, _ := auditFixture(false, kf)
	if c, _ := f.post(rp1, "", "tok"); c != 200 {
		t.Fatal(c)
	}
	ev := decodeEvents(t, kf)
	if len(ev) != 1 || ev[0].Phase != PhaseCommitted {
		t.Fatalf("%+v", ev)
	}
	// best effort + failing commit: nothing is claimed
	kf2 := &kafkaFake{}
	f2, _ := auditFixture(false, kf2)
	f2.act.CommitErr = errors.New("x")
	if c, _ := f2.post(rp1, "", "tok"); c != 502 || len(kf2.vals) != 0 {
		t.Fatalf("%d %d", c, len(kf2.vals))
	}
}

// orderPub records how many audit events existed when the job was published.
type orderPub struct {
	kf     *kafkaFake
	seen   int
	err    error
	called int
}

func (p *orderPub) Publish(context.Context, ReplayJob) error {
	p.called++
	p.seen = len(p.kf.vals)
	return p.err
}

func startReq(h http.Handler) *httptest.ResponseRecorder {
	body := `{"source_id":"` + srcX + `","from":"2026-10-01T00:00:00Z","to":"2026-10-02T00:00:00Z","pipeline_version":1}`
	r := httptest.NewRequest("POST", "/v1/replays", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func startHandler(kf *kafkaFake, pub JobPublisher, required bool) http.Handler {
	pl := &Planner{Alloc: NewMemAllocator(), Counter: FixedCounter{N: 1}, Publisher: pub}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Subject: "alice", Scopes: []string{ScopeReplayWrite}}}
	return (&Handler{Auth: auth, Planner: pl, Audit: newKafkaAuditSink(kf, 0), AuditRequired: required}).Routes()
}

func TestRequiredStartAuditedBeforeJobPublish(t *testing.T) {
	kf := &kafkaFake{}
	pub := &orderPub{kf: kf}
	if w := startReq(startHandler(kf, pub, true)); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	ev := decodeEvents(t, kf)
	if pub.seen != 1 || len(ev) != 1 || ev[0].Action != "replay.started" || ev[0].Phase != PhasePrepared || ev[0].ReplayID == "" {
		t.Fatalf("seen=%d %+v", pub.seen, ev)
	}
}

func TestRequiredStartAuditFailurePublishesNothing(t *testing.T) {
	kf := &kafkaFake{err: errors.New("broker down")}
	pub := &MemPublisher{}
	if w := startReq(startHandler(kf, pub, true)); w.Code != 503 || !strings.Contains(w.Body.String(), "audit_unavailable") {
		t.Fatal(w.Code, w.Body.String())
	}
	if pub.Count() != 0 {
		t.Fatal("job published without audit")
	}
}

func TestRequiredStartJobPublishFailureAddsAborted(t *testing.T) {
	kf := &kafkaFake{}
	pub := &orderPub{kf: kf, err: errors.New("jobs topic down")}
	if w := startReq(startHandler(kf, pub, true)); w.Code != 502 {
		t.Fatal(w.Code)
	}
	ev := decodeEvents(t, kf)
	if len(ev) != 2 || ev[0].Phase != PhasePrepared || ev[1].Phase != PhaseAborted {
		t.Fatalf("%+v", ev)
	}
}

func TestBestEffortStartAfterPublish(t *testing.T) {
	kf := &kafkaFake{}
	pub := &orderPub{kf: kf}
	if w := startReq(startHandler(kf, pub, false)); w.Code != 202 {
		t.Fatal(w.Code)
	}
	ev := decodeEvents(t, kf)
	if pub.seen != 0 || len(ev) != 1 || ev[0].Phase != PhaseCommitted {
		t.Fatalf("seen=%d %+v", pub.seen, ev)
	}
}

// ---- auth.denied ----

func deniedFixture() (*Handler, *MemAuditSink) {
	sink := &MemAuditSink{}
	auth := DevStaticAuthenticator{Token: "secret-token-value", Principal: Principal{TenantID: tenantA, Subject: "alice", Scopes: []string{"astryon:query:read"}}}
	return &Handler{Auth: auth, Planner: &Planner{}, Audit: sink}, sink
}

func denyReq(h http.Handler, remote, tok string) int {
	r := httptest.NewRequest("POST", "/v1/replays/x/activate?secret=1", nil)
	r.RemoteAddr = remote
	r.Header.Set("Cookie", "sid=abc")
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code
}

func waitDenied(t *testing.T, s *MemAuditSink, n int) []DeniedEvent {
	t.Helper()
	for i := 0; i < 200; i++ {
		if d := s.DeniedSnapshot(); len(d) >= n {
			return d
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("want %d denied events, have %d", n, len(s.DeniedSnapshot()))
	return nil
}

func TestAuthDeniedAuditedWithoutSecrets(t *testing.T) {
	h, sink := deniedFixture()
	routes := h.Routes()
	if c := denyReq(routes, "203.0.113.77:5555", ""); c != 401 {
		t.Fatal(c)
	}
	if c := denyReq(routes, "198.51.100.9:1", "secret-token-value"); c != 403 {
		t.Fatal(c)
	}
	d := waitDenied(t, sink, 2)
	by := map[string]DeniedEvent{}
	for _, e := range d {
		by[e.Reason] = e
	}
	u, m := by[ReasonUnauthenticated], by[ReasonMissingScope]
	if u.RemoteIP != "203.0.113.0/24" || u.Subject != "" || u.Action != "auth.denied" || u.Method != "POST" ||
		u.Path != "/v1/replays/x/activate" || u.RequestID == "" || u.EventID == "" || u.Time.IsZero() {
		t.Fatalf("%+v", u)
	}
	if m.Subject != "alice" || m.RemoteIP != "198.51.100.0/24" {
		t.Fatalf("%+v", m)
	}
	b, _ := json.Marshal(d)
	for _, bad := range []string{"secret-token-value", "Bearer", "sid=abc", "secret=1", "tenant"} {
		if strings.Contains(string(b), bad) {
			t.Fatalf("%q leaked: %s", bad, b)
		}
	}
	if len(sink.Snapshot()) != 0 {
		t.Fatal("denial recorded as a normal audit event")
	}
}

func TestAuthDeniedRateLimited(t *testing.T) {
	h, sink := deniedFixture()
	routes := h.Routes()
	for i := 0; i < 50; i++ {
		denyReq(routes, "203.0.113.1:1", "") // same /24 -> same key
		denyReq(routes, "203.0.113.200:1", "")
	}
	waitDenied(t, sink, 1)
	time.Sleep(100 * time.Millisecond)
	if n := len(sink.DeniedSnapshot()); n != 1 {
		t.Fatalf("flood produced %d events", n)
	}
	denyReq(routes, "203.0.114.1:1", "") // other network passes
	waitDenied(t, sink, 2)
}

func TestDenyLimiterWindowAndBound(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newDenyLimiter(10*time.Second, 3)
	l.now = func() time.Time { return now }
	if !l.allow("a") || l.allow("a") {
		t.Fatal("window")
	}
	l.allow("b")
	l.allow("c")
	if l.allow("d") || len(l.last) != 3 {
		t.Fatal("map not bounded")
	}
	now = now.Add(11 * time.Second)
	if !l.allow("a") || !l.allow("d") || len(l.last) > 3 {
		t.Fatal("expired entries not swept")
	}
}

func TestTruncIP(t *testing.T) {
	for in, want := range map[string]string{"203.0.113.77:80": "203.0.113.0/24", "[2001:db8:1:2::5]:80": "2001:db8:1::/48", "garbage": "unknown", "10.1.2.3": "10.1.2.0/24"} {
		if got := truncIP(in); got != want {
			t.Fatalf("%s: %s want %s", in, got, want)
		}
	}
}

func TestAuthDeniedSlowSinkDoesNotBlock(t *testing.T) {
	kf := &kafkaFake{}
	h, _ := deniedFixture()
	h.Audit = newKafkaAuditSink(kf, 0)
	routes := h.Routes()
	if c := denyReq(routes, "203.0.113.1:1", ""); c != 401 {
		t.Fatal(c)
	}
	for i := 0; i < 200 && kf.n() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if kf.n() != 1 || kf.keys[0] != "203.0.113.0/24" || kf.topics[0] != TopicAudit {
		t.Fatalf("%v %v", kf.keys, kf.topics)
	}
}
