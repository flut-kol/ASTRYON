package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

const (
	rp1 = "00000000-0000-0000-0000-0000000000c1" // v1 COMPLETED
	rp2 = "00000000-0000-0000-0000-0000000000c2" // v2 COMPLETED
	rp3 = "00000000-0000-0000-0000-0000000000c3" // v3 RUNNING
	rpB = "00000000-0000-0000-0000-0000000000cb" // tenant B, v5 COMPLETED
)

type actFixture struct {
	act   *MemActivator
	audit *MemAuditSink
	srv   http.Handler
}

func newActFixture() actFixture {
	f := actFixture{audit: &MemAuditSink{}}
	f.act = &MemActivator{Replays: map[string]MemReplay{
		rp1: {tenantA, srcX, "COMPLETED", 1},
		rp2: {tenantA, srcX, "COMPLETED", 2},
		rp3: {tenantA, srcX, "RUNNING", 3},
		rpB: {tenantB, srcX, "COMPLETED", 5},
	}}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Subject: "alice", Scopes: []string{ScopeReplayWrite}}}
	f.srv = (&Handler{Auth: auth, Planner: &Planner{}, Activator: f.act, Audit: f.audit}).Routes()
	return f
}

func (f actFixture) post(id, query, tok string) (int, map[string]any) {
	r := httptest.NewRequest("POST", "/v1/replays/"+id+"/activate"+query, nil)
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	f.srv.ServeHTTP(w, r)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m
}

func TestActivateAuth(t *testing.T) {
	f := newActFixture()
	if c, _ := f.post(rp1, "", ""); c != 401 {
		t.Fatal(c)
	}
	noScope := DevStaticAuthenticator{Token: "t", Principal: Principal{TenantID: tenantA, Scopes: []string{"astryon:query:read"}}}
	h := (&Handler{Auth: noScope, Planner: &Planner{}, Activator: f.act}).Routes()
	r := httptest.NewRequest("POST", "/v1/replays/"+rp1+"/activate", nil)
	r.Header.Set("Authorization", "Bearer t")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	if _, _, ok := f.act.Active(tenantA, srcX); ok {
		t.Fatal("state changed without auth")
	}
}

func TestActivateRefusesNonCompleted(t *testing.T) {
	f := newActFixture()
	c, m := f.post(rp3, "", "tok")
	if c != 409 || m["code"] != "replay_not_completed" {
		t.Fatalf("%d %v", c, m)
	}
	if _, _, ok := f.act.Active(tenantA, srcX); ok || len(f.audit.Snapshot()) != 1 || f.audit.Snapshot()[0].Outcome != OutcomeRejected {
		t.Fatal("refused activation left state or a non-rejected audit")
	}
}

func TestActivateThenRegressionThenRollback(t *testing.T) {
	f := newActFixture()
	if c, m := f.post(rp1, "", "tok"); c != 200 || m["action"] != "activate" || m["active_processing_version"] != float64(1) || m["previous_processing_version"] != nil {
		t.Fatalf("%d %v", c, m)
	}
	if c, m := f.post(rp2, "", "tok"); c != 200 || m["active_processing_version"] != float64(2) || m["previous_processing_version"] != float64(1) {
		t.Fatalf("%d %v", c, m)
	}
	// lower than active -> refused, state untouched
	if c, m := f.post(rp1, "", "tok"); c != 409 || m["code"] != "version_regression" {
		t.Fatalf("%d %v", c, m)
	}
	if a, p, _ := f.act.Active(tenantA, srcX); a != 2 || p == nil || *p != 1 {
		t.Fatalf("state %d %v", a, p)
	}
	// idempotent re-activation: no audit, previous kept
	n := len(f.audit.Snapshot())
	if c, m := f.post(rp2, "", "tok"); c != 200 || m["action"] != "noop" || m["previous_processing_version"] != float64(1) {
		t.Fatalf("%d %v", c, m)
	}
	if len(f.audit.Snapshot()) != n {
		t.Fatal("noop was audited")
	}
	// rollback of a replay that is not the active version
	if c, m := f.post(rp1, "?rollback=true", "tok"); c != 409 || m["code"] != "rollback_unavailable" {
		t.Fatalf("%d %v", c, m)
	}
	// rollback of the active replay restores previous
	if c, m := f.post(rp2, "?rollback=true", "tok"); c != 200 || m["action"] != "rollback" || m["active_processing_version"] != float64(1) || m["previous_processing_version"] != float64(2) {
		t.Fatalf("%d %v", c, m)
	}
	// roll forward again is a normal activation
	if c, m := f.post(rp2, "", "tok"); c != 200 || m["active_processing_version"] != float64(2) {
		t.Fatalf("%d %v", c, m)
	}
	if c, _ := f.post(rp2, "?rollback=maybe", "tok"); c != 400 {
		t.Fatal(c)
	}
	// audit: activate, activate, rollback, activate; actor from the principal
	var got []string
	for _, e := range f.audit.Snapshot() {
		if e.Outcome == OutcomeRejected {
			continue
		}
		got = append(got, e.Action)
		if e.TenantID != tenantA || e.Subject != "alice" || e.Outcome != "success" || e.EventID == "" || e.SourceID != srcX {
			t.Fatalf("%+v", e)
		}
	}
	if strings.Join(got, ",") != "replay.activate,replay.activate,replay.rollback,replay.activate" {
		t.Fatal(got)
	}
}

func TestActivateCrossTenantIs404(t *testing.T) {
	f := newActFixture()
	if c, m := f.post(rpB, "", "tok"); c != 404 || m["code"] != "not_found" {
		t.Fatalf("%d %v", c, m)
	}
	if c, _ := f.post("not-a-uuid", "", "tok"); c != 404 {
		t.Fatal(c)
	}
	if _, _, ok := f.act.Active(tenantB, srcX); ok {
		t.Fatal("tenant B state changed by tenant A")
	}
}

func TestActivateWithoutActivatorIs501(t *testing.T) {
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Scopes: []string{ScopeReplayWrite}}}
	h := (&Handler{Auth: auth, Planner: &Planner{}}).Routes()
	r := httptest.NewRequest("POST", "/v1/replays/"+rp1+"/activate", nil)
	r.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 501 {
		t.Fatal(w.Code)
	}
}

func TestAuditFailureDoesNotFailRequest(t *testing.T) {
	f := newActFixture()
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Scopes: []string{ScopeReplayWrite}}}
	f.srv = (&Handler{Auth: auth, Planner: &Planner{}, Activator: f.act, Audit: failAudit{}}).Routes()
	if c, _ := f.post(rp1, "", "tok"); c != 200 {
		t.Fatal(c)
	}
}

type failAudit struct{}

func (failAudit) Record(context.Context, AuditEvent) error { return errors.New("disk full") }

func TestConcurrentMemActivationsSingleConsistentRow(t *testing.T) {
	f := newActFixture()
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		id := []string{rp1, rp2}[i%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = f.act.Activate(context.Background(), tenantA, id, false, "x", nil)
		}()
	}
	wg.Wait()
	if a, _, ok := f.act.Active(tenantA, srcX); !ok || a != 2 {
		t.Fatalf("active=%d", a)
	}
}

func TestJSONAuditSinkLines(t *testing.T) {
	var buf bytes.Buffer
	s := &JSONAuditSink{W: &buf}
	_ = s.Record(context.Background(), AuditEvent{Action: ActionActivate, TenantID: tenantA, ToVersion: ptr32(2), FromVersion: ptr32(1)})
	_ = s.Record(context.Background(), AuditEvent{Action: ActionRollback, TenantID: tenantA})
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatal(buf.String())
	}
	var e AuditEvent
	if err := json.Unmarshal([]byte(lines[0]), &e); err != nil || *e.ToVersion != 2 || *e.FromVersion != 1 {
		t.Fatalf("%v %+v", err, e)
	}
}
