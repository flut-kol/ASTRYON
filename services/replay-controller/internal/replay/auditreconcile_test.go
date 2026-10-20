package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type sliceReader struct{ vals [][]byte }

func (r *sliceReader) Read(_ context.Context, fn func([]byte) error) error {
	for _, v := range r.vals {
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}

type fakeTruth struct {
	active  map[[2]string]ActiveFact
	status  map[string]StartFact
	lookups []string
	err     error
}

func (f *fakeTruth) ActiveVersion(_ context.Context, t, s string) (ActiveFact, error) {
	f.lookups = append(f.lookups, t)
	return f.active[[2]string{t, s}], f.err
}
func (f *fakeTruth) ReplayStatus(_ context.Context, t, s string, v int32) (StartFact, error) {
	f.lookups = append(f.lookups, t)
	return f.status[t+s], f.err
}

type recEmitter struct {
	mu   sync.Mutex
	keys []string
	evs  []reconEvent
	err  error
}

func (e *recEmitter) Emit(_ context.Context, key string, v []byte) error {
	if e.err != nil {
		return e.err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var ev reconEvent
	if err := json.Unmarshal(v, &ev); err != nil {
		return err
	}
	e.keys = append(e.keys, key)
	e.evs = append(e.evs, ev)
	return nil
}

var t0 = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

func i32(v int32) *int32 { return &v }

func mkEv(action, phase string, at time.Time, rid string) AuditEvent {
	e := AuditEvent{Time: at, TenantID: "t1", Subject: "alice", Action: action, ReplayID: "r1", SourceID: "s1",
		FromVersion: i32(1), ToVersion: i32(2), Outcome: OutcomePending, Phase: phase, RequestID: rid}
	switch phase {
	case PhaseCommitted:
		e.Outcome = OutcomeSuccess
	case PhaseAborted:
		e.Outcome = OutcomeAborted
	}
	e.EventID = NewEventID(e)
	return e
}

func raw(evs ...AuditEvent) *sliceReader {
	r := &sliceReader{}
	for _, e := range evs {
		b, _ := json.Marshal(e)
		r.vals = append(r.vals, b)
	}
	return r
}

func switched(at time.Time, v int32, by string) ActiveFact {
	return ActiveFact{Found: true, Active: v, Previous: i32(1), SwitchedAt: at, SwitchedBy: &by}
}

func run(t *testing.T, rd AuditReader, tr Truth, em Emitter, dry bool) (Report, string) {
	t.Helper()
	var logs bytes.Buffer
	r := &Reconciler{Reader: rd, Truth: tr, Emit: em, Grace: 2 * time.Minute, TxWindow: time.Minute, DryRun: dry,
		Now: func() time.Time { return t0.Add(time.Hour) }, Log: func(f string, a ...any) { logs.WriteString(strings.TrimSpace(strings.ReplaceAll(f, "%", "")) + "\n") }}
	rep, err := r.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return rep, logs.String()
}

func TestReconcileLostAckBecomesCommitted(t *testing.T) {
	p := mkEv(ActionActivate, PhasePrepared, t0, "q1")
	// switched_at = tx start, BEFORE the prepared stamp: the tolerance must accept it
	tr := &fakeTruth{active: map[[2]string]ActiveFact{{"t1", "s1"}: switched(t0.Add(-300*time.Millisecond), 2, "alice")}}
	em := &recEmitter{}
	rep, _ := run(t, raw(p), tr, em, false)
	if rep.ResolvedCommit != 1 || rep.ExitCode(false) != 0 || len(em.evs) != 1 {
		t.Fatalf("%+v %v", rep, em.evs)
	}
	e := em.evs[0]
	if e.Phase != PhaseCommitted || e.Outcome != OutcomeSuccess || e.ResolvedBy != "reconciler" || e.EventID != ResolutionEventID(p, PhaseCommitted) || em.keys[0] != "t1" {
		t.Fatalf("%+v", e)
	}
	if e.RequestID != "q1" || e.ReplayID != "r1" || *e.ToVersion != 2 {
		t.Fatalf("identity lost: %+v", e)
	}
}

func TestReconcileCrashBeforeCommitBecomesAborted(t *testing.T) {
	p := mkEv(ActionActivate, PhasePrepared, t0, "q1")
	for name, f := range map[string]ActiveFact{
		"no row":            {},
		"old row same ver":  switched(t0.Add(-time.Hour), 2, "alice"),
		"old row other ver": switched(t0.Add(-time.Hour), 3, "alice"),
		"other subject":     switched(t0, 2, "mallory"),
	} {
		em := &recEmitter{}
		rep, _ := run(t, raw(p), &fakeTruth{active: map[[2]string]ActiveFact{{"t1", "s1"}: f}}, em, false)
		if rep.ResolvedAbort != 1 || len(em.evs) != 1 || em.evs[0].Phase != PhaseAborted || em.evs[0].Outcome != OutcomeAborted {
			t.Fatalf("%s: %+v", name, rep)
		}
	}
}

func TestReconcileMaskedByLaterActivation(t *testing.T) {
	p := mkEv(ActionActivate, PhasePrepared, t0, "q1")
	// someone activated v3 later; previous = our v2 proves ours committed
	f := ActiveFact{Found: true, Active: 3, Previous: i32(2), SwitchedAt: t0.Add(time.Minute), SwitchedBy: strp("bob")}
	em := &recEmitter{}
	rep, _ := run(t, raw(p), &fakeTruth{active: map[[2]string]ActiveFact{{"t1", "s1"}: f}}, em, false)
	if rep.ResolvedCommit != 1 {
		t.Fatalf("%+v", rep)
	}
	// previous is something else: cannot prove, stays unresolved and the exit code is non-zero
	f.Previous = i32(1)
	em = &recEmitter{}
	rep, _ = run(t, raw(p), &fakeTruth{active: map[[2]string]ActiveFact{{"t1", "s1"}: f}}, em, false)
	if rep.Unresolved != 1 || len(em.evs) != 0 || rep.ExitCode(false) != 1 {
		t.Fatalf("%+v", rep)
	}
}

func strp(s string) *string { return &s }

func TestReconcileAlreadyResolvedUntouched(t *testing.T) {
	p := mkEv(ActionActivate, PhasePrepared, t0, "q1")
	c := mkEv(ActionActivate, PhaseCommitted, t0.Add(time.Second), "q1")
	a := mkEv(ActionRollback, PhaseAborted, t0.Add(time.Second), "q2")
	pa := mkEv(ActionRollback, PhasePrepared, t0, "q2")
	tr := &fakeTruth{active: map[[2]string]ActiveFact{{"t1", "s1"}: switched(t0, 2, "alice")}}
	// aborted rollback: to_version 2 is active but switched_at <= aborted ts and by alice => that is a contradiction.
	// Use a different source for the aborted one to keep the first case clean.
	pa.SourceID, a.SourceID = "s2", "s2"
	tr.active[[2]string{"t1", "s2"}] = ActiveFact{}
	em := &recEmitter{}
	rep, _ := run(t, raw(p, c, pa, a), tr, em, false)
	if len(em.evs) != 0 || rep.AlreadyResolved != 2 || rep.Verified != 2 || rep.ExitCode(false) != 0 {
		t.Fatalf("%+v", rep)
	}
}

func TestReconcileContradictions(t *testing.T) {
	// committed, DB has nothing
	c := mkEv(ActionActivate, PhaseCommitted, t0, "q1")
	rep, logs := run(t, raw(c), &fakeTruth{}, &recEmitter{}, false)
	if rep.Contradictions != 1 || rep.ExitCode(false) != 1 || !strings.Contains(logs, "ALERT") || rep.ContradictionIDs[0] != c.EventID {
		t.Fatalf("%+v %s", rep, logs)
	}
	// aborted but the switch is in the DB (lost COMMIT ack)
	p := mkEv(ActionActivate, PhasePrepared, t0, "q2")
	a := mkEv(ActionActivate, PhaseAborted, t0.Add(2*time.Second), "q2")
	tr := &fakeTruth{active: map[[2]string]ActiveFact{{"t1", "s1"}: switched(t0.Add(-time.Second), 2, "alice")}}
	rep, _ = run(t, raw(p, a), tr, &recEmitter{}, false)
	if rep.Contradictions != 1 {
		t.Fatalf("%+v", rep)
	}
	// aborted, then a LATER retry switched the same version: not a contradiction of the aborted event
	tr = &fakeTruth{active: map[[2]string]ActiveFact{{"t1", "s1"}: switched(t0.Add(time.Hour), 2, "alice")}}
	rep, _ = run(t, raw(p, a), tr, &recEmitter{}, false)
	if rep.Contradictions != 0 || rep.Unverifiable != 1 {
		t.Fatalf("%+v", rep)
	}
	// committed AND aborted for one change
	cc := mkEv(ActionActivate, PhaseCommitted, t0.Add(time.Second), "q2")
	rep, _ = run(t, raw(p, cc, a), &fakeTruth{}, &recEmitter{}, false)
	if rep.Contradictions != 1 {
		t.Fatalf("%+v", rep)
	}
}

func TestReconcileGraceDryRunAndIdempotentIDs(t *testing.T) {
	young := mkEv(ActionActivate, PhasePrepared, t0.Add(time.Hour-30*time.Second), "q1")
	rep, _ := run(t, raw(young), &fakeTruth{}, &recEmitter{}, false)
	if rep.Young != 1 || rep.ExitCode(false) != 0 {
		t.Fatalf("%+v", rep)
	}
	p := mkEv(ActionActivate, PhasePrepared, t0, "q1")
	em := &recEmitter{}
	rep, _ = run(t, raw(p), &fakeTruth{}, em, true)
	if rep.WouldResolve != 1 || len(em.evs) != 0 || rep.ExitCode(true) != 1 {
		t.Fatalf("dry-run %+v", rep)
	}
	// re-run: feed the first run's emitted event back (as the topic would) => nothing new, same id
	em = &recEmitter{}
	run(t, raw(p), &fakeTruth{}, em, false)
	again := em.evs[0]
	b, _ := json.Marshal(again)
	em2 := &recEmitter{}
	rep, _ = run(t, &sliceReader{vals: [][]byte{mustJSON(p), b}}, &fakeTruth{}, em2, false)
	if len(em2.evs) != 0 || rep.AlreadyResolved != 1 || rep.Verified != 1 || rep.ExitCode(false) != 0 {
		t.Fatalf("rerun %+v", rep)
	}
	if again.EventID != ResolutionEventID(p, PhaseAborted) {
		t.Fatal("event id not derived from the prepared event")
	}
}

func mustJSON(e AuditEvent) []byte { b, _ := json.Marshal(e); return b }

func TestReconcileStartedAndIgnored(t *testing.T) {
	p := mkEv(ActionStart, PhasePrepared, t0, "q1")
	for status, want := range map[string]string{"FAILED": PhaseAborted, "RUNNING": PhaseCommitted, "COMPLETED": PhaseCommitted} {
		em := &recEmitter{}
		run(t, raw(p), &fakeTruth{status: map[string]StartFact{"t1s1": {Found: true, Status: status}}}, em, false)
		if len(em.evs) != 1 || em.evs[0].Phase != want {
			t.Fatalf("%s: %+v", status, em.evs)
		}
	}
	for _, f := range []StartFact{{}, {Found: true, Status: "REQUESTED"}} {
		rep, _ := run(t, raw(p), &fakeTruth{status: map[string]StartFact{"t1s1": f}}, &recEmitter{}, false)
		if rep.Unresolved != 1 {
			t.Fatalf("%+v", f)
		}
	}
	// noise: auth.denied, plan, rejected, garbage
	den := mkEv("auth.denied", "", t0, "x")
	rej := mkEv(ActionActivate, "", t0, "y")
	rd := raw(den, rej)
	rd.vals = append(rd.vals, []byte("not json"))
	rep, _ := run(t, rd, &fakeTruth{}, &recEmitter{}, false)
	if rep.Changes != 0 || rep.Malformed != 1 || rep.ExitCode(false) != 0 {
		t.Fatalf("%+v", rep)
	}
}

func TestReconcileBoundedAndErrors(t *testing.T) {
	r := &Reconciler{Reader: raw(mkEv(ActionActivate, PhasePrepared, t0, "a"), mkEv(ActionActivate, PhasePrepared, t0, "b")),
		Truth: &fakeTruth{}, Emit: &recEmitter{}, MaxKeys: 1, Log: func(string, ...any) {}}
	if _, err := r.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "narrow") {
		t.Fatalf("%v", err)
	}
	p := mkEv(ActionActivate, PhasePrepared, t0, "a")
	r = &Reconciler{Reader: raw(p), Truth: &fakeTruth{}, Emit: &recEmitter{err: errors.New("broker down")}, Log: func(string, ...any) {}, Now: func() time.Time { return t0.Add(time.Hour) }}
	if _, err := r.Run(context.Background()); err == nil {
		t.Fatal("emit failure must be an error")
	}
	r = &Reconciler{Reader: raw(p), Truth: &fakeTruth{err: errors.New("pg down")}, Emit: &recEmitter{}, Log: func(string, ...any) {}, Now: func() time.Time { return t0.Add(time.Hour) }}
	if _, err := r.Run(context.Background()); err == nil {
		t.Fatal("lookup failure must be an error")
	}
}

func TestReconcileTenantFromEventForLookup(t *testing.T) {
	p := mkEv(ActionActivate, PhasePrepared, t0, "a")
	p.TenantID = "tenant-from-event"
	tr := &fakeTruth{}
	run(t, raw(p), tr, &recEmitter{}, false)
	if len(tr.lookups) != 1 || tr.lookups[0] != "tenant-from-event" {
		t.Fatalf("%v", tr.lookups)
	}
}

func TestPGTruthSetsEventTenantAndRejectsBadIDs(t *testing.T) {
	db := &recDB{}
	_, err := PGTruth{DB: db}.ActiveVersion(context.Background(), "not-a-uuid", "00000000-0000-0000-0000-000000000001")
	if err == nil || db.began {
		t.Fatalf("bad tenant must not open a transaction: %v", err)
	}
}

type recDB struct{ began bool }

func (d *recDB) Begin(context.Context) (Tx, error) { d.began = true; return nil, errors.New("no") }

func TestReconcileCLIConfigErrors(t *testing.T) {
	var out, errb bytes.Buffer
	env := func(map[string]string) func(string) string { return func(string) string { return "" } }(nil)
	if c := RunReconcileCLI(nil, env, &out, &errb); c != 2 || !strings.Contains(errb.String(), "REPLAY_PG_URL") {
		t.Fatalf("%d %s", c, errb.String())
	}
	errb.Reset()
	if c := RunReconcileCLI([]string{"-since", "bogus"}, env, &out, &errb); c != 2 {
		t.Fatalf("%d", c)
	}
}
