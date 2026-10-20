package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Live reconciler test. Gated on REPLAY_PG_URL (non-superuser NOBYPASSRLS role, migrations 0001-0010 applied) and
// REPLAY_KAFKA_IT_BROKERS (topic audit.events.v1 must exist). Real PGActivator switches, real Kafka round trips.

// tapReader wraps the real Kafka reader, keeps only events of the given tenants and records them.
type tapReader struct {
	in      AuditReader
	tenants map[string]bool
	seen    []reconEvent
}

func (r *tapReader) Read(ctx context.Context, fn func([]byte) error) error {
	r.seen = nil
	return r.in.Read(ctx, func(v []byte) error {
		var e reconEvent
		if json.Unmarshal(v, &e) != nil || !r.tenants[e.TenantID] {
			return nil
		}
		r.seen = append(r.seen, e)
		return fn(v)
	})
}

func TestLiveReconciler(t *testing.T) {
	brokers := os.Getenv("REPLAY_KAFKA_IT_BROKERS")
	if brokers == "" {
		t.Skip("REPLAY_KAFKA_IT_BROKERS not set")
	}
	pool, tA, tB, _, _ := pgFixture(t) // skips without REPLAY_PG_URL
	ctx := context.Background()
	kc, err := LoadKafkaConfig(func(k string) string { return map[string]string{"ASTRYON_KAFKA_BROKERS": brokers}[k] })
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewKafkaAuditSink(kc, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	admin := func(sql string, args ...any) string {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, _ = tx.Exec(ctx, sqlAsAdmin)
		var id string
		if err := tx.QueryRow(ctx, sql, args...).Scan(&id); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return id
	}
	var sources []string
	t.Cleanup(func() {
		admin(`WITH d AS (DELETE FROM source_active_versions WHERE tenant_id IN ($1::uuid,$2::uuid) RETURNING 1) SELECT ''`, tA, tB)
	})
	// scenario fixture: own source + COMPLETED replay targeting v1
	mk := func() (src, rep string) {
		src = admin(`INSERT INTO sources (tenant_id, name, kind) VALUES ($1::uuid, 'rc-' || gen_random_uuid()::text, 'http_push') RETURNING id::text`, tA)
		sources = append(sources, src)
		rep = admin(`INSERT INTO replay_requests (tenant_id, source_id, target_processing_version, status) VALUES ($1::uuid,$2::uuid,1,'COMPLETED') RETURNING id::text`, tA, src)
		return
	}
	prep := func(a Activation, rid string) AuditEvent {
		e := AuditEvent{Time: time.Now().UTC(), TenantID: tA, Subject: "it", Action: "replay." + a.Action, ReplayID: a.ReplayID, SourceID: a.SourceID,
			FromVersion: a.PreviousProcessingVersion, ToVersion: ptr32(a.ActiveProcessingVersion), Outcome: OutcomePending, Phase: PhasePrepared, RequestID: rid}
		e.EventID = NewEventID(e)
		return e
	}
	follow := func(p AuditEvent, phase, outcome string) {
		p.Phase, p.Outcome, p.Time = phase, outcome, time.Now().UTC()
		p.EventID = NewEventID(p)
		if err := sink.Record(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	act := &PGActivator{DB: PoolDB{pool}}
	sfx := fmt.Sprint(time.Now().UnixNano())

	// A: lost ack. Prepared published inside the tx, COMMIT succeeded, no follow-up.
	srcA, repA := mk()
	var pA AuditEvent
	if _, err := act.Activate(ctx, tA, repA, false, "it", func(c context.Context, a Activation) error {
		pA = prep(a, "A"+sfx)
		return sink.Record(c, pA)
	}); err != nil {
		t.Fatal(err)
	}
	// B: crash before COMMIT. Prepared published, then the process dies (tx rolled back), no follow-up.
	srcB, repB := mk()
	var pB AuditEvent
	if _, err := act.Activate(ctx, tA, repB, false, "it", func(c context.Context, a Activation) error {
		pB = prep(a, "B"+sfx)
		if err := sink.Record(c, pB); err != nil {
			return err
		}
		return errors.New("simulated crash before COMMIT")
	}); err == nil {
		t.Fatal("expected the simulated crash")
	}
	// C: already resolved both ways.
	srcC, repC := mk()
	var pC AuditEvent
	if _, err := act.Activate(ctx, tA, repC, false, "it", func(c context.Context, a Activation) error { pC = prep(a, "C"+sfx); return sink.Record(c, pC) }); err != nil {
		t.Fatal(err)
	}
	follow(pC, PhaseCommitted, OutcomeSuccess)
	srcD, repD := mk()
	var pD AuditEvent
	_, _ = act.Activate(ctx, tA, repD, false, "it", func(c context.Context, a Activation) error {
		pD = prep(a, "D"+sfx)
		_ = sink.Record(c, pD)
		return errors.New("commit failed")
	})
	follow(pD, PhaseAborted, OutcomeAborted)
	// E: contradiction. A committed event for a switch that is not in the database.
	srcE, repE := mk()
	ghost := prep(Activation{ReplayID: repE, SourceID: srcE, Action: "activate", ActiveProcessingVersion: 1}, "E"+sfx)
	follow(ghost, PhaseCommitted, OutcomeSuccess)
	_ = srcA
	_ = srcB
	_ = srcC
	_ = srcD

	time.Sleep(2 * time.Second) // grace
	runOnce := func(dry bool) (Report, *tapReader) {
		t.Helper()
		rd, err := newKafkaAuditReader(kc, TopicAudit, "rc-it-"+sfx, kgo.NewOffset().AtStart(), time.Time{}, 4*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()
		tap := &tapReader{in: rd, tenants: map[string]bool{tA: true}}
		p, err := newFranzProducer(kc)
		if err != nil {
			t.Fatal(err)
		}
		defer p.close()
		r := &Reconciler{Reader: tap, Truth: PGTruth{DB: PoolDB{pool}}, Emit: kafkaEmitter{p: p, topic: TopicAudit}, Grace: time.Second,
			TxWindow: time.Minute, DryRun: dry, Log: t.Logf}
		rep, err := r.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("report: %+v", rep)
		return rep, tap
	}
	count := func(tap *tapReader, rid string) (n int, phase string) {
		for _, e := range tap.seen {
			if e.RequestID == rid && e.ResolvedBy == ResolvedByReconciler {
				n++
				phase = e.Phase
			}
		}
		return
	}

	rep, _ := runOnce(true)
	if rep.WouldResolve != 2 || rep.ResolvedCommit+rep.ResolvedAbort != 0 || rep.Contradictions != 1 || rep.ExitCode(true) != 1 {
		t.Fatalf("dry-run %+v", rep)
	}
	rep, _ = runOnce(false)
	if rep.ResolvedCommit != 1 || rep.ResolvedAbort != 1 || rep.Contradictions != 1 || rep.Verified != 2 || rep.ExitCode(false) != 1 {
		t.Fatalf("first run %+v", rep)
	}
	// re-run: nothing new is emitted, resolutions are exactly one each, ids derived from the prepared events
	rep, tap := runOnce(false)
	if rep.ResolvedCommit+rep.ResolvedAbort != 0 || rep.Contradictions != 1 || rep.Unresolved != 0 || rep.AlreadyResolved != 4 {
		t.Fatalf("re-run %+v", rep)
	}
	if n, ph := count(tap, "A"+sfx); n != 1 || ph != PhaseCommitted {
		t.Fatalf("A: %d %s", n, ph)
	}
	if n, ph := count(tap, "B"+sfx); n != 1 || ph != PhaseAborted {
		t.Fatalf("B: %d %s", n, ph)
	}
	for _, rid := range []string{"C", "D", "E"} {
		if n, _ := count(tap, rid+sfx); n != 0 {
			t.Fatalf("%s must be untouched, got %d resolutions", rid, n)
		}
	}
	ids := map[string]bool{}
	for _, e := range tap.seen {
		if e.ResolvedBy != "" {
			ids[e.EventID] = true
		}
	}
	if !ids[ResolutionEventID(pA, PhaseCommitted)] || !ids[ResolutionEventID(pB, PhaseAborted)] || len(ids) != 2 {
		t.Fatalf("resolution ids %v", ids)
	}

	// tenant isolation of the lookup: tenant B's session cannot see tenant A's switch
	f, err := PGTruth{DB: PoolDB{pool}}.ActiveVersion(ctx, tB, srcA)
	if err != nil || f.Found {
		t.Fatalf("cross-tenant lookup saw a row: %+v %v", f, err)
	}
}
