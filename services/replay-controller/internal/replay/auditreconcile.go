package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Audit reconciler (`replay-controller reconcile-audit`).
//
// The two-phase audit publishes `prepared` before COMMIT and `committed`/`aborted` after, so a crash or a lost
// acknowledgement can leave a `prepared` event without an outcome. The reconciler reads a window of audit.events.v1,
// pairs events by (tenant, replay, source, action, request_id), and for every `prepared` older than the grace period
// without an outcome asks PostgreSQL what really happened (source_active_versions for activate/rollback,
// replay_requests for replay.started). It then publishes a resolving event (phase committed or aborted,
// resolved_by=reconciler, event_id derived from the prepared event so a re-run does not duplicate).
// In the other direction it reports `committed` events whose switch is not in the database and `aborted` events
// whose switch is in the database (contradictions, alert-worthy).

const (
	// ResolvedByReconciler marks events written by the reconciler.
	ResolvedByReconciler = "reconciler"

	DefaultReconcileGrace    = 2 * time.Minute
	DefaultReconcileTxWindow = time.Minute
	DefaultReconcileMaxKeys  = 200_000
	DefaultReconcileGroup    = "replay-controller-audit-reconciler"
	maxAuditRecordBytes      = 64 << 10
)

// reconEvent is an AuditEvent plus the reconciler marker (AuditEvent itself is owned by audit.go).
type reconEvent struct {
	AuditEvent
	ResolvedBy string `json:"resolved_by,omitempty"`
}

// ResolutionEventID derives the id of the event that resolves a prepared event. Pure function of the prepared event
// and the phase: every run produces the same id for the same prepared event.
func ResolutionEventID(prepared AuditEvent, phase string) string {
	id := prepared.EventID
	if id == "" {
		id = NewEventID(prepared)
	}
	h := sha256.Sum256([]byte(strings.Join([]string{"reconciler", id, phase}, "\x1f")))
	return hex.EncodeToString(h[:16])
}

// ---- facts and the pure judgement ----

// ActiveFact is the source_active_versions row of (tenant, source).
type ActiveFact struct {
	Found      bool
	Active     int32
	Previous   *int32
	SwitchedAt time.Time
	SwitchedBy *string
}

// StartFact is the newest replay_requests row of (tenant, source, target version).
type StartFact struct {
	Found  bool
	Status string
}

// Truth reads the state of record. tenantID is the audit EVENT's tenant (RLS is set per transaction from it).
type Truth interface {
	ActiveVersion(ctx context.Context, tenantID, sourceID string) (ActiveFact, error)
	ReplayStatus(ctx context.Context, tenantID, sourceID string, version int32) (StartFact, error)
}

// Verdict is what the database says about one audited change.
type Verdict int

const (
	VerdictUnknown Verdict = iota // cannot be proven either way (masked by a later change, ambiguous status)
	VerdictHappened
	VerdictNotHappened
)

func (v Verdict) String() string {
	return [...]string{"unknown", "happened", "not_happened"}[v]
}

// judgeActivation decides whether the audited switch to ev.ToVersion is in the database.
//
// switched_at is now() of the activation transaction, which started BEFORE the prepared event was stamped (the
// event is created inside the transaction), so the README's "switched_at >= prepared.ts" is applied with the
// tolerance txWindow: lower = ref - txWindow. A different `switched_by` (when the event has a subject) never counts.
// upper, when non-zero, rejects a switch that is later than the event itself (a retry made after an `aborted`).
func judgeActivation(ev AuditEvent, f ActiveFact, ref time.Time, txWindow time.Duration, upper time.Time) Verdict {
	if ev.ToVersion == nil {
		return VerdictUnknown
	}
	if !f.Found {
		return VerdictNotHappened
	}
	lower := ref.Add(-txWindow)
	byOK := ev.Subject == "" || (f.SwitchedBy != nil && *f.SwitchedBy == ev.Subject)
	if f.Active == *ev.ToVersion {
		switch {
		case f.SwitchedAt.Before(lower):
			return VerdictNotHappened // the row is older than this change
		case !byOK:
			return VerdictNotHappened // someone else's switch to the same version
		case !upper.IsZero() && f.SwitchedAt.After(upper):
			return VerdictUnknown
		}
		return VerdictHappened
	}
	if f.SwitchedAt.Before(lower) {
		return VerdictNotHappened // nothing was switched since: ours did not happen
	}
	// A different version is active and it was switched after this change began: a later activation may have masked
	// ours. The chain tells: an activation records the replaced version as previous.
	if f.Previous != nil && *f.Previous == *ev.ToVersion {
		return VerdictHappened
	}
	return VerdictUnknown
}

func judgeStart(f StartFact) Verdict {
	if !f.Found {
		return VerdictUnknown
	}
	switch f.Status {
	case "FAILED":
		return VerdictNotHappened
	case "RUNNING", "COMPLETED":
		return VerdictHappened // only a published job can be picked up
	}
	return VerdictUnknown // REQUESTED/APPROVED/CANCELLED: the job may or may not be on replay.jobs.v1
}

func judge(ctx context.Context, tr Truth, ev AuditEvent, ref time.Time, txWindow time.Duration, upper time.Time) (Verdict, error) {
	switch ev.Action {
	case ActionActivate, ActionRollback:
		f, err := tr.ActiveVersion(ctx, ev.TenantID, ev.SourceID)
		if err != nil {
			return VerdictUnknown, err
		}
		return judgeActivation(ev, f, ref, txWindow, upper), nil
	case ActionStart:
		if ev.ToVersion == nil {
			return VerdictUnknown, nil
		}
		f, err := tr.ReplayStatus(ctx, ev.TenantID, ev.SourceID, *ev.ToVersion)
		if err != nil {
			return VerdictUnknown, err
		}
		return judgeStart(f), nil
	}
	return VerdictUnknown, nil
}

// ---- PostgreSQL truth ----

const (
	sqlFactActive = `SELECT active_processing_version, previous_processing_version, switched_at, switched_by
FROM source_active_versions WHERE tenant_id = $1::uuid AND source_id = $2::uuid`
	sqlFactStart = `SELECT status::text FROM replay_requests
WHERE tenant_id = $1::uuid AND source_id = $2::uuid AND target_processing_version = $3 ORDER BY created_at DESC LIMIT 1`
)

// PGTruth reads facts with one read-only transaction per lookup; the RLS tenant is set from the audit event's
// tenant_id (the role must NOT bypass RLS, so another tenant's rows are invisible).
type PGTruth struct{ DB DB }

func (p PGTruth) tx(ctx context.Context, tenantID, sourceID string) (Tx, error) {
	if !uuidRe.MatchString(tenantID) || !uuidRe.MatchString(sourceID) {
		return nil, fmt.Errorf("audit event with malformed tenant/source id")
	}
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err := tx.Exec(ctx, sqlSetTenant, tenantID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func (p PGTruth) ActiveVersion(ctx context.Context, tenantID, sourceID string) (ActiveFact, error) {
	tx, err := p.tx(ctx, tenantID, sourceID)
	if err != nil {
		return ActiveFact{}, err
	}
	defer tx.Rollback(ctx)
	var f ActiveFact
	switch err := tx.QueryRow(ctx, sqlFactActive, tenantID, sourceID).Scan(&f.Active, &f.Previous, &f.SwitchedAt, &f.SwitchedBy); {
	case err == nil:
		f.Found = true
	case !isNoRows(err):
		return ActiveFact{}, err
	}
	return f, nil
}

func (p PGTruth) ReplayStatus(ctx context.Context, tenantID, sourceID string, version int32) (StartFact, error) {
	tx, err := p.tx(ctx, tenantID, sourceID)
	if err != nil {
		return StartFact{}, err
	}
	defer tx.Rollback(ctx)
	var f StartFact
	switch err := tx.QueryRow(ctx, sqlFactStart, tenantID, sourceID, version).Scan(&f.Status); {
	case err == nil:
		f.Found = true
	case !isNoRows(err):
		return StartFact{}, err
	}
	return f, nil
}

// ---- reconciler ----

// AuditReader streams the window of audit events (raw JSON values, at most maxAuditRecordBytes each). fn errors abort.
type AuditReader interface {
	Read(ctx context.Context, fn func(value []byte) error) error
}

// Emitter publishes a resolving event; it returns nil only when the event is durable.
type Emitter interface {
	Emit(ctx context.Context, tenantKey string, value []byte) error
}

// Reconciler configures one run.
type Reconciler struct {
	Reader   AuditReader
	Truth    Truth
	Emit     Emitter
	Grace    time.Duration // prepared events younger than this are left alone (default DefaultReconcileGrace)
	TxWindow time.Duration // tolerance between switched_at (tx start) and the event stamp (default DefaultReconcileTxWindow)
	MaxKeys  int           // bounded memory: more distinct audited changes than this in the window is an error
	DryRun   bool
	Now      func() time.Time
	Log      func(format string, args ...any)
}

// Report is the outcome of a run.
type Report struct {
	Scanned          int      `json:"scanned"`          // audit events of the three audited actions
	Malformed        int      `json:"malformed"`        // not JSON / oversized (ignored)
	Changes          int      `json:"changes"`          // distinct audited changes seen
	AlreadyResolved  int      `json:"already_resolved"` // prepared with committed/aborted
	Young            int      `json:"young"`            // prepared inside the grace period (in flight, left alone)
	ResolvedCommit   int      `json:"resolved_committed"`
	ResolvedAbort    int      `json:"resolved_aborted"`
	WouldResolve     int      `json:"would_resolve"`  // dry run: events that would be emitted
	Unresolved       int      `json:"unresolved"`     // prepared whose truth cannot be established
	Contradictions   int      `json:"contradictions"` // committed/aborted events the database disagrees with
	Unverifiable     int      `json:"unverifiable"`   // outcome cannot be checked (masked by a later change); informational
	Verified         int      `json:"verified"`       // outcome events confirmed by the database
	ContradictionIDs []string `json:"contradiction_event_ids,omitempty"`
}

// ExitCode is non-zero while unresolved or contradicting events remain (dry run: also while something would be resolved).
func (r Report) ExitCode(dryRun bool) int {
	if r.Unresolved > 0 || r.Contradictions > 0 || (dryRun && r.WouldResolve > 0) {
		return 1
	}
	return 0
}

type entry struct {
	prepared  *reconEvent
	committed *reconEvent
	aborted   *reconEvent
}

func changeKey(e AuditEvent) string {
	return strings.Join([]string{e.TenantID, e.ReplayID, e.SourceID, e.Action, e.RequestID}, "\x1f")
}

func isAudited(action string) bool {
	return action == ActionActivate || action == ActionRollback || action == ActionStart
}

// Run reads the window, then resolves and checks. The error is for infrastructure failures; findings are in the Report.
func (r *Reconciler) Run(ctx context.Context) (Report, error) {
	var rep Report
	if r.Reader == nil || r.Truth == nil || (r.Emit == nil && !r.DryRun) {
		return rep, errors.New("reconciler: reader, truth and emitter are required")
	}
	now := r.Now
	if now == nil {
		now = time.Now
	}
	grace, txw, maxKeys := r.Grace, r.TxWindow, r.MaxKeys
	if grace <= 0 {
		grace = DefaultReconcileGrace
	}
	if txw <= 0 {
		txw = DefaultReconcileTxWindow
	}
	if maxKeys <= 0 {
		maxKeys = DefaultReconcileMaxKeys
	}
	logf := r.Log
	if logf == nil {
		logf = log.Printf
	}

	changes := map[string]*entry{}
	err := r.Reader.Read(ctx, func(v []byte) error {
		if len(v) > maxAuditRecordBytes {
			rep.Malformed++
			return nil
		}
		var ev reconEvent
		if json.Unmarshal(v, &ev) != nil {
			rep.Malformed++
			return nil
		}
		if !isAudited(ev.Action) || ev.TenantID == "" ||
			(ev.Phase != PhasePrepared && ev.Phase != PhaseCommitted && ev.Phase != PhaseAborted) { // rejected etc.: single-shot
			return nil
		}
		k := changeKey(ev.AuditEvent)
		en := changes[k]
		if en == nil {
			if len(changes) >= maxKeys {
				return fmt.Errorf("reconciler: more than %d audited changes in the window (REPLAY_RECONCILE_MAX_KEYS); narrow -since/-until", maxKeys)
			}
			en = &entry{}
			changes[k] = en
		}
		rep.Scanned++
		e := ev
		switch ev.Phase {
		case PhasePrepared:
			if en.prepared == nil {
				en.prepared = &e
			}
		case PhaseCommitted:
			if en.committed == nil {
				en.committed = &e
			}
		case PhaseAborted:
			if en.aborted == nil {
				en.aborted = &e
			}
		}
		return nil
	})
	if err != nil {
		return rep, err
	}

	// deterministic processing order: oldest first
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	stamp := func(en *entry) time.Time {
		switch {
		case en.prepared != nil:
			return en.prepared.Time
		case en.committed != nil:
			return en.committed.Time
		}
		return en.aborted.Time
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := stamp(changes[keys[i]]), stamp(changes[keys[j]])
		if !a.Equal(b) {
			return a.Before(b)
		}
		return keys[i] < keys[j]
	})
	rep.Changes = len(keys)

	contradiction := func(ev AuditEvent, why string) {
		rep.Contradictions++
		rep.ContradictionIDs = append(rep.ContradictionIDs, ev.EventID)
		logf("ALERT audit contradiction: %s tenant=%s source=%s replay=%s action=%s event_id=%s request_id=%s",
			why, ev.TenantID, ev.SourceID, ev.ReplayID, ev.Action, ev.EventID, ev.RequestID)
	}

	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		en := changes[k]
		anyEv := en.prepared
		if anyEv == nil {
			anyEv = en.committed
		}
		if anyEv == nil {
			anyEv = en.aborted
		}

		if en.committed != nil && en.aborted != nil {
			contradiction(en.committed.AuditEvent, "both committed and aborted were audited")
			continue
		}
		if en.prepared != nil && en.committed == nil && en.aborted == nil {
			p := en.prepared
			if now().Sub(p.Time) < grace {
				rep.Young++
				continue
			}
			v, err := judge(ctx, r.Truth, p.AuditEvent, p.Time, txw, time.Time{})
			if err != nil {
				return rep, fmt.Errorf("reconciler: lookup for event %s: %w", p.EventID, err)
			}
			if v == VerdictUnknown {
				rep.Unresolved++
				logf("unresolved prepared event %s (%s tenant=%s source=%s): the database cannot prove either outcome", p.EventID, p.Action, p.TenantID, p.SourceID)
				continue
			}
			phase, outcome := PhaseAborted, OutcomeAborted
			if v == VerdictHappened {
				phase, outcome = PhaseCommitted, OutcomeSuccess
			}
			res := *p
			res.Phase, res.Outcome, res.Time, res.ResolvedBy = phase, outcome, now().UTC(), ResolvedByReconciler
			res.EventID = ResolutionEventID(p.AuditEvent, phase)
			if r.DryRun {
				rep.WouldResolve++
				logf("dry-run: would emit %s for prepared event %s (%s tenant=%s source=%s)", phase, p.EventID, p.Action, p.TenantID, p.SourceID)
				continue
			}
			b, err := json.Marshal(res)
			if err != nil {
				return rep, err
			}
			if err := r.Emit.Emit(ctx, res.TenantID, b); err != nil {
				return rep, fmt.Errorf("reconciler: emit resolution of %s: %w", p.EventID, err)
			}
			if phase == PhaseCommitted {
				rep.ResolvedCommit++
			} else {
				rep.ResolvedAbort++
			}
			continue
		}

		// An outcome exists: check it against the database.
		if en.prepared != nil {
			rep.AlreadyResolved++
		}
		switch {
		case en.committed != nil:
			c := en.committed
			ref := c.Time
			if en.prepared != nil {
				ref = en.prepared.Time
			}
			v, err := judge(ctx, r.Truth, c.AuditEvent, ref, txw, time.Time{})
			if err != nil {
				return rep, fmt.Errorf("reconciler: lookup for event %s: %w", c.EventID, err)
			}
			switch v {
			case VerdictHappened:
				rep.Verified++
			case VerdictNotHappened:
				contradiction(c.AuditEvent, "committed event but the switch is not in the database")
			default:
				rep.Unverifiable++
			}
		case en.aborted != nil:
			a := en.aborted
			ref := a.Time
			if en.prepared != nil {
				ref = en.prepared.Time
			}
			v, err := judge(ctx, r.Truth, a.AuditEvent, ref, txw, a.Time)
			if err != nil {
				return rep, fmt.Errorf("reconciler: lookup for event %s: %w", a.EventID, err)
			}
			switch v {
			case VerdictNotHappened:
				rep.Verified++
			case VerdictHappened:
				contradiction(a.AuditEvent, "aborted event but the switch is in the database")
			default:
				rep.Unverifiable++
			}
		}
	}
	return rep, nil
}

// ---- Kafka reader / emitter ----

// kafkaAuditReader consumes a topic with a dedicated consumer group. Offsets are NEVER committed: a window is
// re-read on every run (a prepared event and its outcome may be far apart), so the group only provides partition
// assignment. Events of one tenant share a partition (key = tenant_id), so a pair is never split across members.
type kafkaAuditReader struct {
	cl    *kgo.Client
	idle  time.Duration
	until time.Time
	mu    sync.Mutex
	asg   map[int32]bool
}

func newKafkaAuditReader(c KafkaConfig, topic, group string, start kgo.Offset, until time.Time, idle time.Duration) (*kafkaAuditReader, error) {
	rd := &kafkaAuditReader{idle: idle, until: until, asg: map[int32]bool{}}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID + "-reconciler"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(start),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			rd.mu.Lock()
			defer rd.mu.Unlock()
			for _, ps := range m {
				for _, p := range ps {
					rd.asg[p] = true
				}
			}
		}),
	}
	if c.TLS {
		tc, err := c.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	if m := c.saslMechanism(); m != nil {
		opts = append(opts, kgo.SASL(m))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	rd.cl = cl
	return rd, nil
}

func (k *kafkaAuditReader) Close() { k.cl.Close() }

// Read finishes when every assigned partition delivered a record stamped at/after the start of the run (or after
// `until`), or when polling is idle for k.idle once partitions are assigned.
func (k *kafkaAuditReader) Read(ctx context.Context, fn func([]byte) error) error {
	runStart := time.Now()
	caught := map[int32]bool{}
	for {
		wait := k.idle
		k.mu.Lock()
		n := len(k.asg)
		k.mu.Unlock()
		if n == 0 {
			wait = 30 * time.Second // joining the group
		}
		pctx, cancel := context.WithTimeout(ctx, wait)
		fetches := k.cl.PollFetches(pctx)
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("kafka fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
		recs := 0
		var ferr error
		fetches.EachRecord(func(r *kgo.Record) {
			if ferr != nil {
				return
			}
			recs++
			if (!k.until.IsZero() && r.Timestamp.After(k.until)) || r.Timestamp.After(runStart) {
				caught[r.Partition] = true
				if !k.until.IsZero() && r.Timestamp.After(k.until) {
					return
				}
			}
			ferr = fn(r.Value)
		})
		if ferr != nil {
			return ferr
		}
		k.mu.Lock()
		n = len(k.asg)
		all := n > 0
		for p := range k.asg {
			if !caught[p] {
				all = false
			}
		}
		k.mu.Unlock()
		if all {
			return nil
		}
		if recs == 0 {
			if n == 0 {
				return errors.New("kafka: no partitions assigned within 30s (broker unreachable or group join failed)")
			}
			return nil // idle: caught up
		}
	}
}

type kafkaEmitter struct {
	p     franzProducer
	topic string
}

func (e kafkaEmitter) Emit(ctx context.Context, key string, value []byte) error {
	return e.p.produce(ctx, e.topic, []byte(key), value)
}

// ---- CLI ----

// RunReconcileCLI implements `replay-controller reconcile-audit`. It returns the process exit code:
// 0 clean, 1 unresolved or contradicting events remain, 2 usage/infrastructure error.
func RunReconcileCLI(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("reconcile-audit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	since := fs.String("since", "24h", "window start: a duration back from now (24h) or an RFC3339 time")
	until := fs.String("until", "", "window end (RFC3339); default: the end of the topic at the time of the run")
	fromOffset := fs.Int64("from-offset", -1, "start every partition at this offset instead of -since")
	dry := fs.Bool("dry-run", false, "report only: emit nothing (exit 1 if something would be resolved)")
	grace := fs.Duration("grace", envDuration(getenv, "REPLAY_RECONCILE_GRACE", DefaultReconcileGrace), "leave prepared events younger than this alone (REPLAY_RECONCILE_GRACE)")
	txw := fs.Duration("tx-window", envDuration(getenv, "REPLAY_RECONCILE_TX_WINDOW", DefaultReconcileTxWindow), "tolerance between switched_at and the event stamp (REPLAY_RECONCILE_TX_WINDOW)")
	group := fs.String("group", orDefault(getenv("REPLAY_RECONCILE_GROUP"), DefaultReconcileGroup), "dedicated consumer group (offsets are never committed)")
	maxKeys := fs.Int("max-keys", DefaultReconcileMaxKeys, "bound on distinct audited changes held in memory")
	idle := fs.Duration("idle", 5*time.Second, "stop when the topic delivers nothing for this long")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	fail := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "reconcile-audit: "+format+"\n", a...)
		return 2
	}
	if *grace <= 0 || *txw <= 0 || *maxKeys <= 0 || *idle <= 0 {
		return fail("grace, tx-window, max-keys and idle must be positive")
	}
	start := kgo.NewOffset().AtStart()
	switch {
	case *fromOffset >= 0:
		start = kgo.NewOffset().At(*fromOffset)
	case *since != "":
		var at time.Time
		if d, err := time.ParseDuration(*since); err == nil {
			at = time.Now().Add(-d)
		} else if at, err = time.Parse(time.RFC3339, *since); err != nil {
			return fail("-since must be a duration or RFC3339: %v", err)
		}
		start = kgo.NewOffset().AfterMilli(at.UnixMilli())
	}
	var untilT time.Time
	if *until != "" {
		t, err := time.Parse(time.RFC3339, *until)
		if err != nil {
			return fail("-until must be RFC3339: %v", err)
		}
		untilT = t
	}
	pgURL := getenv("REPLAY_PG_URL")
	if pgURL == "" {
		return fail("REPLAY_PG_URL is required (role must NOT bypass RLS)")
	}
	kc, err := LoadKafkaConfig(getenv)
	if err != nil {
		return fail("%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return fail("REPLAY_PG_URL: %v", err)
	}
	defer pool.Close()
	rd, err := newKafkaAuditReader(kc, TopicAudit, *group, start, untilT, *idle)
	if err != nil {
		return fail("%v", err)
	}
	defer rd.Close()
	rec := &Reconciler{Reader: rd, Truth: PGTruth{DB: PoolDB{Pool: pool}}, Grace: *grace, TxWindow: *txw, MaxKeys: *maxKeys, DryRun: *dry,
		Log: func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) }}
	if !*dry {
		p, err := newFranzProducer(kc)
		if err != nil {
			return fail("%v", err)
		}
		defer p.close()
		rec.Emit = kafkaEmitter{p: p, topic: TopicAudit}
	}
	rep, err := rec.Run(ctx)
	_ = json.NewEncoder(stdout).Encode(rep)
	if err != nil {
		return fail("%v", err)
	}
	return rep.ExitCode(*dry)
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func envDuration(getenv func(string) string, key string, d time.Duration) time.Duration {
	if v := getenv(key); v != "" {
		if x, err := time.ParseDuration(v); err == nil && x > 0 {
			return x
		}
		log.Printf("WARN: %s=%q is not a positive duration, using %s", key, v, d)
	}
	return d
}
