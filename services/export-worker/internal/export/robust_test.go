package export

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stallCH serves `rows` NDJSON rows and then, depending on mode, hangs (stall) or keeps dripping rows (drip) until
// the client goes away. The connection is never closed by the server: the failure only a timeout can detect.
func stallCH(mode string, rows int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, ndjsonRows(rows))
		fl.Flush()
		for i := 0; ; i++ {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(15 * time.Millisecond):
			}
			if mode == "drip" {
				if _, err := fmt.Fprintf(w, "{\"event_id\":\"d%d\"}\n", i); err != nil {
					return
				}
				fl.Flush()
			}
		}
	}))
}

func runnerFor(url string, st ObjectStore, idle, max time.Duration) *Runner {
	return &Runner{CH: ClickHouse{URL: url, IdleTimeout: idle, MaxDuration: max}, Store: st, Audit: &MemoryAudit{}}
}

func TestCHIdleTimeoutFailsJobAndDeletesPartial(t *testing.T) {
	srv := stallCH("stall", 5)
	defer srv.Close()
	st := newMemStore()
	r := runnerFor(srv.URL, st, 150*time.Millisecond, time.Minute)
	t0 := time.Now()
	_, err := r.Run(context.Background(), Principal{TenantID: tenantA}, "exp-00000002", goodSpec())
	el := time.Since(t0)
	if !errors.Is(err, ErrCHTimeout) || !strings.Contains(err.Error(), "idle") {
		t.Fatalf("want idle timeout, got %v", err)
	}
	if el > 2*time.Second {
		t.Fatalf("took %s", el)
	}
	if got := errCode(err, context.Background()); got != "clickhouse_timeout" {
		t.Fatalf("code %s", got)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.objects) != 0 {
		t.Fatalf("partial object must be deleted: %v", st.objects)
	}
}

func TestCHMaxDurationBoundsASlowDrip(t *testing.T) {
	srv := stallCH("drip", 1)
	defer srv.Close()
	r := runnerFor(srv.URL, newMemStore(), 5*time.Second, 250*time.Millisecond) // idle never trips: data keeps coming
	spec := goodSpec()
	spec.MaxRows = 1_000_000
	t0 := time.Now()
	_, err := r.Run(context.Background(), Principal{TenantID: tenantA}, "exp-00000003", spec)
	if !errors.Is(err, ErrCHTimeout) || !strings.Contains(err.Error(), "max duration") {
		t.Fatalf("want max duration, got %v", err)
	}
	if el := time.Since(t0); el < 200*time.Millisecond || el > 2*time.Second {
		t.Fatalf("took %s", el)
	}
	if got := errCode(err, context.Background()); got != "clickhouse_timeout" {
		t.Fatalf("code %s", got)
	}
}

// A slow object store must not trip the idle timeout (the clock only runs inside a ClickHouse Read).
type trickleStore struct{ *memStore }

func (s trickleStore) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	buf := make([]byte, 16)
	var n int64
	for {
		time.Sleep(60 * time.Millisecond)
		k, err := r.Read(buf)
		n += int64(k)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
	}
}

func TestCHIdleTimeoutIgnoresSlowConsumer(t *testing.T) {
	ch := newFakeCH(ndjsonRows(8), 200)
	defer ch.Close()
	r := runnerFor(ch.URL, trickleStore{newMemStore()}, 100*time.Millisecond, time.Minute)
	if _, err := r.Run(context.Background(), Principal{TenantID: tenantA}, "exp-00000004", goodSpec()); err != nil {
		t.Fatal(err)
	}
}

// ownedView adds HeartbeatOwned (the LeaseReporter of the PG store) to the fake queue; lost ids are not reported.
type ownedView struct {
	*fqView
	mu   sync.Mutex
	lost map[string]bool
}

func (o *ownedView) HeartbeatOwned(ctx context.Context) ([]string, error) {
	o.q.mu.Lock()
	defer o.q.mu.Unlock()
	var ids []string
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, j := range o.q.jobs {
		if j.owner == o.w && !o.lost[j.ID] && (j.Status == StatusQueued || j.Status == StatusRunning) {
			ids = append(ids, j.ID)
		}
	}
	return ids, nil
}

func (o *ownedView) lose(id string) {
	o.mu.Lock()
	o.lost[id] = true
	o.mu.Unlock()
	o.q.mu.Lock() // what recovery does: FAILED worker_lost, owner cleared
	for _, j := range o.q.jobs {
		if j.ID == id {
			j.Status, j.ErrorCode, j.owner = StatusFailed, "worker_lost", ""
		}
	}
	o.q.mu.Unlock()
}

func newOwned(q *fakeQueue, w string) *ownedView {
	return &ownedView{fqView: q.view(w), lost: map[string]bool{}}
}

// probeStore records whether Put's context was cancelled and whether the object was deleted.
type probeStore struct {
	*memStore
	started   chan struct{}
	cancelled chan struct{}
}

func (p *probeStore) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	p.started <- struct{}{}
	_, _ = io.Copy(io.Discard, r)
	select {
	case <-ctx.Done():
		close(p.cancelled)
		return 0, ctx.Err()
	case <-time.After(10 * time.Second):
		return 0, errors.New("never cancelled")
	}
}

func TestLeaseLostCancelsRunAndWritesNothing(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	q := newFakeQueue()
	id := q.add(tenantA)
	v := newOwned(q, "w1")
	ps := &probeStore{memStore: newMemStore(), started: make(chan struct{}, 1), cancelled: make(chan struct{})}
	d := testDispatcher(v, ps, ch, 1)
	before := Counters.LeaseLost.Load()
	d.Start(context.Background())
	<-ps.started
	t0 := time.Now()
	v.lose(id)
	select {
	case <-ps.cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("run was not cancelled after the heartbeat showed the lease is lost")
	}
	t.Logf("cancelled %s after the lease was lost", time.Since(t0))
	waitFor(t, "counter", func() bool { return Counters.LeaseLost.Load() == before+1 })
	if j := q.get(id); j.Status != StatusFailed || j.ErrorCode != "worker_lost" {
		t.Fatalf("the recovered state must stay untouched: %+v", j)
	}
	shutdown(t, d)
}

// finishRejected: the owner guard says no (ok=false) after the object was written.
type finishRejected struct{ *fqView }

func (f finishRejected) Finish(context.Context, Job, JobResult) (bool, error) { return false, nil }

func TestRejectedFinishDeletesTheObject(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	q := newFakeQueue()
	id := q.add(tenantA)
	st := newMemStore()
	d := testDispatcher(finishRejected{q.view("w1")}, st, ch, 1)
	d.Start(context.Background())
	key := ObjectKey(tenantA, id, "ndjson")
	waitFor(t, "object deleted", func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		for _, k := range st.deleted {
			if k == key {
				_, still := st.objects[key]
				return !still
			}
		}
		return false
	})
	shutdown(t, d)
}

// leaseView gives the fake queue real lease semantics for the outage test: abandoned jobs are not heartbeated and
// RecoverJobs fails running jobs whose lease expired as worker_lost.
type leaseView struct {
	*fqView
	mu        sync.Mutex
	abandoned map[string]bool
	failFin   bool
}

func (l *leaseView) Finish(ctx context.Context, j Job, r JobResult) (bool, error) {
	l.mu.Lock()
	f := l.failFin
	l.mu.Unlock()
	if f {
		return false, errors.New("dial tcp: connection refused")
	}
	return l.fqView.Finish(ctx, j, r)
}
func (l *leaseView) Abandon(j Job) { l.mu.Lock(); l.abandoned[j.ID] = true; l.mu.Unlock() }
func (l *leaseView) Heartbeat(context.Context) (int, error) {
	l.q.mu.Lock()
	defer l.q.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, j := range l.q.jobs {
		if j.owner == l.w && !l.abandoned[j.ID] && (j.Status == StatusQueued || j.Status == StatusRunning) {
			j.lease = time.Now().Add(150 * time.Millisecond)
			n++
		}
	}
	return n, nil
}
func (l *leaseView) RecoverJobs(context.Context) (RecoverStats, error) {
	l.q.mu.Lock()
	defer l.q.mu.Unlock()
	var st RecoverStats
	for _, j := range l.q.jobs {
		if j.Status == StatusRunning && j.owner != "" && j.lease.Before(time.Now()) {
			j.Status, j.ErrorCode, j.owner = StatusFailed, "worker_lost", ""
			st.Failed++
		}
	}
	return st, nil
}

// An outage longer than the finish-retry window: the job is abandoned (alertable log line + counter), its lease
// stops being renewed and recovery fails it worker_lost; the runner is free again.
func TestOutageLongerThanFinishRetryAbandonsThenRecoveryMarksWorkerLost(t *testing.T) {
	var logs bytes.Buffer
	var lmu sync.Mutex
	log.SetOutput(writerFunc(func(p []byte) (int, error) { lmu.Lock(); defer lmu.Unlock(); return logs.Write(p) }))
	defer log.SetOutput(io.Discard)
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	q := newFakeQueue()
	id := q.add(tenantA)
	lv := &leaseView{fqView: q.view("w1"), abandoned: map[string]bool{}, failFin: true}
	d := testDispatcher(lv, newMemStore(), ch, 1)
	d.FinishRetry = 400 * time.Millisecond
	d.RecoverEvery = 20 * time.Millisecond
	before := Counters.JobsAbandoned.Load()
	d.Start(context.Background())
	waitFor(t, "abandon", func() bool { return Counters.JobsAbandoned.Load() == before+1 })
	waitFor(t, "recovery marks worker_lost", func() bool { j := q.get(id); return j.Status == StatusFailed && j.ErrorCode == "worker_lost" })
	lmu.Lock()
	out := logs.String()
	lmu.Unlock()
	if !strings.Contains(out, "ALERT export_job_abandoned job="+id) {
		t.Fatalf("missing alertable log line:\n%s", out)
	}
	shutdown(t, d)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func settledGoroutines(base int, d time.Duration) int {
	end := time.Now().Add(d)
	n := runtime.NumGoroutine()
	for n > base && time.Now().Before(end) {
		runtime.GC()
		time.Sleep(25 * time.Millisecond)
		n = runtime.NumGoroutine()
	}
	return n
}

// Many jobs through every end (success, ClickHouse error, idle timeout, max duration, lease lost, finish rejected,
// shutdown) must leave no goroutine behind.
func TestNoGoroutineLeakAcrossJobOutcomes(t *testing.T) {
	log.SetOutput(io.Discard)
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	good := newFakeCH(ndjsonRows(50), 200)
	bad := newFakeCH("boom", 500)
	stall := stallCH("stall", 3)
	drip := stallCH("drip", 1)
	hang := stallCH("stall", 3) // used for lease lost / shutdown: no idle timeout
	closeAll := func() {
		for _, s := range []*httptest.Server{good.Server, bad.Server, stall, drip, hang} {
			s.CloseClientConnections()
			s.Close()
		}
		tr.CloseIdleConnections()
	}
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	one := func(kind string) {
		q := newFakeQueue()
		id := q.add(tenantA)
		var view Queue = q.view("w1")
		var ov *ownedView
		switch kind {
		case "lease_lost":
			ov = newOwned(q, "w1")
			view = ov
		case "finish_rejected":
			view = finishRejected{q.view("w1")}
		}
		urls := map[string]string{"success": good.URL, "clickhouse_error": bad.URL, "idle_timeout": stall.URL, "max_duration": drip.URL,
			"lease_lost": hang.URL, "finish_rejected": good.URL, "shutdown": hang.URL}
		idle, max := time.Second, time.Minute
		switch kind {
		case "idle_timeout":
			idle = 50 * time.Millisecond
		case "max_duration":
			max = 120 * time.Millisecond
		case "lease_lost", "shutdown":
			idle, max = -1, -1
		}
		r := &Runner{CH: ClickHouse{URL: urls[kind], IdleTimeout: idle, MaxDuration: max, HTTP: client}, Store: newMemStore(), Audit: &MemoryAudit{}}
		d := &Dispatcher{Queue: view, Runner: r, Workers: 2, PollMin: time.Millisecond, PollMax: 5 * time.Millisecond,
			HeartbeatEvery: 5 * time.Millisecond, RecoverEvery: 10 * time.Millisecond}
		d.Start(context.Background())
		status := func(s, code string) {
			waitFor(t, kind, func() bool { j := q.get(id); return j.Status == s && j.ErrorCode == code })
		}
		switch kind {
		case "success":
			status(StatusSucceeded, "")
		case "clickhouse_error":
			status(StatusFailed, "clickhouse_error")
		case "idle_timeout", "max_duration":
			status(StatusFailed, "clickhouse_timeout")
		case "lease_lost":
			waitFor(t, "running", func() bool { return q.get(id).Status == StatusRunning })
			time.Sleep(30 * time.Millisecond)
			before := Counters.LeaseLost.Load()
			ov.lose(id)
			waitFor(t, "cancelled", func() bool { return Counters.LeaseLost.Load() > before })
		case "finish_rejected":
			before := Counters.FinishRejected.Load()
			waitFor(t, "rejected", func() bool { return Counters.FinishRejected.Load() > before })
		case "shutdown":
			waitFor(t, "running", func() bool { return q.get(id).Status == StatusRunning })
			ctx, c := context.WithTimeout(context.Background(), 20*time.Millisecond)
			d.Shutdown(ctx)
			c()
			status(StatusFailed, "shutdown")
			return
		}
		shutdown(t, d)
	}
	kinds := []string{"success", "clickhouse_error", "idle_timeout", "max_duration", "lease_lost", "finish_rejected", "shutdown"}
	const rounds = 8
	for round := 0; round < rounds; round++ {
		for _, k := range kinds {
			one(k)
		}
	}
	closeAll()
	if n := settledGoroutines(base, 5*time.Second); n > base {
		buf := make([]byte, 1<<16)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf("goroutines %d > baseline %d after %d jobs\n%s", n, base, rounds*len(kinds), buf)
	}
}

func TestPGHeartbeatOwnedReturnsExtendedIDs(t *testing.T) {
	db := &fakeDB{handler: func(_ *fakeTx, sql string, _ []any) (int64, Row, error) {
		if sql == sqlHeartbeatOwned {
			return 0, scanFn(func(d ...any) error { *(d[0].(*[]string)) = []string{jobA}; return nil }), nil
		}
		return 0, nil, nil
	}}
	s := &PGJobStore{DB: db, WorkerID: "w1"}
	ids, err := s.HeartbeatOwned(context.Background())
	if err != nil || len(ids) != 1 || ids[0] != jobA {
		t.Fatalf("%v %v", ids, err)
	}
	var _ LeaseReporter = s
}

// hangHeaders accepts the request and never answers: the stall happens BEFORE the response headers.
func hangHeaders() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
}

func TestCHHeaderTimeoutFailsJobAndLeaksNothing(t *testing.T) {
	srv := hangHeaders()
	tr := &http.Transport{}
	runtime.GC()
	base := runtime.NumGoroutine()
	r := &Runner{CH: ClickHouse{URL: srv.URL, HeaderTimeout: 100 * time.Millisecond, IdleTimeout: -1, MaxDuration: -1,
		HTTP: &http.Client{Transport: tr}}, Store: newMemStore(), Audit: &MemoryAudit{}}
	t0 := time.Now()
	_, err := r.Run(context.Background(), Principal{TenantID: tenantA}, "exp-00000003", goodSpec())
	if !errors.Is(err, ErrCHTimeout) || !strings.Contains(err.Error(), "header") {
		t.Fatalf("want header timeout, got %v", err)
	}
	if errCode(err, context.Background()) != "clickhouse_timeout" {
		t.Fatalf("code %q", errCode(err, context.Background()))
	}
	if el := time.Since(t0); el > 2*time.Second {
		t.Fatalf("took %s", el)
	}
	srv.CloseClientConnections()
	srv.Close()
	tr.CloseIdleConnections()
	if n := settledGoroutines(base, 5*time.Second); n > base {
		t.Fatalf("goroutines %d > baseline %d", n, base)
	}
}

// A parent cancellation during the header wait is the caller's cancellation, not a ClickHouse timeout.
func TestCHHeaderWaitParentCancelIsNotTimeout(t *testing.T) {
	srv := hangHeaders()
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	r := runnerFor(srv.URL, newMemStore(), -1, -1)
	r.CH.HeaderTimeout = time.Minute
	_, err := r.Run(ctx, Principal{TenantID: tenantA}, "exp-00000004", goodSpec())
	if err == nil || errors.Is(err, ErrCHTimeout) {
		t.Fatalf("got %v", err)
	}
}

// failingHB: heartbeat cannot reach the queue at all (PostgreSQL down/failover) after the job started.
type failingHB struct {
	*fqView
	down atomic.Bool
	abMu sync.Mutex
	ab   []string
}

func (f *failingHB) Abandon(j Job) { f.abMu.Lock(); f.ab = append(f.ab, j.ID); f.abMu.Unlock() }

func (f *failingHB) Heartbeat(ctx context.Context) (int, error) {
	if f.down.Load() {
		return 0, errors.New("dial tcp: connection refused")
	}
	return f.fqView.Heartbeat(ctx)
}

// Queue unreachable for a full lease: the DB lease has expired, so the run is cancelled (no zombie) and nothing is
// written; with LeaseTTL = 0 the run is left alone (it learns the truth at the first heartbeat/Finish after the outage).
func TestHeartbeatOutageLongerThanLeaseCancelsRun(t *testing.T) {
	log.SetOutput(io.Discard)
	for _, ttl := range []time.Duration{150 * time.Millisecond, 0} {
		ch := newFakeCH(ndjsonRows(3), 200)
		q := newFakeQueue()
		id := q.add(tenantA)
		hb := &failingHB{fqView: q.view("w1")}
		ps := &probeStore{memStore: newMemStore(), started: make(chan struct{}, 1), cancelled: make(chan struct{})}
		d := testDispatcher(hb, ps, ch, 1)
		d.LeaseTTL = ttl
		before := Counters.LeaseLost.Load()
		d.Start(context.Background())
		<-ps.started
		hb.down.Store(true)
		if ttl > 0 {
			select {
			case <-ps.cancelled:
			case <-time.After(3 * time.Second):
				t.Fatal("run not cancelled after a heartbeat outage longer than the lease")
			}
			waitFor(t, "counter", func() bool { return Counters.LeaseLost.Load() == before+1 })
			if j := q.get(id); j.Status != StatusRunning {
				t.Fatalf("nothing may be written by the zombie: %+v", j)
			}
			// Found live (e2e postgres_restart_and_outage): once PostgreSQL returned, the heartbeat renewed the lease of this
			// runner-less RUNNING row forever. It must be abandoned so recovery fails it as worker_lost.
			waitFor(t, "abandon", func() bool { hb.abMu.Lock(); defer hb.abMu.Unlock(); return len(hb.ab) == 1 && hb.ab[0] == id })
		} else {
			select {
			case <-ps.cancelled:
				t.Fatal("LeaseTTL=0 must not cancel on outage")
			case <-time.After(400 * time.Millisecond):
			}
		}
		d.jobCancel() // release the probe (ttl=0 case)
		shutdown(t, d)
		ch.Close()
	}
}
