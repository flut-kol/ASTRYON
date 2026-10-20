package export

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeQueue is an in-memory Queue shared by several dispatchers (FIFO claim, owner + lease per job).
type fakeQueue struct {
	mu        sync.Mutex
	jobs      []*fqJob
	claimLog  []string
	hbCalls   map[string]int
	startFail bool
	finFail   bool
}
type fqJob struct {
	Job
	owner string
	lease time.Time
}

func newFakeQueue() *fakeQueue { return &fakeQueue{hbCalls: map[string]int{}} }

func (q *fakeQueue) add(tenant string) string {
	id := NewExportID()
	q.mu.Lock()
	q.jobs = append(q.jobs, &fqJob{Job: Job{ID: id, TenantID: tenant, Subject: "u", Spec: goodSpec(), Status: StatusQueued}})
	q.mu.Unlock()
	return id
}

func (q *fakeQueue) get(id string) fqJob {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, j := range q.jobs {
		if j.ID == id {
			return *j
		}
	}
	panic("no job")
}

func (q *fakeQueue) countStatus(s string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, j := range q.jobs {
		if j.Status == s {
			n++
		}
	}
	return n
}

// view returns a queue handle bound to a worker id.
func (q *fakeQueue) view(w string) *fqView { return &fqView{q: q, w: w} }

type fqView struct {
	q *fakeQueue
	w string
}

func (v *fqView) Claim(context.Context) (Job, bool, error) {
	q := v.q
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, j := range q.jobs {
		if j.Status == StatusQueued && j.owner == "" {
			j.owner, j.lease = v.w, time.Now().Add(time.Minute)
			q.claimLog = append(q.claimLog, j.ID)
			return j.Job, true, nil
		}
	}
	return Job{}, false, nil
}

func (v *fqView) find(id string) *fqJob {
	for _, j := range v.q.jobs {
		if j.ID == id {
			return j
		}
	}
	return nil
}

func (v *fqView) Start(_ context.Context, jb Job) (bool, error) {
	v.q.mu.Lock()
	defer v.q.mu.Unlock()
	j := v.find(jb.ID)
	if v.q.startFail || j.owner != v.w || j.Status != StatusQueued {
		return false, nil
	}
	j.Status, j.StartedAt = StatusRunning, time.Now()
	return true, nil
}

func (v *fqView) Finish(_ context.Context, jb Job, r JobResult) (bool, error) {
	v.q.mu.Lock()
	defer v.q.mu.Unlock()
	j := v.find(jb.ID)
	if v.q.finFail || j.owner != v.w || j.Status != StatusRunning {
		return false, nil
	}
	j.Status, j.Rows, j.Bytes, j.ObjectKey, j.ErrorCode, j.owner = r.Status, r.Rows, r.Bytes, r.ObjectKey, r.ErrorCode, ""
	return true, nil
}

func (v *fqView) Release(_ context.Context, jb Job) error {
	v.q.mu.Lock()
	defer v.q.mu.Unlock()
	if j := v.find(jb.ID); j.owner == v.w && j.Status == StatusQueued {
		j.owner = ""
	}
	return nil
}

func (v *fqView) Heartbeat(context.Context) (int, error) {
	v.q.mu.Lock()
	defer v.q.mu.Unlock()
	n := 0
	for _, j := range v.q.jobs {
		if j.owner == v.w && (j.Status == StatusQueued || j.Status == StatusRunning) {
			j.lease = time.Now().Add(time.Minute)
			v.q.hbCalls[j.ID]++
			n++
		}
	}
	return n, nil
}

func (v *fqView) RecoverJobs(context.Context) (RecoverStats, error) { return RecoverStats{}, nil }

func (v *fqView) FailUnfinished(code string, at time.Time) int {
	v.q.mu.Lock()
	defer v.q.mu.Unlock()
	n := 0
	for _, j := range v.q.jobs {
		if j.owner == v.w && (j.Status == StatusQueued || j.Status == StatusRunning) {
			j.Status, j.ErrorCode, j.owner, j.FinishedAt = StatusFailed, code, "", at
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	dl := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(dl) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func testDispatcher(q Queue, st ObjectStore, ch *fakeCH, workers int) *Dispatcher {
	return &Dispatcher{Queue: q, Runner: newRunner(ch, st, &MemoryAudit{}), Workers: workers,
		PollMin: time.Millisecond, PollMax: 5 * time.Millisecond, HeartbeatEvery: 5 * time.Millisecond}
}

func TestDispatcherClaimsInOrderAndRunsEachJobOnce(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	st := newMemStore()
	q := newFakeQueue()
	var ids []string
	for i := 0; i < 20; i++ {
		ids = append(ids, q.add(tenantA))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d1, d2 := testDispatcher(q.view("w1"), st, ch, 2), testDispatcher(q.view("w2"), st, ch, 2)
	d1.Start(ctx)
	d2.Start(ctx)
	waitFor(t, "all succeeded", func() bool { return q.countStatus(StatusSucceeded) == 20 })
	q.mu.Lock()
	if strings.Join(q.claimLog, ",") != strings.Join(ids, ",") {
		t.Fatalf("claims must be FIFO exactly once each: %v", q.claimLog)
	}
	q.mu.Unlock()
	st.mu.Lock()
	if len(st.puts) != 20 {
		t.Fatalf("each job runs exactly once: %d puts", len(st.puts))
	}
	seen := map[string]bool{}
	for _, k := range st.puts {
		if seen[k] {
			t.Fatalf("object %s written twice", k)
		}
		seen[k] = true
	}
	st.mu.Unlock()
	if j := q.get(ids[0]); j.Rows != 3 || j.ObjectKey != ObjectKey(tenantA, ids[0], "ndjson") || j.Bytes == 0 {
		t.Fatalf("counters: %+v", j)
	}
	shutdown(t, d1)
	shutdown(t, d2)
}

func shutdown(t *testing.T, d *Dispatcher) {
	t.Helper()
	ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	d.Shutdown(ctx)
}

func TestDispatcherFailureWritesFailedWithCode(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	g := newGate()
	g.err = errBoom
	close(g.release)
	q := newFakeQueue()
	id := q.add(tenantA)
	d := testDispatcher(q.view("w1"), g, ch, 1)
	d.Start(context.Background())
	waitFor(t, "failed", func() bool { return q.get(id).Status == StatusFailed })
	if j := q.get(id); j.ErrorCode != "export_failed" {
		t.Fatalf("%+v", j)
	}
	shutdown(t, d)
}

func TestDispatcherHeartbeatExtendsLease(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	g := newGate()
	q := newFakeQueue()
	id := q.add(tenantA)
	d := testDispatcher(q.view("w1"), g, ch, 1)
	d.Start(context.Background())
	<-g.started
	l0 := q.get(id).lease
	waitFor(t, "heartbeats while running", func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.hbCalls[id] >= 3
	})
	if !q.get(id).lease.After(l0) {
		t.Fatal("lease must be extended while the job runs")
	}
	close(g.release)
	waitFor(t, "succeeded", func() bool { return q.get(id).Status == StatusSucceeded })
	shutdown(t, d)
}

func TestDispatcherGracefulShutdownWaitsThenStopsClaiming(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	g := newGate()
	q := newFakeQueue()
	id := q.add(tenantA)
	d := testDispatcher(q.view("w1"), g, ch, 1)
	d.Start(context.Background())
	<-g.started
	late := q.add(tenantA)
	done := make(chan struct{})
	go func() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		d.Shutdown(ctx)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("shutdown must wait for the running job")
	case <-time.After(50 * time.Millisecond):
	}
	close(g.release)
	<-done
	if j := q.get(id); j.Status != StatusSucceeded {
		t.Fatalf("running job should finish within the grace period: %+v", j)
	}
	if j := q.get(late); j.Status != StatusQueued || j.owner != "" {
		t.Fatalf("no new job may be claimed after shutdown began (or it must be released): %+v", j)
	}
}

func TestDispatcherShutdownTimeoutFailsRunningJob(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	g := newGate() // never released: the job blocks until cancelled
	q := newFakeQueue()
	id := q.add(tenantA)
	d := testDispatcher(q.view("w1"), g, ch, 1)
	d.Start(context.Background())
	<-g.started
	ctx, c := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer c()
	d.Shutdown(ctx)
	if j := q.get(id); j.Status != StatusFailed || j.ErrorCode != "shutdown" {
		t.Fatalf("want FAILED shutdown: %+v", j)
	}
}

func TestDispatcherHardStopWritesNothing(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	g := newGate()
	q := newFakeQueue()
	id := q.add(tenantA)
	ctx, cancel := context.WithCancel(context.Background())
	d := testDispatcher(q.view("w1"), g, ch, 1)
	d.Start(ctx)
	<-g.started
	cancel() // crash: no graceful path
	d.Wait()
	if j := q.get(id); j.Status != StatusRunning || j.owner != "w1" {
		t.Fatalf("a crashed dispatcher leaves the row for lease recovery: %+v", j)
	}
}

func TestDispatcherDoesNotRunWhenStartLosesClaim(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	st := newMemStore()
	q := newFakeQueue()
	q.startFail = true
	q.add(tenantA)
	d := testDispatcher(q.view("w1"), st, ch, 1)
	d.Start(context.Background())
	waitFor(t, "claimed", func() bool { q.mu.Lock(); defer q.mu.Unlock(); return len(q.claimLog) == 1 })
	time.Sleep(30 * time.Millisecond)
	shutdown(t, d)
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.puts) != 0 {
		t.Fatal("job whose Start failed must not run")
	}
}

func TestDispatcherWakeSkipsIdleBackoff(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	q := newFakeQueue()
	d := testDispatcher(q.view("w1"), newMemStore(), ch, 1)
	d.PollMin, d.PollMax = time.Hour, time.Hour
	d.Start(context.Background())
	time.Sleep(30 * time.Millisecond) // first claim finds nothing; runner now sleeps ~1h
	id := q.add(tenantA)
	d.Wake()
	waitFor(t, "woken run", func() bool { return q.get(id).Status == StatusSucceeded })
	shutdown(t, d)
}

func TestServerDispatcherModeOnlyEnqueues(t *testing.T) {
	e := newEnv(t, newMemStore(), nil, 1)
	woke := 0
	e.srv.OnEnqueue = func() { woke++ }
	w := e.do("POST", "/v1/exports", "a", specBody)
	if w.Code != 202 || woke != 1 {
		t.Fatalf("%d %s woke=%d", w.Code, w.Body, woke)
	}
	id := decodeMap(t, w)["id"].(string)
	time.Sleep(50 * time.Millisecond)
	j, err := e.jobs.Get(tenantA, id)
	if err != nil || j.Status != StatusQueued {
		t.Fatalf("POST must not run the job locally: %v %+v", err, j)
	}
	if len(e.store.(*memStore).puts) != 0 {
		t.Fatal("nothing may be written by the accepting replica")
	}
}
