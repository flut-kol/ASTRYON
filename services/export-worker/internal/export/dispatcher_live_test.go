package export

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Live dispatcher tests (EXPORT_PG_URL, see pg_live_test.go for the required role and migrations). The data
// path (ClickHouse, object store) is faked; the queue, leases and recovery are the real PostgreSQL ones.

type jobState struct {
	Status, ErrorCode string
	Attempts          int
	Rows              int64
}

func (f *pgFix) state(t *testing.T, id string) jobState {
	t.Helper()
	var s jobState
	f.admin(t, func(tx pgx.Tx) error {
		return tx.QueryRow(context.Background(), `SELECT status, COALESCE(error_code,''), attempts, rows FROM export_jobs WHERE id = $1::uuid`, id).
			Scan(&s.Status, &s.ErrorCode, &s.Attempts, &s.Rows)
	})
	return s
}

func (f *pgFix) waitState(t *testing.T, id string, d time.Duration, ok func(jobState) bool) jobState {
	t.Helper()
	dl := time.Now().Add(d)
	for {
		s := f.state(t, id)
		if ok(s) {
			return s
		}
		if time.Now().After(dl) {
			t.Fatalf("job %s stuck in %+v", id, s)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// slowStore delays each Put so that two dispatchers actually share the work.
type slowStore struct {
	*memStore
	d time.Duration
}

func (s slowStore) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	time.Sleep(s.d)
	return s.memStore.Put(ctx, key, r)
}

func putsOf(m *memStore) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.puts...)
}

func TestPGLiveTwoDispatchersRunEachJobExactlyOnce(t *testing.T) {
	f, s1 := newPGFix(t)
	s2 := f.store(t, "w-two", time.Minute)
	s1.WorkerID = "w-one"
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	st1, st2 := newMemStore(), newMemStore()
	var ids []string
	for i := 0; i < 30; i++ {
		j := liveJob([]string{f.tA, f.tB}[i%2])
		if err := s1.Enqueue(j); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d1 := testDispatcher(s1, slowStore{st1, 15 * time.Millisecond}, ch, 3)
	d2 := testDispatcher(s2, slowStore{st2, 15 * time.Millisecond}, ch, 3)
	for _, d := range []*Dispatcher{d1, d2} {
		d.PollMax = 20 * time.Millisecond
		d.HeartbeatEvery = 200 * time.Millisecond
		d.RecoverEvery = 200 * time.Millisecond
		d.Start(ctx)
	}
	for _, id := range ids {
		f.waitState(t, id, 30*time.Second, func(s jobState) bool { return s.Status == StatusSucceeded })
	}
	seen := map[string]int{}
	p1, p2 := putsOf(st1), putsOf(st2)
	for _, k := range append(append([]string{}, p1...), p2...) {
		seen[k]++
	}
	if len(seen) != 30 {
		t.Fatalf("want 30 distinct objects, got %d", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("object %s written %d times", k, n)
		}
	}
	if len(p1) == 0 || len(p2) == 0 {
		t.Fatalf("both dispatchers should have taken work: %d / %d", len(p1), len(p2))
	}
	for _, id := range ids {
		if s := f.state(t, id); s.Rows != 3 || s.Attempts != 0 || s.ErrorCode != "" {
			t.Fatalf("%s: %+v", id, s)
		}
	}
	t.Logf("dispatcher one ran %d, two ran %d", len(p1), len(p2))
	shutdown(t, d1)
	shutdown(t, d2)
}

// startHook wraps a Queue; Start for the listed ids blocks until the context ends (a claim that never starts).
type startHook struct {
	Queue
	mu    sync.Mutex
	block map[string]bool
}

func (h *startHook) Start(ctx context.Context, j Job) (bool, error) {
	h.mu.Lock()
	b := h.block[j.ID]
	h.mu.Unlock()
	if b {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return h.Queue.Start(ctx, j)
}

func TestPGLiveKilledDispatcherStartedJobFailsClaimedJobRequeued(t *testing.T) {
	f, sA := newPGFix(t)
	sA.WorkerID, sA.Lease = "w-dead", time.Second
	sB := f.store(t, "w-alive", time.Second)
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()

	started, claimedOnly := liveJob(f.tA), liveJob(f.tA)
	if err := sA.Enqueue(started); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := sA.Enqueue(claimedOnly); err != nil {
		t.Fatal(err)
	}
	g := newGate() // the started job blocks inside the store forever
	hook := &startHook{Queue: sA, block: map[string]bool{claimedOnly.ID: true}}
	ctxA, kill := context.WithCancel(context.Background())
	defer kill()
	dA := testDispatcher(hook, g, ch, 2)
	dA.HeartbeatEvery, dA.RecoverEvery = 250*time.Millisecond, time.Hour
	dA.Start(ctxA)
	<-g.started
	f.waitState(t, started.ID, 5*time.Second, func(s jobState) bool { return s.Status == StatusRunning })
	var owner string
	for i := 0; i < 200 && owner == ""; i++ { // the second runner claims asynchronously
		f.admin(t, func(tx pgx.Tx) error {
			return tx.QueryRow(context.Background(), `SELECT COALESCE(locked_by,'') FROM export_jobs WHERE id=$1::uuid`, claimedOnly.ID).Scan(&owner)
		})
		time.Sleep(10 * time.Millisecond)
	}
	if s := f.state(t, claimedOnly.ID); s.Status != StatusQueued || owner != "w-dead" {
		t.Fatalf("claimed-not-started must be QUEUED and owned: %+v owner=%q", s, owner)
	}
	// a live heartbeat keeps both leases beyond the 1s lease for a while
	time.Sleep(1500 * time.Millisecond)
	if s := f.state(t, started.ID); s.Status != StatusRunning {
		t.Fatalf("heartbeat must keep the lease: %+v", s)
	}

	kill() // crash: no graceful shutdown, nothing written
	dA.Wait()

	stB := newMemStore()
	dB := testDispatcher(sB, stB, ch, 2)
	dB.HeartbeatEvery, dB.RecoverEvery = 250*time.Millisecond, 250*time.Millisecond
	dB.Start(context.Background())
	got := f.waitState(t, started.ID, 15*time.Second, func(s jobState) bool { return s.Status == StatusFailed })
	if got.ErrorCode != "worker_lost" || got.Attempts != 0 {
		t.Fatalf("started job: %+v", got)
	}
	got = f.waitState(t, claimedOnly.ID, 15*time.Second, func(s jobState) bool { return s.Status == StatusSucceeded })
	if got.Attempts != 1 || got.Rows != 3 || got.ErrorCode != "" {
		t.Fatalf("claimed job: %+v", got)
	}
	puts := putsOf(stB)
	if len(puts) != 1 || puts[0] != ObjectKey(f.tA, claimedOnly.ID, "ndjson") {
		t.Fatalf("the survivor must run only the re-queued job exactly once: %v", puts)
	}
	shutdown(t, dB)
}

func TestPGLiveMaxAttempts(t *testing.T) {
	f, s := newPGFix(t)
	s.WorkerID = "w-flaky"
	j := liveJob(f.tA)
	if err := s.Enqueue(j); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	expire := func() {
		f.admin(t, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE export_jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1::uuid`, j.ID)
			return err
		})
	}
	for want := 1; want <= 2; want++ {
		if c, ok, err := s.Claim(ctx); err != nil || !ok || c.ID != j.ID {
			t.Fatalf("claim %d: %v %v %+v", want, err, ok, c)
		}
		if st := f.state(t, j.ID); st.Status != StatusQueued {
			t.Fatalf("claim keeps QUEUED: %+v", st)
		}
		expire()
		rs, err := s.RecoverJobs(ctx)
		if err != nil || rs.Requeued != 1 || rs.Failed != 0 {
			t.Fatalf("recover %d: %v %+v", want, err, rs)
		}
		if st := f.state(t, j.ID); st.Status != StatusQueued || st.Attempts != want {
			t.Fatalf("after recover %d: %+v", want, st)
		}
	}
	if _, ok, err := s.Claim(ctx); err != nil || !ok {
		t.Fatalf("third claim: %v %v", err, ok)
	}
	expire()
	rs, err := s.RecoverJobs(ctx)
	if err != nil || rs.Requeued != 0 || rs.Failed != 1 {
		t.Fatalf("third recover: %v %+v", err, rs)
	}
	if st := f.state(t, j.ID); st.Status != StatusFailed || st.ErrorCode != "max_attempts" || st.Attempts != 2 {
		t.Fatalf("exhausted: %+v", st)
	}
	// a Release does not consume an attempt, and Start/Finish are owner-guarded
	j2 := liveJob(f.tA)
	_ = s.Enqueue(j2)
	c, _, _ := s.Claim(ctx)
	if err := s.Release(ctx, c); err != nil {
		t.Fatal(err)
	}
	other := f.store(t, "w-other", time.Minute)
	c2, ok, _ := other.Claim(ctx)
	if !ok || c2.ID != j2.ID {
		t.Fatalf("released job should be claimable: %+v", c2)
	}
	if ok, _ := s.Start(ctx, c); ok {
		t.Fatal("a worker that lost the claim must not start the job")
	}
	if ok, err := other.Start(ctx, c2); !ok || err != nil {
		t.Fatalf("%v %v", ok, err)
	}
	if ok, _ := s.Finish(ctx, c2, JobResult{Status: StatusSucceeded}); ok {
		t.Fatal("non-owner must not finish")
	}
	if ok, err := other.Finish(ctx, c2, JobResult{Status: StatusSucceeded, Rows: 7}); !ok || err != nil {
		t.Fatalf("%v %v", ok, err)
	}
	if st := f.state(t, j2.ID); st.Status != StatusSucceeded || st.Attempts != 0 || st.Rows != 7 {
		t.Fatalf("%+v", st)
	}
}

func TestPGLiveGracefulShutdownFailsRunningAndReleasesIdle(t *testing.T) {
	f, s := newPGFix(t)
	s.WorkerID = "w-graceful"
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	j := liveJob(f.tA)
	_ = s.Enqueue(j)
	g := newGate()
	d := testDispatcher(s, g, ch, 1)
	d.Start(context.Background())
	<-g.started
	late := liveJob(f.tA)
	_ = s.Enqueue(late)
	ctx, c := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer c()
	d.Shutdown(ctx)
	if st := f.state(t, j.ID); st.Status != StatusFailed || st.ErrorCode != "shutdown" {
		t.Fatalf("%+v", st)
	}
	if st := f.state(t, late.ID); st.Status != StatusQueued {
		t.Fatalf("a job not yet claimed stays queued: %+v", st)
	}
}
