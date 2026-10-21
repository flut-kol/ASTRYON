package export

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// flakyQueue fails Finish with a connection error for the first failN calls (a PostgreSQL restart).
type flakyQueue struct {
	Queue
	mu        sync.Mutex
	failN     int
	calls     int
	abandoned []string
}

func (f *flakyQueue) Finish(ctx context.Context, j Job, r JobResult) (bool, error) {
	f.mu.Lock()
	f.calls++
	fail := f.calls <= f.failN
	f.mu.Unlock()
	if fail {
		return false, errors.New("dial tcp: connection refused")
	}
	return f.Queue.Finish(ctx, j, r)
}
func (f *flakyQueue) Abandon(j Job) {
	f.mu.Lock()
	f.abandoned = append(f.abandoned, j.ID)
	f.mu.Unlock()
}

func openGate() *gateStore {
	g := newGate()
	close(g.release)
	return g
}

func TestDispatcherFinishRetriesThroughQueueOutage(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	q := newFakeQueue()
	id := q.add(tenantA)
	fq := &flakyQueue{Queue: q.view("w1"), failN: 3}
	d := testDispatcher(fq, openGate(), ch, 1)
	d.FinishRetry = 30 * time.Second
	d.Start(context.Background())
	waitFor(t, "succeeded after the outage", func() bool { return q.get(id).Status == StatusSucceeded })
	fq.mu.Lock()
	calls, ab := fq.calls, len(fq.abandoned)
	fq.mu.Unlock()
	if calls != 4 || ab != 0 {
		t.Fatalf("finish calls=%d abandoned=%d, want 4 and 0", calls, ab)
	}
	if n := q.countStatus(StatusSucceeded); n != 1 {
		t.Fatalf("job must finish exactly once, got %d", n)
	}
	shutdown(t, d)
}

func TestDispatcherFinishGivesUpAndAbandons(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	q := newFakeQueue()
	id := q.add(tenantA)
	fq := &flakyQueue{Queue: q.view("w1"), failN: 1 << 30}
	d := testDispatcher(fq, openGate(), ch, 1)
	d.FinishRetry = 700 * time.Millisecond
	d.Start(context.Background())
	waitFor(t, "abandon", func() bool { fq.mu.Lock(); defer fq.mu.Unlock(); return len(fq.abandoned) == 1 })
	if fq.abandoned[0] != id {
		t.Fatalf("abandoned %v want %s", fq.abandoned, id)
	}
	if st := q.get(id).Status; st != StatusRunning {
		t.Fatalf("the unwritten job stays RUNNING for recovery (worker_lost), got %s", st)
	}
	// bounded: the runner is free again and keeps claiming
	id2 := q.add(tenantA)
	fq.mu.Lock()
	fq.failN = 0
	fq.mu.Unlock()
	waitFor(t, "next job served", func() bool { return q.get(id2).Status == StatusSucceeded })
	shutdown(t, d)
}

type blockQueue struct {
	Queue
	hbErr error
	order []string
	mu    sync.Mutex
}

func (b *blockQueue) Heartbeat(ctx context.Context) (int, error) {
	b.mu.Lock()
	b.order = append(b.order, "hb")
	b.mu.Unlock()
	if b.hbErr != nil {
		return 0, b.hbErr
	}
	return b.Queue.Heartbeat(ctx)
}
func (b *blockQueue) RecoverJobs(ctx context.Context) (RecoverStats, error) {
	b.mu.Lock()
	b.order = append(b.order, "rc")
	b.mu.Unlock()
	return b.Queue.RecoverJobs(ctx)
}

func TestDispatcherHeartbeatsBeforeRecovery(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	q := newFakeQueue()
	bq := &blockQueue{Queue: q.view("w1")}
	d := &Dispatcher{Queue: bq, Runner: newRunner(ch, openGate(), &MemoryAudit{}), Workers: 1,
		PollMin: time.Millisecond, PollMax: 5 * time.Millisecond, HeartbeatEvery: time.Hour, RecoverEvery: 10 * time.Millisecond}
	d.Start(context.Background())
	waitFor(t, "a recovery pass", func() bool { bq.mu.Lock(); defer bq.mu.Unlock(); return len(bq.order) >= 2 })
	shutdown(t, d)
	bq.mu.Lock()
	defer bq.mu.Unlock()
	if bq.order[0] != "hb" || bq.order[1] != "rc" {
		t.Fatalf("a recovery pass must be preceded by our own heartbeat: %v", bq.order)
	}
}

func TestErrCodeClickHouse(t *testing.T) {
	ctx := context.Background()
	for _, err := range []error{
		fmt.Errorf("%w: Code: 394. DB::Exception: Query was cancelled", ErrTruncated),
		fmt.Errorf("%w: connection reset by peer", ErrClickHouse),
	} {
		if got := errCode(err, ctx); got != "clickhouse_error" {
			t.Fatalf("%v -> %s", err, got)
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if got := errCode(fmt.Errorf("%w: x", ErrTruncated), cctx); got != "shutdown" {
		t.Fatalf("cancelled ctx must stay shutdown, got %s", got)
	}
}

func TestGuardKeepsClickHouseExceptionText(t *testing.T) {
	g := newGuard(&errAfter{data: []byte("{\"a\":1}\nCode: 394. DB::Exception: Query was cancelled. (QUERY_WAS_CANCELLED)\n")}, false, 10, 1<<20)
	buf := make([]byte, 4096)
	var err error
	for err == nil {
		_, err = g.Read(buf)
	}
	if !errors.Is(err, ErrTruncated) || !contains(err.Error(), "Code: 394") {
		t.Fatalf("%v", err)
	}
}

type errAfter struct {
	data []byte
	done bool
}

func (e *errAfter) Read(p []byte) (int, error) {
	if e.done {
		return 0, errors.New("eof")
	}
	e.done = true
	return copy(p, e.data), nil
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ClickHouse 24.8 ends a KILLed JSONEachRow stream with a valid JSON line {"exception": ...}. Found live: the guard
// counted it as a row and the truncated export was marked SUCCEEDED.
func TestGuardDetectsJSONExceptionLineAndSplitChunks(t *testing.T) {
	line := `{"exception": "Code: 394. DB::Exception: Query was cancelled. (QUERY_WAS_CANCELLED)"}` + "\n"
	for _, split := range []int{0, 5, 13, 20, 60} {
		data := []byte("{\"a\":1}\n{\"a\":2}\n" + line)
		var chunks [][]byte
		if split == 0 {
			chunks = [][]byte{data}
		} else {
			cut := 16 + split
			chunks = [][]byte{data[:cut], data[cut:]}
		}
		g := newGuard(&chunkReader{chunks: chunks}, false, 100, 1<<20)
		buf := make([]byte, 64<<10)
		var err error
		for err == nil {
			_, err = g.Read(buf)
		}
		if !errors.Is(err, ErrTruncated) || !contains(err.Error(), "QUERY_WAS_CANCELLED") || g.Rows != 2 {
			t.Fatalf("split %d: rows=%d err=%v", split, g.Rows, err)
		}
	}
	// CSV: the exception is plain text after the last row
	g := newGuard(&chunkReader{chunks: [][]byte{[]byte("h\n\"a\"\nCode: 394. DB::Exception: Query was cancelled.\n")}}, true, 100, 1<<20)
	var err error
	for err == nil {
		_, err = g.Read(make([]byte, 4096))
	}
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("csv: %v", err)
	}
}

type chunkReader struct {
	chunks [][]byte
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.chunks) == 0 {
		return 0, errors.New("eof")
	}
	n := copy(p, c.chunks[0])
	c.chunks[0] = c.chunks[0][n:]
	if len(c.chunks[0]) == 0 {
		c.chunks = c.chunks[1:]
	}
	return n, nil
}

// startLostReply commits Start but loses the reply (PostgreSQL stopped between COMMIT and the response) and fails Release.
type startLostReply struct {
	flakyQueue
	started chan struct{}
}

func (s *startLostReply) Start(ctx context.Context, j Job) (bool, error) {
	if _, err := s.Queue.Start(ctx, j); err != nil {
		return false, err
	}
	select {
	case s.started <- struct{}{}:
	default:
	}
	return false, errors.New("write tcp: broken pipe")
}
func (s *startLostReply) Release(context.Context, Job) error {
	return errors.New("dial tcp: connection refused")
}

// Found live (e2e postgres_restart_and_outage): Start committed RUNNING but its reply was lost, Release failed too, and the
// job was left owned by this worker with no runner. It must be abandoned so heartbeats stop and recovery fails it as
// worker_lost, instead of its lease being renewed forever.
func TestDispatcherStartErrorAbandonsPossiblyRunningJob(t *testing.T) {
	ch := newFakeCH(ndjsonRows(3), 200)
	defer ch.Close()
	q := newFakeQueue()
	id := q.add(tenantA)
	sq := &startLostReply{flakyQueue: flakyQueue{Queue: q.view("w1")}, started: make(chan struct{}, 1)}
	d := testDispatcher(sq, openGate(), ch, 1)
	d.Start(context.Background())
	<-sq.started
	waitFor(t, "abandon", func() bool { sq.mu.Lock(); defer sq.mu.Unlock(); return len(sq.abandoned) == 1 })
	if sq.abandoned[0] != id {
		t.Fatalf("abandoned %v want %s", sq.abandoned, id)
	}
	shutdown(t, d)
}
