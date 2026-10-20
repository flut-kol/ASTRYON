package export

import (
	"context"
	"log"
	"math/rand/v2"
	"sync"
	"time"
)

// JobResult is the terminal state a dispatcher writes for a job it ran.
type JobResult struct {
	Status      string
	Rows, Bytes int64
	ObjectKey   string
	ErrorCode   string
}

// Queue is the shared job queue a Dispatcher consumes (PGJobStore implements it; tests use fakes).
type Queue interface {
	// Claim leases one unowned QUEUED job to this worker (it stays QUEUED, not started). ok=false: nothing to claim.
	Claim(ctx context.Context) (Job, bool, error)
	// Start moves a claimed job to RUNNING; ok=false means the claim was lost and the job must not run.
	Start(ctx context.Context, j Job) (bool, error)
	// Finish writes the terminal state if this worker still owns the RUNNING job; ok=false: lease lost, ignored.
	Finish(ctx context.Context, j Job, r JobResult) (bool, error)
	// Release returns a claimed-but-not-started job to the queue.
	Release(ctx context.Context, j Job) error
	// Heartbeat extends the lease of every job this worker owns.
	Heartbeat(ctx context.Context) (int, error)
	// RecoverJobs handles expired leases: re-queue (not started, attempts left) or FAIL (worker_lost / max_attempts).
	RecoverJobs(ctx context.Context) (RecoverStats, error)
	// FailUnfinished fails this worker's remaining non-terminal jobs.
	FailUnfinished(code string, at time.Time) int
}

// Dispatcher runs queued exports on this replica: Workers concurrent runners claim jobs from the shared queue,
// run them with Runner, heartbeat the leases and write the result. Start it once; stop with Shutdown (graceful)
// or by cancelling the Start context (hard stop, simulates a crash: nothing is written, leases expire and
// another replica's recovery decides).
type Dispatcher struct {
	Queue          Queue
	Runner         *Runner
	Audit          AuditSink     // used when Runner.Audit is nil
	Workers        int           // concurrent runners, default 2
	JobTimeout     time.Duration // default 1h
	PollMin        time.Duration // idle poll start, default 200ms
	PollMax        time.Duration // idle poll cap, default 5s (jittered +-50%)
	HeartbeatEvery time.Duration // default 20s (use lease/3)
	RecoverEvery   time.Duration // default 30s
	OpTimeout      time.Duration // bound of one queue operation (claim, heartbeat, recover), default 15s
	FinishRetry    time.Duration // how long a terminal write is retried while the queue is unreachable, default 2m
	Now            func() time.Time

	once                sync.Once
	base                context.Context
	jobCtx, bgCtx       context.Context
	jobCancel, bgCancel context.CancelFunc
	stop                chan struct{}
	stopOnce            sync.Once
	wake                chan struct{}
	wg, bgWG            sync.WaitGroup
	hbDownSince         time.Time // maintain goroutine only: first failed heartbeat of the current outage
}

// Abandoner is optionally implemented by a Queue: after the dispatcher gives up writing a job's terminal state it
// is no longer heartbeated, so lease expiry lets recovery fail it (worker_lost) instead of keeping it RUNNING forever.
type Abandoner interface{ Abandon(j Job) }

func (d *Dispatcher) init() {
	d.once.Do(func() {
		if d.Workers <= 0 {
			d.Workers = 2
		}
		if d.JobTimeout <= 0 {
			d.JobTimeout = time.Hour
		}
		if d.PollMin <= 0 {
			d.PollMin = 200 * time.Millisecond
		}
		if d.PollMax < d.PollMin {
			d.PollMax = 5 * time.Second
			if d.PollMax < d.PollMin {
				d.PollMax = d.PollMin
			}
		}
		if d.HeartbeatEvery <= 0 {
			d.HeartbeatEvery = 20 * time.Second
		}
		if d.RecoverEvery <= 0 {
			d.RecoverEvery = 30 * time.Second
		}
		if d.OpTimeout <= 0 {
			d.OpTimeout = 15 * time.Second
		}
		if d.FinishRetry <= 0 {
			d.FinishRetry = 2 * time.Minute
		}
		d.stop = make(chan struct{})
		d.wake = make(chan struct{}, d.Workers)
	})
}

func (d *Dispatcher) now() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}

// Wake tells idle runners that a job was enqueued locally (they poll immediately instead of waiting).
func (d *Dispatcher) Wake() {
	d.init()
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// Start launches the runners and the heartbeat/recovery loop. Cancelling ctx is a HARD stop.
func (d *Dispatcher) Start(ctx context.Context) {
	d.init()
	d.base = ctx
	d.jobCtx, d.jobCancel = context.WithCancel(ctx)
	d.bgCtx, d.bgCancel = context.WithCancel(ctx)
	for i := 0; i < d.Workers; i++ {
		d.wg.Add(1)
		go d.loop()
	}
	d.bgWG.Add(1)
	go d.maintain()
}

// Wait blocks until all runners and the maintenance loop have exited.
func (d *Dispatcher) Wait() { d.wg.Wait(); d.bgWG.Wait() }

func (d *Dispatcher) stopping() bool {
	select {
	case <-d.stop:
		return true
	default:
		return d.base.Err() != nil
	}
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int64N(int64(d)+1))
}

// sleep waits for a jittered backoff; false means the dispatcher is stopping.
func (d *Dispatcher) sleep(b time.Duration) (woken bool, alive bool) {
	t := time.NewTimer(jitter(b))
	defer t.Stop()
	select {
	case <-t.C:
		return false, true
	case <-d.wake:
		return true, true
	case <-d.stop:
		return false, false
	case <-d.base.Done():
		return false, false
	}
}

func (d *Dispatcher) loop() {
	defer d.wg.Done()
	backoff := d.PollMin
	for !d.stopping() {
		cctx, ccancel := context.WithTimeout(d.base, d.OpTimeout)
		j, ok, err := d.Queue.Claim(cctx)
		ccancel()
		if err != nil && d.base.Err() == nil {
			log.Printf("dispatcher: claim: %v", err)
		}
		if err == nil && ok {
			backoff = d.PollMin
			d.execute(j)
			continue
		}
		woken, alive := d.sleep(backoff)
		if !alive {
			return
		}
		if woken {
			backoff = d.PollMin
		} else if backoff *= 2; backoff > d.PollMax {
			backoff = d.PollMax
		}
	}
}

func (d *Dispatcher) execute(j Job) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(d.base), 15*time.Second)
	if d.stopping() { // claimed while stopping: do not start it, give it back
		_ = d.Queue.Release(ctx, j)
		cancel()
		return
	}
	cancel()
	sctx, scancel := context.WithTimeout(d.base, 15*time.Second)
	started, err := d.Queue.Start(sctx, j)
	scancel()
	if err != nil || !started {
		if err != nil {
			log.Printf("dispatcher: start %s: %v", j.ID, err)
			if d.base.Err() == nil { // after a hard stop nothing is written: lease expiry re-queues the claim
				rctx, rcancel := context.WithTimeout(d.base, 15*time.Second)
				_ = d.Queue.Release(rctx, j) // if this fails too, lease expiry re-queues it
				rcancel()
			}
		}
		return
	}
	jctx, jcancel := context.WithTimeout(d.jobCtx, d.JobTimeout)
	defer jcancel()
	r := *d.Runner
	r.Quota = nil // reserved at POST
	if r.Audit == nil {
		r.Audit = d.Audit
	}
	// The tenant comes from the claimed row (written from the verified principal at POST), never from a request.
	res, err := r.runNoPresign(jctx, Principal{TenantID: j.TenantID, Subject: j.Subject}, j.ID, j.Spec)
	out := JobResult{Status: StatusSucceeded, Rows: res.Rows, Bytes: res.Bytes, ObjectKey: res.Key}
	if err != nil {
		if d.jobCtx.Err() != nil {
			return // stopping: Shutdown fails it ("shutdown"), or after a hard stop recovery decides
		}
		out = JobResult{Status: StatusFailed, Rows: res.Rows, Bytes: res.Bytes, ErrorCode: errCode(err, jctx)}
	}
	d.finish(j, out)
}

// finish writes the terminal state, retrying with jittered exponential backoff while the queue is unreachable
// (PostgreSQL restart) for at most FinishRetry or until the dispatcher is hard-stopped / the shutdown deadline hits.
// The job stays RUNNING and keeps being heartbeated meanwhile, so a short outage resumes with the real result.
// When the budget is spent the job is abandoned: heartbeats stop and recovery fails it as worker_lost.
func (d *Dispatcher) finish(j Job, out JobResult) {
	end := time.Now().Add(d.FinishRetry)
	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		fctx, fcancel := context.WithTimeout(d.base, d.OpTimeout)
		ok, err := d.Queue.Finish(fctx, j, out)
		fcancel()
		if err == nil {
			if !ok {
				log.Printf("dispatcher: finish %s ignored: lease lost", j.ID)
			} else if attempt > 1 {
				log.Printf("dispatcher: finish %s written after %d attempts", j.ID, attempt)
			}
			return
		}
		if d.base.Err() != nil || d.jobCtx.Err() != nil {
			return // hard stop / shutdown deadline: recovery or FailUnfinished decides
		}
		if time.Now().Add(backoff).After(end) {
			log.Printf("dispatcher: finish %s gave up after %s: %v (abandoned; recovery will fail it as worker_lost)", j.ID, d.FinishRetry, err)
			if a, ok := d.Queue.(Abandoner); ok {
				a.Abandon(j)
			}
			return
		}
		if attempt == 1 {
			log.Printf("dispatcher: finish %s: %v (retrying for up to %s)", j.ID, err, d.FinishRetry)
		}
		t := time.NewTimer(jitter(backoff))
		select {
		case <-t.C:
		case <-d.jobCtx.Done():
			t.Stop()
			return
		case <-d.base.Done():
			t.Stop()
			return
		}
		if backoff *= 2; backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
}

// maintain heartbeats the leases and recovers expired ones until the background context ends.
func (d *Dispatcher) maintain() {
	defer d.bgWG.Done()
	hb, rc := time.NewTicker(d.HeartbeatEvery), time.NewTicker(d.RecoverEvery)
	defer hb.Stop()
	defer rc.Stop()
	for {
		select {
		case <-d.bgCtx.Done():
			return
		case <-hb.C:
			d.heartbeat()
		case <-rc.C:
			// Heartbeat first: after a queue outage our own live jobs get their lease back before recovery judges them.
			d.heartbeat()
			rctx, rcancel := context.WithTimeout(d.bgCtx, d.OpTimeout)
			st, err := d.Queue.RecoverJobs(rctx)
			rcancel()
			if err != nil {
				if d.bgCtx.Err() == nil && d.hbDownSince.IsZero() { // during a known outage the heartbeat already reported
					log.Printf("dispatcher: recover: %v", err)
				}
			} else if st.Requeued+st.Failed > 0 {
				log.Printf("dispatcher: recovered requeued=%d failed=%d", st.Requeued, st.Failed)
			}
		}
	}
}

// heartbeat extends our leases; logs only the first failure of an outage and the recovery with its duration.
func (d *Dispatcher) heartbeat() {
	hctx, hcancel := context.WithTimeout(d.bgCtx, d.OpTimeout)
	_, err := d.Queue.Heartbeat(hctx)
	hcancel()
	switch {
	case err != nil && d.bgCtx.Err() == nil:
		if d.hbDownSince.IsZero() {
			d.hbDownSince = time.Now()
			log.Printf("dispatcher: heartbeat: %v (queue unreachable; further failures not logged)", err)
		}
	case err == nil && !d.hbDownSince.IsZero():
		log.Printf("dispatcher: queue reachable again after %s", time.Since(d.hbDownSince).Round(time.Millisecond))
		d.hbDownSince = time.Time{}
	}
}

// Shutdown stops claiming, waits for running jobs until ctx expires, then cancels them, marks this worker's
// unfinished jobs FAILED ("shutdown") and stops heartbeating.
func (d *Dispatcher) Shutdown(ctx context.Context) {
	d.init()
	d.stopOnce.Do(func() { close(d.stop) })
	done := make(chan struct{})
	go func() { d.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		d.jobCancel()
		<-done
	}
	d.jobCancel()
	d.Queue.FailUnfinished("shutdown", d.now())
	d.bgCancel()
	d.bgWG.Wait()
}
