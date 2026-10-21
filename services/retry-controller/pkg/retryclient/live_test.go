package retryclient

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
)

// Live test, skipped unless RETRYCLIENT_LIVE=1 (needs docker). Throwaway postgres:16, all migrations,
// queried as a non-superuser so tenant RLS is enforced; the container is removed afterwards.
func TestLiveScheduleDispatchDeadLetter(t *testing.T) {
	if os.Getenv("RETRYCLIENT_LIVE") != "1" {
		t.Skip("RETRYCLIENT_LIVE not set")
	}
	ctx := context.Background()
	out, err := exec.Command("docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=pw", "-p", "127.0.0.1::5432", "postgres:16").Output()
	if err != nil {
		t.Fatalf("docker run: %v", err)
	}
	cid := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", cid).Run() })
	psql := func(sql string) {
		t.Helper()
		cmd := exec.Command("docker", "exec", "-i", cid, "psql", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-X", "-q")
		cmd.Stdin = strings.NewReader(sql)
		var eb bytes.Buffer
		cmd.Stderr = &eb
		if err := cmd.Run(); err != nil {
			t.Fatalf("psql: %v: %s", err, eb.String())
		}
	}
	for i := 0; i < 60; i++ {
		if exec.Command("docker", "exec", cid, "pg_isready", "-U", "postgres").Run() == nil {
			time.Sleep(time.Second)
			if exec.Command("docker", "exec", cid, "pg_isready", "-U", "postgres").Run() == nil {
				break
			}
		}
		time.Sleep(time.Second)
	}
	files, _ := filepath.Glob("../../../../db/postgres/migrations/*.up.sql")
	if len(files) < 13 {
		t.Fatalf("migrations not found: %v", files)
	}
	for _, f := range files {
		b, _ := os.ReadFile(f)
		psql("BEGIN;\n" + string(b) + "\nCOMMIT;\n")
	}
	psql(`CREATE ROLE app LOGIN PASSWORD 'pw'; GRANT ALL ON ALL TABLES IN SCHEMA public TO app; GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO app;
		GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO app;`)
	portOut, err := exec.Command("docker", "port", cid, "5432/tcp").Output()
	if err != nil {
		t.Fatal(err)
	}
	hp := strings.TrimSpace(strings.Split(string(portOut), "\n")[0])
	hp = hp[strings.LastIndex(hp, ":")+1:]
	db, err := sql.Open("pgx", fmt.Sprintf("postgres://app:pw@127.0.0.1:%s/postgres", hp))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(20)

	admin := func(q string, args ...any) {
		t.Helper()
		tx, _ := db.BeginTx(ctx, nil)
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `SELECT set_config('astryon.platform_admin','on',true)`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	adminStr := func(q string, args ...any) (s string) {
		t.Helper()
		tx, _ := db.BeginTx(ctx, nil)
		defer tx.Rollback()
		tx.ExecContext(ctx, `SELECT set_config('astryon.platform_admin','on',true)`)
		if err := tx.QueryRowContext(ctx, q, args...).Scan(&s); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return
	}
	for i, id := range []string{tenantA, tenantB} {
		admin(`INSERT INTO tenants (id, slug, name) VALUES ($1, $2, 'x')`, id, fmt.Sprintf("tenant-%d", i))
	}
	newJob := func(tenant, worker string, max int) string {
		return adminStr(`INSERT INTO jobs (tenant_id, kind, status, locked_by, locked_until, max_attempts) VALUES ($1,'process_file','RUNNING',$2, now()+interval '1 hour',$3) RETURNING id::text`, tenant, worker, max)
	}
	pol := Policy{BaseDelay: 50 * time.Millisecond, MaxDelay: 100 * time.Millisecond, MaxAttempts: 3}
	ca, err := New(db, Config{TenantID: tenantA, WorkerID: "w1", Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	cb, _ := New(db, Config{TenantID: tenantB, WorkerID: "w1", Policy: pol})

	// Schedule, idempotent replay, lease fencing, tenant isolation.
	job := newJob(tenantA, "w1", 5)
	d, err := ca.Fail(ctx, job, Transientf(errors.New("503")))
	if err != nil || d.Action != ActionRetry || d.Attempt != 1 {
		t.Fatalf("%+v %v", d, err)
	}
	if d, err = ca.Fail(ctx, job, Transientf(errors.New("503"))); err != nil || d.Action != ActionAlready {
		t.Fatalf("replay: %+v %v", d, err)
	}
	if got := adminStr(`SELECT count(*)::text FROM scheduled_retries WHERE job_id=$1`, job); got != "1" {
		t.Fatalf("scheduled rows %s", got)
	}
	if _, err := cb.Fail(ctx, job, Transientf(errors.New("x"))); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("cross-tenant must not see job: %v", err)
	}
	wrong, _ := New(db, Config{TenantID: tenantA, WorkerID: "other", Policy: pol})
	j2 := newJob(tenantA, "w1", 5)
	if _, err := wrong.Fail(ctx, j2, Transientf(errors.New("x"))); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong worker: %v", err)
	}

	// Concurrent dispatchers: due job published exactly once; not-yet-due not published.
	time.Sleep(200 * time.Millisecond)
	var mu sync.Mutex
	pubs := map[string]int{}
	pub := PublisherFunc(func(_ context.Context, m Message) error {
		mu.Lock()
		pubs[m.TenantID+"/"+m.JobID]++
		mu.Unlock()
		return nil
	})
	store := PGDispatchStore{DB: db}
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			(Dispatcher{Store: store, Publisher: pub, Owner: fmt.Sprintf("d%d", i), Batch: 10, Lease: time.Minute}).Once(ctx)
		}(i)
	}
	wg.Wait()
	if pubs[tenantA+"/"+job] != 1 || len(pubs) != 1 {
		t.Fatalf("publishes: %v", pubs)
	}
	if got := adminStr(`SELECT status::text FROM jobs WHERE id=$1`, job); got != "QUEUED" {
		t.Fatalf("status %s", got)
	}

	// Crash between claim and dispatch: lease expires, another dispatcher recovers; zombie is fenced.
	j3 := newJob(tenantA, "w1", 5)
	if _, err := ca.Fail(ctx, j3, Transientf(errors.New("x"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	crashed, err := store.Claim(ctx, "crashy", 10, 300*time.Millisecond)
	if err != nil || len(crashed) != 1 {
		t.Fatalf("claim: %v %v", crashed, err)
	}
	if got, _ := store.Claim(ctx, "other", 10, time.Minute); len(got) != 0 {
		t.Fatal("live lease stolen")
	}
	time.Sleep(400 * time.Millisecond)
	if n, err := (Dispatcher{Store: store, Publisher: pub, Owner: "rescuer", Batch: 10, Lease: time.Minute}).Once(ctx); n != 1 || err != nil {
		t.Fatalf("rescue n=%d err=%v", n, err)
	}
	if err := store.Complete(ctx, crashed[0]); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie complete: %v", err)
	}

	// Max attempts -> dead-letter; permanent -> dead-letter immediately.
	j4 := newJob(tenantA, "w1", 2)
	for i := 1; i <= 2; i++ {
		if d, err := ca.Fail(ctx, j4, Transientf(errors.New("x"))); err != nil || d.Action != ActionRetry || d.Attempt != i {
			t.Fatalf("attempt %d: %+v %v", i, d, err)
		}
		admin(`UPDATE jobs SET status='RUNNING', locked_by='w1' WHERE id=$1`, j4)
	}
	var sink []string
	cs, _ := New(db, Config{TenantID: tenantA, WorkerID: "w1", Policy: pol, Sink: sinkFunc(func(_, id string, _ int, r string) { sink = append(sink, id+":"+r) })})
	if d, err := cs.Fail(ctx, j4, Transientf(errors.New("x"))); err != nil || d.Action != ActionDeadLetter || !strings.Contains(d.Reason, "max attempts") {
		t.Fatalf("exhausted: %+v %v", d, err)
	}
	if got := adminStr(`SELECT status::text FROM jobs WHERE id=$1`, j4); got != "FAILED" || len(sink) != 1 {
		t.Fatalf("status %s sink %v", got, sink)
	}
	if d, _ := cs.Fail(ctx, j4, Transientf(errors.New("x"))); d.Action != ActionDeadAlready {
		t.Fatalf("replay after dead-letter: %+v", d)
	}
	j5 := newJob(tenantA, "w1", 5)
	if d, err := ca.Fail(ctx, j5, errors.New("bad schema")); err != nil || d.Action != ActionDeadLetter {
		t.Fatalf("permanent: %+v %v", d, err)
	}
}

type sinkFunc func(tenantID, jobID string, attempts int, reason string)

func (f sinkFunc) DeadLetter(_ context.Context, t, j string, a int, r string) error {
	f(t, j, a, r)
	return nil
}
