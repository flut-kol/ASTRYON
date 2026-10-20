// Command export-worker serves POST /v1/exports and GET /v1/exports/{id} (DPP-049).
// Env: LISTEN_ADDR (:8083) EXPORT_WORKERS (2) CH_URL CH_DATABASE CH_USER CH_PASSWORD
// S3_ENDPOINT S3_BUCKET S3_REGION S3_ACCESS_KEY S3_SECRET_KEY
// EXPORT_MAX_PER_DAY (0 = unlimited) EXPORT_MAX_CONCURRENT_PER_TENANT (2) EXPORT_URL_TTL_SECONDS (900).
// AAS: AAS_BASE_URL AAS_ISSUER AAS_AUDIENCE AAS_ENV AAS_ALLOWED_AZP (scopes astryon:export:write / astryon:export:read).
// DEV ONLY: EXPORT_DEV_TOKEN + EXPORT_DEV_TENANT. Neither configured -> every request is rejected (fail closed).
// EXPORT_PG_URL (non-superuser role without BYPASSRLS; migration db/postgres/migrations/0009) makes jobs and quota
// durable and replica-safe (a per-replica dispatcher runs the queued jobs); EXPORT_WORKER_ID (default hostname-pid), EXPORT_LEASE_SECONDS (60).
// EXPORT_SWEEP_SECONDS (300, 0 = off) and EXPORT_SWEEP_GRACE_SECONDS (60) drive the orphan multipart-upload sweeper (dispatcher mode).
// EXPORT_CH_IDLE_TIMEOUT (30s) fails a job when ClickHouse sends nothing for that long; EXPORT_CH_MAX_DURATION (15m) bounds the
// whole ClickHouse stream (0 = off for both; duration like 30s or bare seconds). EXPORT_FINISH_RETRY (2m) is how long a terminal
// write is retried while PostgreSQL is down before the job is abandoned (log "ALERT export_job_abandoned").
// EXPORT_PPROF_ADDR (off) = loopback-only debug listener with pprof and /debug/vars counters.
// Without EXPORT_PG_URL jobs and quota are in memory (lost on restart, per process); see README.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/astryon/export-worker/internal/export"
	goauth "github.com/astryon/go-common/auth"
	"github.com/jackc/pgx/v5/pgxpool"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			log.Fatalf("%s: invalid value %q", k, v)
		}
		return n
	}
	return d
}

type denyAll struct{}

func (denyAll) Authenticate(*http.Request) (export.Principal, error) {
	return export.Principal{}, export.ErrUnauthenticated
}

func main() {
	var auth export.Authenticator = denyAll{}
	if os.Getenv("AAS_BASE_URL") != "" {
		v, err := goauth.NewVerifier(context.Background(), aasConfig())
		if err != nil {
			log.Fatalf("aas: %v", err)
		}
		auth = aasAuthenticator{v}
	}
	if t := os.Getenv("EXPORT_DEV_TOKEN"); t != "" {
		auth = export.DevStaticAuthenticator{Token: t, Principal: export.Principal{
			TenantID: os.Getenv("EXPORT_DEV_TENANT"), Subject: "dev",
			Scopes: []string{export.ScopeExportWrite, export.ScopeExportRead}}}
		log.Print("WARNING: dev static authenticator enabled")
	}
	s3 := &export.S3Store{Endpoint: env("S3_ENDPOINT", "http://localhost:9000"), Bucket: env("S3_BUCKET", "astryon-exports"),
		Region: env("S3_REGION", "us-east-1"),
		Creds:  export.Creds{AccessKey: os.Getenv("S3_ACCESS_KEY"), SecretKey: os.Getenv("S3_SECRET_KEY")}}
	runner := &export.Runner{
		CH: export.ClickHouse{URL: env("CH_URL", "http://localhost:8123"), Database: env("CH_DATABASE", "astryon"), User: os.Getenv("CH_USER"), Password: os.Getenv("CH_PASSWORD"),
			IdleTimeout: offIfZero(envDuration("EXPORT_CH_IDLE_TIMEOUT", export.DefaultCHIdleTimeout)),
			MaxDuration: offIfZero(envDuration("EXPORT_CH_MAX_DURATION", export.DefaultCHMaxDuration))},
		Store:   s3,
		Presign: s3,
		Audit:   &export.JSONLinesAudit{W: os.Stdout},
		TTL:     time.Duration(envInt("EXPORT_URL_TTL_SECONDS", 900)) * time.Second,
	}
	startDebug(os.Getenv("EXPORT_PPROF_ADDR"))
	maxDay, maxConc := envInt("EXPORT_MAX_PER_DAY", 0), envInt("EXPORT_MAX_CONCURRENT_PER_TENANT", 2)
	s := &export.Server{Auth: auth, Runner: runner, Audit: runner.Audit, Workers: envInt("EXPORT_WORKERS", 2)}
	stopDisp := func(context.Context) {}
	if pgURL := os.Getenv("EXPORT_PG_URL"); pgURL != "" {
		pool, err := pgxpool.New(context.Background(), pgURL)
		if err != nil {
			log.Fatalf("postgres: %v", err)
		}
		defer pool.Close()
		host, _ := os.Hostname()
		lease := time.Duration(envInt("EXPORT_LEASE_SECONDS", 60)) * time.Second
		store := &export.PGJobStore{DB: export.PoolDB{Pool: pool}, Lease: lease,
			WorkerID: env("EXPORT_WORKER_ID", fmt.Sprintf("%s-%d", host, os.Getpid()))}
		s.Jobs = store
		s.Quota = &export.PGQuota{Store: store, MaxPerDay: maxDay, MaxConcurrent: maxConc, Unowned: true}
		bg, cancelBG := context.WithCancel(context.Background())
		if st, err := store.RecoverJobs(bg); err != nil {
			log.Fatalf("startup recovery: %v", err) // fail closed: do not serve with an unreachable job table
		} else if st.Requeued+st.Failed > 0 {
			log.Printf("recovered expired leases: requeued=%d failed=%d", st.Requeued, st.Failed)
		}
		// Dispatcher: every replica claims queued jobs (EXPORT_WORKERS concurrent), heartbeats and recovers leases.
		disp := &export.Dispatcher{Queue: store, Runner: runner, Audit: runner.Audit, Workers: envInt("EXPORT_WORKERS", 2),
			HeartbeatEvery: lease / 3, RecoverEvery: lease / 3, FinishRetry: envDuration("EXPORT_FINISH_RETRY", 2*time.Minute)}
		disp.Start(bg)
		// Orphan multipart uploads of dead workers (kill -9) are aborted by every replica; 0 disables.
		if every := envInt("EXPORT_SWEEP_SECONDS", 300); every > 0 {
			sw := &export.UploadSweeper{Store: s3, Jobs: store, Grace: time.Duration(envInt("EXPORT_SWEEP_GRACE_SECONDS", 60)) * time.Second}
			sw.Run(bg, time.Duration(every)*time.Second, nil)
		}
		s.OnEnqueue = disp.Wake
		stopDisp = func(ctx context.Context) { disp.Shutdown(ctx); cancelBG() }
		log.Printf("jobs and quota: PostgreSQL dispatcher (worker %s, lease %s, %d runners)", store.WorkerID, lease, disp.Workers)
	} else {
		log.Print("WARNING: EXPORT_PG_URL not set: jobs and quota are in memory (lost on restart, not replica-safe)")
		s.Jobs = &export.MemoryJobStore{}
		s.Quota = &export.MemoryQuota{MaxPerDay: maxDay, MaxConcurrent: maxConc}
	}
	srv := &http.Server{Addr: env("LISTEN_ADDR", ":8083"), Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Printf("export-worker listening on %s", srv.Addr)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errc:
		log.Fatal(err)
	case <-sig:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("http shutdown: %v", err)
	}
	stopDisp(ctx)   // dispatcher mode: stop claiming, wait for running jobs until ctx expires, then FAIL them (shutdown)
	s.Shutdown(ctx) // in-memory mode: waits for local jobs until ctx expires, then marks them FAILED
}
