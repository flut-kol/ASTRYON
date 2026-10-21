// Command file-worker processes file jobs: claim -> read the verified object from S3 -> parse NDJSON -> insert into
// ClickHouse (events_normalized / processing_errors) -> checkpoint. Run any number of replicas.
//
// Required: FILE_WORKER_PG_URL, FILE_S3_ENDPOINT, FILE_S3_BUCKET, FILE_S3_ACCESS_KEY, FILE_S3_SECRET_KEY, CH_URL,
// FILE_EVENT_TYPE. Optional: FILE_S3_REGION, CH_DATABASE, CH_USER, CH_PASSWORD, FILE_EVENTS_TABLE, FILE_ERRORS_TABLE,
// FILE_EVENT_TIME_FIELD, FILE_ENTITY_ID_FIELD, FILE_PROCESSING_VERSION (default 1), FILE_BATCH_SIZE (500),
// FILE_MAX_RECORD_BYTES (1 MiB), FILE_RETRY_MODE (local = built-in retry, default; controller = failures go through the
// retry-controller client, with FILE_RETRY_BASE_SECONDS 5, FILE_RETRY_MAX_SECONDS 900, FILE_RETRY_MAX_ATTEMPTS 10), FILE_LEASE_SECONDS (60), FILE_WORKER_ID (default hostname-pid), FILE_METRICS_ADDR (/metrics, off when unset).
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/astryon/file-ingestion/internal/chsink"
	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/retrybridge"
	"github.com/astryon/file-ingestion/internal/s3"
	"github.com/astryon/file-ingestion/internal/worker"
	"github.com/astryon/retry-controller/pkg/retryclient"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func must(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s is required", k)
	}
	return v
}

func num(k string, d int) int {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Fatalf("%s must be a positive integer", k)
	}
	return n
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, must("FILE_WORKER_PG_URL"))
	if err != nil {
		log.Fatalf("FILE_WORKER_PG_URL: %v", err)
	}
	defer pool.Close()
	id := os.Getenv("FILE_WORKER_ID")
	if id == "" {
		host, _ := os.Hostname()
		id = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	lease := time.Duration(num("FILE_LEASE_SECONDS", 60)) * time.Second
	db := jobs.PoolDB{Pool: pool}
	store := &jobs.Store{DB: db, WorkerID: id, Lease: lease}
	bucket := must("FILE_S3_BUCKET")
	c := &s3.Client{Endpoint: must("FILE_S3_ENDPOINT"), Bucket: bucket, Region: os.Getenv("FILE_S3_REGION"),
		Creds: s3.Creds{AccessKey: must("FILE_S3_ACCESS_KEY"), SecretKey: must("FILE_S3_SECRET_KEY")}}
	files := objects.PGStore{DB: db}
	sink, err := chsink.New(chsink.Config{URL: must("CH_URL"), Database: os.Getenv("CH_DATABASE"), User: os.Getenv("CH_USER"), Password: os.Getenv("CH_PASSWORD"),
		EventsTable: os.Getenv("FILE_EVENTS_TABLE"), ErrorsTable: os.Getenv("FILE_ERRORS_TABLE"), Files: files,
		Mapping: chsink.Mapping{EventType: must("FILE_EVENT_TYPE"), EventTimeField: os.Getenv("FILE_EVENT_TIME_FIELD"),
			EntityIDField: os.Getenv("FILE_ENTITY_ID_FIELD"), ProcessingVersion: uint32(num("FILE_PROCESSING_VERSION", 1))}}) // #nosec G115 -- small bounded counter or length, reviewed 2026-10-21
	if err != nil {
		log.Fatal(err)
	}
	var jobStore worker.JobStore = store
	if m := os.Getenv("FILE_RETRY_MODE"); m == "controller" {
		jobStore = retryStore(pool, store, id)
	} else if m != "" && m != "local" {
		log.Fatalf("FILE_RETRY_MODE must be local or controller, got %q", m)
	}
	w := worker.New(worker.Config{Store: jobStore, Source: objects.Source{Store: files, Objs: c, Bucket: bucket}, Sink: sink,
		BatchSize: num("FILE_BATCH_SIZE", 500), MaxRecordBytes: num("FILE_MAX_RECORD_BYTES", 1<<20), Heartbeat: lease / 4, Policies: policies()})
	serveMetrics(w.M.WritePrometheus)
	go func() { // recovery of vanished workers and promotion of due retries; safe on every replica
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			if p, r, f, err := store.Sweep(ctx); err != nil {
				log.Printf("sweep: %v", err)
			} else if p+r+f > 0 {
				log.Printf("sweep: promoted=%d retried=%d failed=%d", p, r, f)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	log.Printf("file-worker %s started", id) // #nosec G706 -- logged value is operator config or sanitized, reviewed 2026-10-21
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

// serveMetrics exposes /metrics on FILE_METRICS_ADDR (unset = off; bind loopback or a cluster-internal address).
func serveMetrics(write func(io.Writer)) {
	addr := os.Getenv("FILE_METRICS_ADDR")
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		write(w)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { log.Printf("metrics: %v", srv.ListenAndServe()) }()
}

// policies reads FILE_CREDENTIALS_POLICY (quarantine|block|allow, default quarantine) and optional
// FILE_CREDENTIALS_POLICY_OVERRIDES ("<tenant_id>/<source_id>=block,<tenant_id>=allow"). Source-type profiles sit in between.
func policies() classify.PolicySet {
	def, err := classify.ParsePolicy(os.Getenv("FILE_CREDENTIALS_POLICY"))
	if err != nil {
		log.Fatalf("FILE_CREDENTIALS_POLICY: %v", err)
	}
	ov, err := classify.ParseOverrides(os.Getenv("FILE_CREDENTIALS_POLICY_OVERRIDES"))
	if err != nil {
		log.Fatalf("FILE_CREDENTIALS_POLICY_OVERRIDES: %v", err)
	}
	return classify.PolicySet{Default: def, Overrides: ov}
}

// retryStore routes Fail through the retry-controller client (FILE_RETRY_MODE=controller). It shares the worker's
// PostgreSQL role, so tenant RLS applies; the tenant of each client is the claimed job's tenant.
func retryStore(pool *pgxpool.Pool, base *jobs.Store, workerID string) *retrybridge.Store {
	sdb := stdlib.OpenDBFromPool(pool)
	pol := retryclient.Policy{BaseDelay: time.Duration(num("FILE_RETRY_BASE_SECONDS", 5)) * time.Second,
		MaxDelay: time.Duration(num("FILE_RETRY_MAX_SECONDS", 900)) * time.Second, Jitter: 0.2, MaxAttempts: num("FILE_RETRY_MAX_ATTEMPTS", 10)}
	return &retrybridge.Store{Inner: base, Ledger: retrybridge.SQLLedger{DB: sdb, WorkerID: workerID},
		NewClient: func(tenant string) (retrybridge.Failer, error) {
			return retryclient.New(sdb, retryclient.Config{TenantID: tenant, WorkerID: workerID, Policy: pol})
		}}
}
