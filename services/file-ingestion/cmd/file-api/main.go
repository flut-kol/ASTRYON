// Command file-api serves POST /v1/files/uploads and /v1/files/uploads/{id}/complete.
//
// Required: FILE_API_PG_URL, FILE_S3_ENDPOINT, FILE_S3_BUCKET, FILE_S3_ACCESS_KEY, FILE_S3_SECRET_KEY.
// Optional: FILE_S3_REGION, LISTEN_ADDR (:8085), FILE_MAX_BYTES (default 5 GiB), FILE_MAX_PARTS (default 1000),
// FILE_CONTENT_TYPES (comma list, default application/x-ndjson,text/csv).
// Tenant upload quota defaults (override per tenant in tenant_quotas): FILE_MAX_BYTES per file, FILE_QUOTA_OPEN (20) concurrently open
// uploads, FILE_QUOTA_BYTES_PER_DAY (100 GiB) declared bytes in the last 24h.
// FILE_METRICS_ADDR serves /metrics (off when unset). Audit tuning: FILE_AUDIT_QUEUE_SIZE, FILE_AUDIT_BATCH_SIZE,
// FILE_AUDIT_FLUSH_MS, FILE_AUDIT_WRITE_TIMEOUT_MS, FILE_AUDIT_RETRIES and FILE_AUDIT_DRAIN_SECONDS.
// Archive handling is disabled by default; FILE_ARCHIVE_MODE=inspect enables bounded metadata inspection at complete.
// AAS: see aas.go. DEV ONLY: FILE_API_DEV_TOKEN + FILE_API_DEV_TENANT. Without AAS or the dev token every request is rejected.
package main

import (
	"context"
	"crypto/subtle"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/astryon/audit"
	"github.com/astryon/file-ingestion/internal/api"
	"github.com/astryon/file-ingestion/internal/joblist"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/listing"
	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/quota"
	"github.com/astryon/file-ingestion/internal/s3"
	"github.com/astryon/file-ingestion/internal/upload"
	goauth "github.com/astryon/go-common/auth"
	"github.com/jackc/pgx/v5/pgxpool"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func must(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s is required", k)
	}
	return v
}

func num(k string, d int64) int64 {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		log.Fatalf("%s must be a positive integer", k)
	}
	return n
}

type denyAll struct{}

func (denyAll) Authenticate(*http.Request) (api.Principal, error) {
	return api.Principal{}, api.ErrUnauthenticated
}

type devAuth struct{ token, tenant string }

func (d devAuth) Authenticate(r *http.Request) (api.Principal, error) {
	h := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if d.token == "" || d.tenant == "" || subtle.ConstantTimeCompare([]byte(h), []byte(d.token)) != 1 {
		return api.Principal{}, api.ErrUnauthenticated
	}
	return api.Principal{TenantID: d.tenant, ClientID: "file-api-dev", Scopes: []string{api.ScopeUploadsWrite, api.ScopeJobsRead}}, nil
}

func boundedNum(k string, d, max int64, allowZero bool) int64 {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n > max || (!allowZero && n <= 0) || (allowZero && n < 0) {
		log.Fatalf("%s must be an integer between %d and %d", k, map[bool]int64{true: 0, false: 1}[allowZero], max)
	}
	return n
}

func main() {
	var auth api.Authenticator = denyAll{}
	if os.Getenv("AAS_BASE_URL") != "" {
		cfg, err := aasConfig()
		if err != nil {
			log.Fatalf("aas: %v", err)
		}
		v, err := goauth.NewVerifier(context.Background(), cfg)
		if err != nil {
			log.Fatalf("aas: %v", err)
		}
		auth = aasAuthenticator{v}
	}
	if t := os.Getenv("FILE_API_DEV_TOKEN"); t != "" {
		auth = devAuth{t, os.Getenv("FILE_API_DEV_TENANT")}
		log.Print("WARNING: dev static authenticator enabled")
	}
	pool, err := pgxpool.New(context.Background(), must("FILE_API_PG_URL"))
	if err != nil {
		log.Fatalf("FILE_API_PG_URL: %v", err)
	}
	auditRetries := int(boundedNum("FILE_AUDIT_RETRIES", 2, 10, true))
	if auditRetries == 0 {
		auditRetries = -1 // audit.Config uses -1 for an explicit zero; zero selects its default.
	}
	auditWriter := audit.NewWriter(pool, audit.Config{
		QueueSize:     int(boundedNum("FILE_AUDIT_QUEUE_SIZE", 1024, 65536, false)),
		BatchSize:     int(boundedNum("FILE_AUDIT_BATCH_SIZE", 100, 1000, false)),
		FlushInterval: time.Duration(boundedNum("FILE_AUDIT_FLUSH_MS", 500, 60000, false)) * time.Millisecond,
		WriteTimeout:  time.Duration(boundedNum("FILE_AUDIT_WRITE_TIMEOUT_MS", 5000, 60000, false)) * time.Millisecond,
		Retries:       auditRetries,
		OnDrop: func(e audit.Event, reason error) {
			log.Printf("file-api audit event dropped: action=%s reason_type=%T", e.Action, reason)
		},
	})
	bucket := must("FILE_S3_BUCKET")
	c := &s3.Client{Endpoint: must("FILE_S3_ENDPOINT"), Bucket: bucket, Region: os.Getenv("FILE_S3_REGION"),
		Creds: s3.Creds{AccessKey: must("FILE_S3_ACCESS_KEY"), SecretKey: must("FILE_S3_SECRET_KEY")}}
	types := map[string]bool{}
	for _, t := range strings.Split(env("FILE_CONTENT_TYPES", "application/x-ndjson,text/csv"), ",") {
		types[strings.TrimSpace(t)] = true
	}
	archiveMode := env("FILE_ARCHIVE_MODE", "")
	if archiveMode == "inspect" {
		for _, t := range []string{"application/zip", "application/x-tar", "application/tar", "application/gzip", "application/x-gzip"} {
			types[t] = true
		}
	}
	db := jobs.PoolDB{Pool: pool}
	apiJobs := &jobs.Store{DB: db, WorkerID: "file-api"}
	s := &api.Server{Auth: auth, Quota: quota.Checker{Store: quota.PGStore{DB: db}, Defaults: quota.Limits{
		MaxFileBytes: num("FILE_MAX_BYTES", 5<<30), MaxOpen: num("FILE_QUOTA_OPEN", 20), BytesPerDay: num("FILE_QUOTA_BYTES_PER_DAY", 100<<30)}},
		Bucket: bucket, Files: objects.PGStore{DB: db}, Objs: c, Multipart: c, ArchiveMode: archiveMode,
		Jobs: apiJobs, Insights: apiJobs, Lister: listing.PGStore{Pool: pool}, JobReader: joblist.PGStore{Pool: pool}, Audit: auditWriter,
		Uploads: upload.Service{Store: api.MultipartStarter{C: c}, Now: time.Now, MaxBytes: num("FILE_MAX_BYTES", 5<<30),
			MaxParts: int(num("FILE_MAX_PARTS", 1000)), ContentTypes: types}}
	metricsSrv := serveMetrics(s.WritePrometheus)
	srv := &http.Server{Addr: env("LISTEN_ADDR", ":8085"), Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("file-api listening on %s", srv.Addr)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("file-api: %v", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("file-api shutdown: %v", err)
		}
		if metricsSrv != nil {
			_ = metricsSrv.Shutdown(shutdownCtx)
		}
		cancel()
	}
	stop()
	drainCtx, cancel := context.WithTimeout(context.Background(), time.Duration(boundedNum("FILE_AUDIT_DRAIN_SECONDS", 10, 60, false))*time.Second)
	if err := auditWriter.Close(drainCtx); err != nil {
		log.Printf("file-api audit drain: %v", err)
	}
	cancel()
	pool.Close()
}

// serveMetrics exposes /metrics on FILE_METRICS_ADDR (unset = off; bind loopback or a cluster-internal address).
func serveMetrics(write func(io.Writer)) *http.Server {
	addr := os.Getenv("FILE_METRICS_ADDR")
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		write(w)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics: %v", err)
		}
	}()
	return srv
}
