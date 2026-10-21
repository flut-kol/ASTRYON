// Command file-api serves POST /v1/files/uploads and /v1/files/uploads/{id}/complete.
//
// Required: FILE_API_PG_URL, FILE_S3_ENDPOINT, FILE_S3_BUCKET, FILE_S3_ACCESS_KEY, FILE_S3_SECRET_KEY.
// Optional: FILE_S3_REGION, LISTEN_ADDR (:8085), FILE_MAX_BYTES (default 5 GiB), FILE_MAX_PARTS (default 1000),
// FILE_CONTENT_TYPES (comma list, default application/x-ndjson,text/csv).
// Tenant upload quota defaults (override per tenant in tenant_quotas): FILE_MAX_BYTES per file, FILE_QUOTA_OPEN (20) concurrently open
// uploads, FILE_QUOTA_BYTES_PER_DAY (100 GiB) declared bytes in the last 24h.
// FILE_METRICS_ADDR serves /metrics (off when unset). AAS: see aas.go. DEV ONLY: FILE_API_DEV_TOKEN + FILE_API_DEV_TENANT. Without AAS or the dev token every request is rejected.
package main

import (
	"context"
	"crypto/subtle"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/astryon/file-ingestion/internal/api"
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
	return api.Principal{TenantID: d.tenant, Scopes: []string{api.ScopeUploadsWrite}}, nil
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
	bucket := must("FILE_S3_BUCKET")
	c := &s3.Client{Endpoint: must("FILE_S3_ENDPOINT"), Bucket: bucket, Region: os.Getenv("FILE_S3_REGION"),
		Creds: s3.Creds{AccessKey: must("FILE_S3_ACCESS_KEY"), SecretKey: must("FILE_S3_SECRET_KEY")}}
	types := map[string]bool{}
	for _, t := range strings.Split(env("FILE_CONTENT_TYPES", "application/x-ndjson,text/csv"), ",") {
		types[strings.TrimSpace(t)] = true
	}
	db := jobs.PoolDB{Pool: pool}
	apiJobs := &jobs.Store{DB: db, WorkerID: "file-api"}
	s := &api.Server{Auth: auth, Quota: quota.Checker{Store: quota.PGStore{DB: db}, Defaults: quota.Limits{
		MaxFileBytes: num("FILE_MAX_BYTES", 5<<30), MaxOpen: num("FILE_QUOTA_OPEN", 20), BytesPerDay: num("FILE_QUOTA_BYTES_PER_DAY", 100<<30)}},
		Bucket: bucket, Files: objects.PGStore{DB: db}, Objs: c, Multipart: c,
		Jobs: apiJobs, Insights: apiJobs, Lister: listing.PGStore{Pool: pool},
		Uploads: upload.Service{Store: api.MultipartStarter{C: c}, Now: time.Now, MaxBytes: num("FILE_MAX_BYTES", 5<<30),
			MaxParts: int(num("FILE_MAX_PARTS", 1000)), ContentTypes: types}}
	serveMetrics(s.WritePrometheus)
	srv := &http.Server{Addr: env("LISTEN_ADDR", ":8085"), Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("file-api listening on %s", srv.Addr)
	log.Fatal(srv.ListenAndServe())
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
