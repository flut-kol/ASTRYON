// Command connector-manager runs the REST pull runner: pullstore -> restpull -> Kafka raw topic.
// Config (env only): ASTRYON_DATABASE_URL, ASTRYON_KAFKA_*, ASTRYON_WORKER_ID (optional),
// ASTRYON_CONNECTOR_SYNC_INTERVAL (default 30s), ASTRYON_CONNECTOR_METRICS_ADDR (optional, e.g. :9102).
// Optional Debezium CDC consumer (off unless ASTRYON_CDC_TOPIC): ASTRYON_CDC_{CONNECTOR_ID,TENANT_ID,SOURCE_ID,SCHEMA_ID,PRIMARY_KEY,GROUP,SCHEMA_VERSION,PROCESSING_VERSION}.
// Admin API (off unless ASTRYON_CONNECTOR_ADMIN_ADDR is set, e.g. :8089): auth via ASTRYON_AAS_BASE_URL/_ISSUER/_AUDIENCE/_ENV/_ALLOWED_AZP/_TOKEN_TYPE
// (AAS, scope astryon:connectors:write) or dev-only ASTRYON_CONNECTOR_DEV_TOKEN + ASTRYON_CONNECTOR_DEV_TENANT; neither -> all requests 401.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/astryon/audit"
	"github.com/astryon/connector-manager/internal/adminapi"
	"github.com/astryon/connector-manager/internal/cdc"
	"github.com/astryon/connector-manager/internal/connstore"
	"github.com/astryon/connector-manager/internal/kafkaemit"
	"github.com/astryon/connector-manager/internal/pullstore"
	"github.com/astryon/connector-manager/internal/runner"
	"github.com/astryon/connector-manager/internal/secrets"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("connector-manager: %v", err)
		stop()
		os.Exit(1) // #nosec -- after stop(); no deferred work left that matters
	}
}

func run(ctx context.Context, getenv func(string) string) error {
	dbURL := getenv("ASTRYON_DATABASE_URL")
	if dbURL == "" {
		return errors.New("ASTRYON_DATABASE_URL is required")
	}
	kc, err := kafkaemit.LoadConfig(getenv)
	if err != nil {
		return err
	}
	syncEvery := 30 * time.Second
	if v := getenv("ASTRYON_CONNECTOR_SYNC_INTERVAL"); v != "" {
		if syncEvery, err = time.ParseDuration(v); err != nil || syncEvery <= 0 {
			return errors.New("ASTRYON_CONNECTOR_SYNC_INTERVAL must be a positive duration")
		}
	}
	auditCfg, err := loadAuditConfig(getenv)
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	em, err := kafkaemit.New(kc)
	if err != nil {
		return err
	}
	defer em.Close()
	worker := getenv("ASTRYON_WORKER_ID")
	if worker == "" {
		h, _ := os.Hostname()
		worker = fmt.Sprintf("%s-%d", h, os.Getpid())
	}
	store := &pullstore.Store{DB: pullstore.PoolDB{Pool: pool}, WorkerID: worker}
	r := runner.New(runner.Config{Store: store, Emitter: em, Resolver: secrets.Env{}, HTTP: &http.Client{Timeout: 30 * time.Second}})

	cs, err := loadCDC(getenv)
	if err != nil {
		return err
	}
	if cs != nil {
		stopCDC, err := startCDC(ctx, cs, kc, em, &cdc.PGPositions{Pool: pool, Tenant: cs.Map.Tenant})
		if err != nil {
			return fmt.Errorf("cdc: %w", err)
		}
		defer stopCDC()
	}

	var auditWriter *audit.Writer
	var auditPool *pgxpool.Pool
	if auditCfg.PGURL != "" {
		pc, err := pgxpool.ParseConfig(auditCfg.PGURL)
		if err != nil {
			return errors.New("ASTRYON_AUDIT_PG_URL is invalid")
		}
		pc.MaxConns = auditCfg.PGMaxConns
		auditPool, err = pgxpool.NewWithConfig(ctx, pc)
		if err != nil {
			return fmt.Errorf("audit postgres: %w", err)
		}
		pingCtx, cancel := context.WithTimeout(ctx, auditCfg.WriteTimeout)
		err = auditPool.Ping(pingCtx)
		cancel()
		if err != nil {
			auditPool.Close()
			return fmt.Errorf("audit postgres: %w", err)
		}
		retries := auditCfg.Retries
		if retries == 0 {
			retries = -1 // audit.Config reserves zero for its default of two retries.
		}
		auditWriter = audit.NewWriter(auditPool, audit.Config{QueueSize: auditCfg.QueueSize, BatchSize: auditCfg.BatchSize,
			FlushInterval: auditCfg.FlushInterval, WriteTimeout: auditCfg.WriteTimeout, Retries: retries,
			OnDrop: func(e audit.Event, reason error) {
				log.Printf("connector-manager audit event dropped: action=%s reason_type=%T", e.Action, reason)
			}})
	}

	var admin *http.Server
	if addr := getenv("ASTRYON_CONNECTOR_ADMIN_ADDR"); addr != "" {
		auth, err := newAuthenticator(ctx, getenv)
		if err != nil {
			if auditWriter != nil {
				closeCtx, cancel := context.WithTimeout(context.Background(), auditCfg.DrainTimeout)
				_ = auditWriter.Close(closeCtx)
				cancel()
				auditPool.Close()
			}
			return fmt.Errorf("admin auth: %w", err)
		}
		if auditWriter == nil {
			log.Print("WARNING: ASTRYON_AUDIT_PG_URL unset: connector admin mutations and authorization denials are not persisted")
		}
		cs := &connstore.Store{Pool: pool}
		api := &adminapi.Server{Auth: auth, Store: cs, Sources: cs, Audit: auditWriter}
		admin = &http.Server{Addr: addr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	}

	var srv *http.Server
	if addr := getenv("ASTRYON_CONNECTOR_METRICS_ADDR"); addr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			r.M.WritePrometheus(w)
			writeAuditMetrics(w, auditWriter)
		})
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("metrics server: %v", err)
			}
		}()
	}
	if admin != nil {
		go func() {
			if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("admin server: %v", err)
			}
		}()
	}
	go func() { // keep state rows in step with enabled connectors
		t := time.NewTicker(syncEvery)
		defer t.Stop()
		for {
			if _, err := store.Sync(ctx); err != nil && ctx.Err() == nil {
				log.Printf("connector sync: %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	err = r.Run(ctx) // returns ctx.Err() on SIGTERM; an in-flight page finishes or is retried (events are idempotent)
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if srv != nil {
		_ = srv.Shutdown(sctx)
	}
	if admin != nil {
		_ = admin.Shutdown(sctx)
	}
	if auditWriter != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), auditCfg.DrainTimeout)
		if closeErr := auditWriter.Close(closeCtx); closeErr != nil {
			log.Printf("connector-manager audit drain: %v", closeErr)
		}
		closeCancel()
		auditPool.Close()
	}
	return err
}
