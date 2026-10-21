// admin-bff: Admin UI backend-for-frontend (OIDC Authorization Code + PKCE against AAS, server-side tokens,
// /api/<service>/ reverse proxy, OIDC back-channel logout at POST /auth/backchannel-logout). Env:
//
//	BFF_ADDR (:8090) BFF_PUBLIC_ORIGIN (https://admin.example) BFF_COOKIE_INSECURE=1 (http dev only)
//	AAS_BASE_URL  BFF_CLIENT_ID (astryon-portal)  BFF_CLIENT_KEY_FILE + BFF_CLIENT_KEY_ID (private_key_jwt)
//	BFF_CLIENT_SECRET (dev only, instead of the key)  BFF_SCOPES ("openid ...", space separated)
//	BFF_UPSTREAMS "gateway=http://..,files=..,query=..,connectors=..,exports=..,quarantine=.."
//	BFF_REDIS_URL (redis://[:password@]host:port[/db] or rediss://; shared session store for several replicas, default: in-memory)
//	BFF_SESSION_KEY (base64, 32 random bytes; required with BFF_REDIS_URL, seals sessions in Redis)  BFF_REDIS_PREFIX (astryon-bff:)
//	BFF_SESSION_MAX_AGE (8h)  BFF_ALLOWED_ORIGINS (comma separated extra Origins for unsafe requests, e.g. http://127.0.0.1:15173)  BFF_AFTER_LOGIN_URL (default "/"; absolute URL when the UI is served from another origin, e.g. the Vite dev server)
//	BFF_AUDIT_PG_URL (optional PostgreSQL audit_log writer) BFF_METRICS_ADDR (optional, bare :port binds loopback)
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/astryon/admin-bff/internal/bff"
	"github.com/astryon/audit"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	origin := strings.TrimSuffix(os.Getenv("BFF_PUBLIC_ORIGIN"), "/")
	up := map[string]string{}
	for _, kv := range strings.Split(os.Getenv("BFF_UPSTREAMS"), ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok && k != "" && v != "" {
			up[k] = v
		}
	}
	age := 8 * time.Hour
	if v := os.Getenv("BFF_SESSION_MAX_AGE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("BFF_SESSION_MAX_AGE: %v", err)
		}
		age = d
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var store bff.Store // nil: in-memory
	if ru := os.Getenv("BFF_REDIS_URL"); ru != "" {
		key, err := bff.ParseSessionKey(os.Getenv("BFF_SESSION_KEY"))
		if err != nil {
			log.Fatalf("BFF_SESSION_KEY: %v", err)
		}
		if store, err = bff.NewRedisStore(ctx, bff.RedisConfig{URL: ru, Key: key, Prefix: os.Getenv("BFF_REDIS_PREFIX")}); err != nil {
			log.Fatal(err)
		}
	}
	auditCfg, err := loadAuditConfig(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	var auditWriter *audit.Writer
	var auditPool *pgxpool.Pool
	if auditCfg.PGURL != "" {
		pc, err := pgxpool.ParseConfig(auditCfg.PGURL)
		if err != nil {
			log.Fatal("BFF_AUDIT_PG_URL: invalid PostgreSQL URL")
		}
		pc.MaxConns = auditCfg.PGMaxConns
		auditPool, err = pgxpool.NewWithConfig(context.Background(), pc)
		if err != nil {
			log.Fatalf("BFF_AUDIT_PG_URL: %v", err)
		}
		pingCtx, pingCancel := context.WithTimeout(context.Background(), auditCfg.WriteTimeout)
		err = auditPool.Ping(pingCtx)
		pingCancel()
		if err != nil {
			auditPool.Close()
			log.Fatalf("BFF_AUDIT_PG_URL: PostgreSQL unavailable: %v", err)
		}
		retries := auditCfg.Retries
		if retries == 0 {
			retries = -1 // libs/audit reserves zero for its default of two retries
		}
		auditWriter = audit.NewWriter(auditPool, audit.Config{QueueSize: auditCfg.QueueSize, BatchSize: auditCfg.BatchSize,
			FlushInterval: auditCfg.FlushInterval, WriteTimeout: auditCfg.WriteTimeout, Retries: retries,
			OnDrop: func(e audit.Event, reason error) {
				log.Printf("admin-bff audit event dropped: action=%s reason_type=%T", e.Action, reason)
			},
		})
	} else {
		log.Print("WARNING: BFF_AUDIT_PG_URL unset: session security events are not persisted")
	}
	s, err := bff.New(ctx, bff.Options{Store: store,
		OIDC: bff.Config{BaseURL: os.Getenv("AAS_BASE_URL"), ClientID: os.Getenv("BFF_CLIENT_ID"), ClientSecret: os.Getenv("BFF_CLIENT_SECRET"),
			KeyFile: os.Getenv("BFF_CLIENT_KEY_FILE"), KeyID: os.Getenv("BFF_CLIENT_KEY_ID"), RedirectURL: origin + "/auth/callback", Scopes: strings.Fields(os.Getenv("BFF_SCOPES"))},
		PublicOrigin: origin, Upstreams: up, CookieSecure: os.Getenv("BFF_COOKIE_INSECURE") != "1", SessionMaxAge: age, AfterLoginPath: os.Getenv("BFF_AFTER_LOGIN_URL"), AllowedOrigins: splitList(os.Getenv("BFF_ALLOWED_ORIGINS")), Audit: auditWriter,
	})
	if err != nil {
		log.Fatal(err)
	}
	addr := os.Getenv("BFF_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	srv := &http.Server{Addr: addr, Handler: s, ReadHeaderTimeout: 5 * time.Second}
	var metricsSrv *http.Server
	if metricsAddr := os.Getenv("BFF_METRICS_ADDR"); metricsAddr != "" {
		if strings.HasPrefix(metricsAddr, ":") {
			metricsAddr = "127.0.0.1" + metricsAddr
		}
		metricsSrv = &http.Server{Addr: metricsAddr, Handler: s.MetricsHandler(), ReadHeaderTimeout: 5 * time.Second}
	}
	errCh := make(chan error, 2)
	go func() { errCh <- srv.ListenAndServe() }()
	if metricsSrv != nil {
		go func() { errCh <- metricsSrv.ListenAndServe() }()
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-stop:
		log.Printf("shutting down on %s", sig)
	case serveErr := <-errCh:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			log.Printf("HTTP server stopped: %v", serveErr)
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("admin-bff shutdown: %v", err)
	}
	if metricsSrv != nil {
		if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
			log.Printf("admin-bff metrics shutdown: %v", err)
		}
	}
	shutdownCancel()
	if auditWriter != nil {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), auditCfg.DrainTimeout)
		if err := auditWriter.Close(drainCtx); err != nil {
			log.Printf("admin-bff audit drain: %v", err)
		}
		drainCancel()
		auditPool.Close()
	}
}

func splitList(v string) []string {
	var out []string
	for _, x := range strings.Split(v, ",") {
		if x = strings.TrimSuffix(strings.TrimSpace(x), "/"); x != "" {
			out = append(out, x)
		}
	}
	return out
}
