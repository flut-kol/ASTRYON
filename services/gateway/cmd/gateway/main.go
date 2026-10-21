package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/astryon/audit"
	"github.com/astryon/gateway/internal/gateway"
	goauth "github.com/astryon/go-common/auth"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg, err := gateway.LoadConfig(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	var auth gateway.Authenticator = gateway.AASJWTAuthenticator{} // rejects everything unless AAS_BASE_URL or dev auth is set
	if os.Getenv("AAS_BASE_URL") != "" {
		acfg, err := dpopConfig(aasConfig(os.Getenv), os.Getenv)
		if err != nil {
			log.Fatalf("aas dpop: %v", err)
		}
		v, err := goauth.NewVerifier(context.Background(), acfg)
		if err != nil {
			log.Fatalf("aas: %v", err)
		}
		auth = aasAuthenticator{v}
	}
	if cfg.DevAuth {
		log.Println("WARNING: ASTRYON_DEV_AUTH=1 static dev authenticator enabled; never use outside dev")
		auth = gateway.DevStaticAuthenticator{Token: cfg.DevToken, Principal: gateway.Principal{TenantID: cfg.DevTenantID, ClientID: "gateway-dev", Scopes: cfg.DevScopes}}
	}
	var pub gateway.Publisher
	if cfg.DevFakeBroker {
		log.Println("WARNING: ASTRYON_DEV_FAKE_PUBLISHER=1 in-memory fake publisher; events are NOT durable")
		pub = &gateway.FakePublisher{}
	} else {
		kc, err := gateway.LoadKafkaConfig(os.Getenv)
		if err != nil {
			log.Fatal(err)
		}
		kp, err := gateway.NewKafkaPublisher(kc)
		if err != nil {
			log.Fatal(err)
		}
		defer kp.Close()
		pub = kp
	}
	var auditWriter *audit.Writer
	var auditPool *pgxpool.Pool
	if cfg.AuditPGURL != "" {
		pc, err := pgxpool.ParseConfig(cfg.AuditPGURL)
		if err != nil {
			log.Fatal("ASTRYON_AUDIT_PG_URL: invalid PostgreSQL URL")
		}
		pc.MaxConns = cfg.AuditPGMaxConns
		auditPool, err = pgxpool.NewWithConfig(context.Background(), pc)
		if err != nil {
			log.Fatalf("ASTRYON_AUDIT_PG_URL: %v", err)
		}
		pingCtx, cancel := context.WithTimeout(context.Background(), cfg.AuditWriteTimeout)
		err = auditPool.Ping(pingCtx)
		cancel()
		if err != nil {
			auditPool.Close()
			log.Fatalf("ASTRYON_AUDIT_PG_URL: PostgreSQL unavailable: %v", err)
		}
		writerRetries := cfg.AuditRetries
		if writerRetries == 0 {
			// audit.Config uses a negative value to explicitly select zero retries;
			// its zero value intentionally means the default of two.
			writerRetries = -1
		}
		auditWriter = audit.NewWriter(auditPool, audit.Config{
			QueueSize: cfg.AuditQueueSize, BatchSize: cfg.AuditBatchSize,
			FlushInterval: cfg.AuditFlushInterval, WriteTimeout: cfg.AuditWriteTimeout, Retries: writerRetries,
			OnDrop: func(e audit.Event, reason error) {
				log.Printf("gateway audit event dropped: action=%s reason_type=%T", e.Action, reason)
			},
		})
	} else {
		log.Print("WARNING: ASTRYON_AUDIT_PG_URL unset: tenant security events are not persisted")
	}
	var srv *gateway.Server
	if auditWriter != nil {
		srv = gateway.NewServer(cfg, auth, pub, auditWriter)
	} else {
		srv = gateway.NewServer(cfg, auth, pub)
	}
	hs := &http.Server{Addr: cfg.Addr, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second}

	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errc:
		log.Print(err)
	case <-stop:
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := hs.Shutdown(ctx); err != nil {
			log.Print(err)
		}
	}
	if auditWriter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.AuditDrainTimeout)
		if err := auditWriter.Close(ctx); err != nil {
			log.Printf("gateway audit drain: %v", err)
		}
		cancel()
		auditPool.Close()
	}
}
