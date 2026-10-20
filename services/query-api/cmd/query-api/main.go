package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/astryon/go-common/pkg/authn"
	"github.com/astryon/go-common/pkg/logging"
	"github.com/astryon/query-api/internal/api"
)

func main() {
	cfg, err := api.LoadConfig(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	logger := logging.New("query-api", os.Getenv("ASTRYON_LOG_LEVEL"))
	var auth api.Authenticator
	if cfg.DevAuth {
		logger.Warn("ASTRYON_DEV_AUTH=1 static dev authenticator enabled; never use outside dev")
		auth = api.DevStaticAuthenticator{Token: cfg.DevToken, Principal: api.Principal{TenantID: cfg.DevTenantID, Scopes: cfg.DevScopes}}
	} else {
		v, err := authn.New(authn.Config{BaseURL: cfg.AASBaseURL, Audience: cfg.AASAudience})
		if err != nil {
			log.Fatal(err)
		}
		auth = api.NewAASJWTAuthenticator(v)
	}
	// PostgreSQL backs only /v1/sources and /v1/jobs; without it those
	// endpoints answer 503 and /healthz reports postgres down, while
	// event queries and aggregates keep working.
	var pg api.ControlPlane = api.DisabledControlPlane{}
	if cfg.PostgresDSN == "" {
		log.Print("DATABASE_URL not set: /v1/sources and /v1/jobs disabled")
	} else {
		p, err := api.OpenPG(cfg.PostgresDSN)
		if err != nil {
			log.Fatal(err)
		}
		pg = p
	}
	ch := api.NewCHClient(cfg.CHURL, cfg.CHDatabase, cfg.CHUser, cfg.CHPassword)
	srv := api.NewServer(cfg, auth, api.CHAnalytics{R: ch}, pg, logger)
	hs := &http.Server{Addr: cfg.Addr, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 40 * time.Second}
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
		_ = hs.Shutdown(ctx)
	}
}
