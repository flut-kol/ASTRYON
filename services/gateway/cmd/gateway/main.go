package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/astryon/gateway/internal/gateway"
)

func main() {
	cfg, err := gateway.LoadConfig(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	var auth gateway.Authenticator = gateway.AASJWTAuthenticator{} // stub: rejects everything
	if cfg.DevAuth {
		log.Println("WARNING: ASTRYON_DEV_AUTH=1 static dev authenticator enabled; never use outside dev")
		auth = gateway.DevStaticAuthenticator{Token: cfg.DevToken, Principal: gateway.Principal{TenantID: cfg.DevTenantID, Scopes: cfg.DevScopes}}
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
	srv := gateway.NewServer(cfg, auth, pub)
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
}
