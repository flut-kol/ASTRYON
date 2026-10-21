// Command quarantine serves the tenant-safe GET /v1/quarantine browser.
//
// ClickHouse: CH_URL, CH_DATABASE, CH_USER, CH_PASSWORD, QUARANTINE_CH_TABLE.
// AAS: AAS_BASE_URL, AAS_ISSUER, AAS_AUDIENCE, AAS_ENV, AAS_ALLOWED_AZP.
// Kafka replay/audit: ASTRYON_KAFKA_* and optional QUARANTINE_REPLAY_TOPIC.
// DEV ONLY: QUARANTINE_DEV_TOKEN and QUARANTINE_DEV_TENANT.
// With neither AAS nor the dev token configured, authentication fails closed.
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

	goauth "github.com/astryon/go-common/auth"
	"github.com/astryon/quarantine/internal/replay"
)

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

type denyAll struct{}

func (denyAll) Authenticate(*http.Request) (replay.BrowserPrincipal, error) {
	return replay.BrowserPrincipal{}, replay.ErrUnauthenticated
}

func main() {
	if err := validateAuthConfiguration(os.Getenv); err != nil {
		log.Fatal(err)
	}
	var authenticator replay.BrowserAuthenticator = denyAll{}
	if os.Getenv("AAS_BASE_URL") != "" {
		cfg, err := dpopConfig(aasConfig())
		if err != nil {
			log.Fatalf("aas dpop: %v", err)
		}
		verifier, err := goauth.NewVerifier(context.Background(), cfg)
		if err != nil {
			log.Fatalf("aas: %v", err)
		}
		authenticator = aasAuthenticator{verifier: verifier}
	}
	if token := os.Getenv("QUARANTINE_DEV_TOKEN"); token != "" {
		authenticator = replay.DevStaticAuthenticator{Token: token, Principal: replay.BrowserPrincipal{
			TenantID: os.Getenv("QUARANTINE_DEV_TENANT"), Subject: "dev-static",
			Scopes: map[string]bool{
				replay.ScopeQuarantineRead:        true,
				replay.ScopeQuarantineReadPayload: true,
				replay.ReplayScope:                true,
				replay.ReplayPublishScope:         true,
			},
		}}
		log.Print("WARNING: dev static authenticator enabled")
	}
	store := &replay.ClickHouseStore{
		URL: env("CH_URL", "http://localhost:8123"), Database: env("CH_DATABASE", "astryon"),
		User: os.Getenv("CH_USER"), Password: os.Getenv("CH_PASSWORD"),
		Table: env("QUARANTINE_CH_TABLE", "processing_errors_distributed"),
	}
	var kafka *replay.KafkaPublisher
	if os.Getenv("ASTRYON_KAFKA_BROKERS") != "" {
		config, err := replay.LoadKafkaConfig(os.Getenv)
		if err != nil {
			log.Fatalf("kafka config: %v", err)
		}
		kafka, err = replay.NewKafkaPublisher(config)
		if err != nil {
			log.Fatalf("kafka: %v", err)
		}
		defer kafka.Close()
	}
	handler := (replay.BrowserHandler{
		Auth: authenticator, Store: store, ReplayPlanner: store, ReplayJobs: kafka, Audit: kafka,
	}).Routes()
	server := &http.Server{Addr: env("LISTEN_ADDR", ":8085"), Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	errorChannel := make(chan error, 1)
	go func() { errorChannel <- server.ListenAndServe() }()
	log.Printf("quarantine browser listening on %s", server.Addr)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errorChannel:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-signals:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

func validateAuthConfiguration(getenv func(string) string) error {
	aas, dev := getenv("AAS_BASE_URL"), getenv("QUARANTINE_DEV_TOKEN")
	if aas != "" && dev != "" {
		return errors.New("AAS_BASE_URL and QUARANTINE_DEV_TOKEN are mutually exclusive")
	}
	if dev != "" {
		if getenv("QUARANTINE_ENABLE_DEV_AUTH") != "true" {
			return errors.New("QUARANTINE_ENABLE_DEV_AUTH=true is required with QUARANTINE_DEV_TOKEN")
		}
		if getenv("QUARANTINE_DEV_TENANT") == "" {
			return errors.New("QUARANTINE_DEV_TENANT is required with QUARANTINE_DEV_TOKEN")
		}
	}
	if aas != "" {
		for _, key := range []string{"AAS_ISSUER", "AAS_AUDIENCE", "AAS_ENV", "AAS_ALLOWED_AZP"} {
			if getenv(key) == "" {
				return fmt.Errorf("%s is required when AAS_BASE_URL is set", key)
			}
		}
	}
	return nil
}
