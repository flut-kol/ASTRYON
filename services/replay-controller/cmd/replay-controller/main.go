// replay-controller: POST /v1/replays and POST /v1/replays/{id}/activate (cutover, needs REPLAY_PG_URL). Jobs go to Kafka replay.jobs.v1 when ASTRYON_KAFKA_BROKERS is set.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	goauth "github.com/astryon/go-common/auth"
	"github.com/astryon/replay-controller/internal/replay"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// `replay-controller reconcile-audit [flags]`: resolve prepared audit events without an outcome (see README).
	if len(os.Args) > 1 && os.Args[1] == "reconcile-audit" {
		os.Exit(replay.RunReconcileCLI(os.Args[2:], os.Getenv, os.Stdout, os.Stderr))
	}
	ch := &replay.CH{URL: os.Getenv("CH_URL"), Database: os.Getenv("CH_DATABASE"),
		User: os.Getenv("CH_USER"), Password: os.Getenv("CH_PASSWORD")}
	if ch.URL == "" {
		log.Fatal("CH_URL is required")
	}
	// Auth: AAS verifier when AAS_BASE_URL is set; DEV-ONLY static token when REPLAY_DEV_TOKEN is set (overrides AAS,
	// logged loudly); deny-all otherwise.
	var auth replay.Authenticator = denyAll{}
	if os.Getenv("AAS_BASE_URL") != "" {
		v, err := goauth.NewVerifier(context.Background(), aasConfig())
		if err != nil {
			log.Fatalf("aas: %v", err)
		}
		auth = aasAuthenticator{v}
	}
	if t := os.Getenv("REPLAY_DEV_TOKEN"); t != "" {
		auth = replay.DevStaticAuthenticator{Token: t, Principal: replay.Principal{
			TenantID: os.Getenv("REPLAY_DEV_TENANT_ID"), Subject: "dev", Scopes: strings.Fields(os.Getenv("REPLAY_DEV_SCOPES"))}}
		log.Print("WARNING: dev static authenticator enabled (REPLAY_DEV_TOKEN), never use outside development")
	}
	// Multi-replica safe allocation needs REPLAY_PG_URL (PostgreSQL, role without BYPASSRLS). Without it the
	// ClickHouse allocator is used, which is only race-free inside ONE process: run a single replica.
	var alloc replay.VersionAllocator = ch
	var activator replay.Activator // cutover needs PostgreSQL
	if u := os.Getenv("REPLAY_PG_URL"); u != "" {
		pool, err := pgxpool.New(context.Background(), u)
		if err != nil {
			log.Fatalf("REPLAY_PG_URL: %v", err)
		}
		defer pool.Close()
		alloc = &replay.PGAllocator{DB: replay.PoolDB{Pool: pool}, CH: ch}
		activator = &replay.PGActivator{DB: replay.PoolDB{Pool: pool}}
	} else {
		log.Print("WARN: REPLAY_PG_URL unset: in-process version allocator, single replica only")
	}
	// ASTRYON_KAFKA_BROKERS set -> jobs go to replay.jobs.v1 (consumed by services/replay-worker); unset = in-memory fake.
	var pub replay.JobPublisher = &replay.MemPublisher{}
	if os.Getenv("ASTRYON_KAFKA_BROKERS") != "" {
		kc, err := replay.LoadKafkaConfig(os.Getenv)
		if err != nil {
			log.Fatal(err)
		}
		kp, err := replay.NewKafkaJobPublisher(kc)
		if err != nil {
			log.Fatal(err)
		}
		defer kp.Close()
		pub = kp
	} else {
		log.Print("WARN: ASTRYON_KAFKA_BROKERS unset: replay jobs are NOT published (in-memory publisher)")
	}
	// Audit: Kafka audit.events.v1 when ASTRYON_KAFKA_BROKERS is set, else JSON lines on stdout (not durable).
	// REPLAY_AUDIT_MODE=required|best_effort (default required with Kafka): required refuses an activation with 503
	// before committing it when the audit event cannot be published.
	var audit replay.AuditSink = &replay.JSONAuditSink{W: os.Stdout}
	kafkaOn := os.Getenv("ASTRYON_KAFKA_BROKERS") != ""
	auditRequired, err := replay.ParseAuditMode(os.Getenv("REPLAY_AUDIT_MODE"), kafkaOn)
	if err != nil {
		log.Fatal(err)
	}
	if kafkaOn {
		kc, err := replay.LoadKafkaConfig(os.Getenv)
		if err != nil {
			log.Fatal(err)
		}
		var to time.Duration
		if v := os.Getenv("REPLAY_AUDIT_TIMEOUT"); v != "" {
			if to, err = time.ParseDuration(v); err != nil || to <= 0 {
				log.Fatal("REPLAY_AUDIT_TIMEOUT must be a positive duration")
			}
		}
		ks, err := replay.NewKafkaAuditSink(kc, to)
		if err != nil {
			log.Fatal(err)
		}
		defer ks.Close()
		audit = ks
	} else {
		log.Print("WARN: ASTRYON_KAFKA_BROKERS unset: audit goes to stdout only (not durable)")
	}
	log.Printf("audit mode: required=%v", auditRequired) // #nosec G706 -- logged value is operator config or sanitized, reviewed 2026-10-21
	pl := &replay.Planner{Alloc: alloc, Counter: ch, Publisher: pub}
	addr := os.Getenv("REPLAY_ADDR")
	if addr == "" {
		addr = ":8087"
	}
	srv := &http.Server{Addr: addr, Handler: (&replay.Handler{Auth: auth, Planner: pl, Activator: activator, Audit: audit, AuditRequired: auditRequired}).Routes(), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
