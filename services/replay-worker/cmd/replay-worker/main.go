// replay-worker: consumes replay.jobs.v1 and copies the previous processing_version of a source forward to
// the job's new version on processed.events.v1 (see README.md). Env only; no secrets in the repo.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/astryon/replay-worker/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	kc, err := worker.LoadKafkaConfig(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	chURL := os.Getenv("REPLAYW_CH_URL")
	if chURL == "" {
		log.Fatal("REPLAYW_CH_URL is required")
	}
	page, err := strconv.Atoi(env("REPLAYW_PAGE_SIZE", "200"))
	if err != nil || page < 1 || page > 5000 {
		log.Fatal("REPLAYW_PAGE_SIZE must be 1..5000")
	}
	src, err := worker.NewKafkaJobSource(kc, env("REPLAYW_GROUP_ID", "replay-worker"), env("REPLAYW_JOBS_TOPIC", "replay.jobs.v1"))
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()
	sink, err := worker.NewKafkaSink(kc)
	if err != nil {
		log.Fatal(err)
	}
	defer sink.Close()
	w := &worker.Worker{
		Source: src, Sink: sink, OutTopic: env("REPLAYW_OUT_TOPIC", "processed.events.v1"), PageSize: page,
		Reader: &worker.CH{URL: chURL, Database: env("REPLAYW_CH_DATABASE", "astryon"), User: os.Getenv("REPLAYW_CH_USER"),
			Password: os.Getenv("REPLAYW_CH_PASSWORD"), Table: os.Getenv("REPLAYW_CH_TABLE")},
	}
	// REPLAYW_PG_URL: PostgreSQL role WITHOUT BYPASSRLS (replay_requests status). Unset = no status tracking.
	if u := os.Getenv("REPLAYW_PG_URL"); u != "" {
		pool, err := pgxpool.New(context.Background(), u)
		if err != nil {
			log.Fatalf("REPLAYW_PG_URL: %v", err)
		}
		defer pool.Close()
		lease := time.Duration(0) // default 1h; must exceed the longest job, but bounds how long a crashed worker's job stays stuck
		if v := os.Getenv("REPLAYW_RUNNING_LEASE"); v != "" {
			if lease, err = time.ParseDuration(v); err != nil || lease <= 0 {
				log.Fatalf("REPLAYW_RUNNING_LEASE %q: want a positive duration like 15m", v)
			}
		}
		w.Status = &worker.PGStatus{DB: worker.PoolDB{Pool: pool}, RunningLease: lease}
	} else {
		log.Print("WARN: REPLAYW_PG_URL unset: replay_requests status is NOT updated")
	}
	// REPLAYW_METRICS_ADDR (off when unset): GET /metrics; a bare ":9108" binds 127.0.0.1 only, set a host to expose it.
	w.Metrics = &worker.Metrics{}
	worker.ServeMetrics(os.Getenv("REPLAYW_METRICS_ADDR"), w.Metrics.WritePrometheus, log.Printf)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := w.Run(ctx); err != nil {
		log.Printf("replay-worker stopped: %v", err)
		stop()
		os.Exit(1)
	}
}
