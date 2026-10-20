// Command clickhouse-sink consumes processed.events.v1 and inserts into ClickHouse (DPP-008).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/astryon/clickhouse-sink/internal/sink"
)

func main() {
	cfg, err := sink.LoadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	ch, err := sink.NewCHSink(cfg)
	if err != nil {
		log.Fatalf("clickhouse: %v", err)
	}
	k, err := sink.NewKafka(cfg)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	p := sink.NewPipeline(k, ch, k, sink.Options{
		BatchSize: cfg.BatchSize, BatchInterval: cfg.BatchInterval, MaxRetries: cfg.MaxRetries,
		RetryBackoff: cfg.RetryBackoff, RetryBackoffMax: cfg.RetryBackoffMax, ShutdownTimeout: cfg.ShutdownTimeout,
	})
	var ready atomic.Bool
	p.M.Publish()
	if cfg.MetricsAddr != "" {
		go func() {
			log.Printf("metrics: %v", http.ListenAndServe(cfg.MetricsAddr, p.M.Mux(&ready)))
		}()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("clickhouse-sink started topic=%s table=%s batch=%d/%s", cfg.Topic, cfg.CHTable, cfg.BatchSize, cfg.BatchInterval)
	ready.Store(true)
	if err := p.Run(ctx); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
	log.Print("clickhouse-sink stopped cleanly")
}
