// Command clickhouse-sink consumes processed.events.v1 and inserts into ClickHouse (DPP-008).
package main

import (
	"context"
	"expvar"
	"log"
	"net/http"
	"os"
	"os/signal"
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
	var intents sink.IntentStore
	if cfg.IntentTable != "" {
		in, err := sink.NewCHIntents(cfg)
		if err != nil {
			log.Fatalf("intents: %v", err)
		}
		intents = in
		log.Printf("durable chunk boundaries enabled (table %s)", cfg.IntentTable)
	}
	p := sink.NewPipeline(k, ch, k, sink.Options{Intents: intents, IntentWait: cfg.IntentWait,
		BatchSize: cfg.BatchSize, BatchInterval: cfg.BatchInterval, MaxRetries: cfg.MaxRetries,
		RetryBackoff: cfg.RetryBackoff, RetryBackoffMax: cfg.RetryBackoffMax, ShutdownTimeout: cfg.ShutdownTimeout,
	})
	p.M.Publish()
	if cfg.MetricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/debug/vars", expvar.Handler())
			mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/plain; version=0.0.4")
				p.M.WritePrometheus(w)
			})
			log.Printf("metrics: %v", http.ListenAndServe(cfg.MetricsAddr, mux))
		}()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("clickhouse-sink started topic=%s table=%s batch=%d/%s", cfg.Topic, cfg.CHTable, cfg.BatchSize, cfg.BatchInterval)
	if err := p.Run(ctx); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
	log.Print("clickhouse-sink stopped cleanly")
}
