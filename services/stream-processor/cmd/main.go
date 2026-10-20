// Command stream-processor consumes raw.events.v1, validates + normalizes + dedups
// and produces processed.events.v1 / events.quarantine.v1 / events.dlq.v1 (DPP-026/027).
package main

import (
	"context"
	"expvar"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/astryon/stream-processor/internal/processor"
)

func main() {
	cfg, err := processor.LoadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	win := processor.NewWindow(cfg.DedupTTL, cfg.DedupMaxEntries)
	if cfg.WarmStart {
		n, err := processor.WarmWindow(ctx, cfg, win, 3*time.Second)
		if err != nil {
			log.Printf("warm start incomplete (continuing, loss bounded by dedup window): %v", err)
		}
		log.Printf("dedup window warmed from %s: %d keys", cfg.OutputTopic, n)
	}
	k, err := processor.NewKafka(cfg)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	p := processor.NewPipeline(k, k, win, cfg.Options())
	p.M.Publish()
	if cfg.MetricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/debug/vars", expvar.Handler())
			log.Printf("metrics: %v", http.ListenAndServe(cfg.MetricsAddr, mux))
		}()
	}
	log.Printf("stream-processor started %s -> %s ttl=%s max=%d", cfg.InputTopic, cfg.OutputTopic, cfg.DedupTTL, cfg.DedupMaxEntries)
	if err := p.Run(ctx); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
	log.Print("stream-processor stopped cleanly")
}
