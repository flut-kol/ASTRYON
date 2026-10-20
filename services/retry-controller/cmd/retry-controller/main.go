package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/astryon/retry-controller/internal/retry"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	interval, err := durationEnv("ASTRYON_RETRY_POLL_INTERVAL", 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	batch, err := intEnv("ASTRYON_RETRY_BATCH", 100)
	if err != nil {
		log.Fatal(err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}
	metrics := &retry.Metrics{}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	metricsAddr := os.Getenv("ASTRYON_RETRY_METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":8081"
	}
	httpServer := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server: %v", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("metrics shutdown: %v", err)
		}
	}()
	s := retry.Scheduler{Store: retry.PostgresStore{DB: db}, Interval: interval, Batch: batch, OnError: func(err error) { log.Printf("retry poll: %v", err) }, OnPoll: metrics.Observe}
	if err := s.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	if v := os.Getenv(key); v != "" {
		return time.ParseDuration(v)
	}
	return fallback, nil
}
func intEnv(key string, fallback int) (int, error) {
	if v := os.Getenv(key); v != "" {
		return strconv.Atoi(v)
	}
	return fallback, nil
}
