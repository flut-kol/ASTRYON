package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/astryon/quarantine/internal/replay"
)

// metricsListenAddr validates QUARANTINE_METRICS_ADDR: empty = off; a bare port (":9100") binds
// loopback only; an explicit non-loopback host is accepted (cluster-internal scraping) but must be named.
func metricsListenAddr(addr string) (string, error) {
	if addr == "" {
		return "", nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("QUARANTINE_METRICS_ADDR: %w", err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), nil
}

func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		replay.Metrics.WritePrometheus(w)
	})
	return mux
}

// serveMetrics exposes /metrics on QUARANTINE_METRICS_ADDR (unset = off).
func serveMetrics(addr string) {
	addr, err := metricsListenAddr(addr)
	if err != nil {
		log.Fatal(err)
	}
	if addr == "" {
		return
	}
	srv := &http.Server{Addr: addr, Handler: metricsHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { log.Printf("quarantine metrics: %v", srv.ListenAndServe()) }()
}
