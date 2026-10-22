package main

import (
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"time"

	"github.com/astryon/export-worker/internal/export"
)

// envDuration reads a Go duration ("30s", "15m") or a bare number of seconds; "0" is returned as 0 (callers map it to
// "off"). Invalid input is fatal at startup.
func envDuration(k string, d time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	x, err := time.ParseDuration(v)
	if err != nil || x < 0 {
		log.Fatalf("%s: invalid duration %q", k, v) // #nosec G706 -- logged value is operator config or sanitized, reviewed 2026-10-21
	}
	return x
}

// offIfZero maps the "0 = disabled" setting to the negative "off" value of the export package.
func offIfZero(d time.Duration) time.Duration {
	if d == 0 {
		return -1
	}
	return d
}

// loopbackAddr rejects a debug listen address that is not bound to loopback (pprof must never face the network).
func loopbackAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("debug address %q must be a loopback host (127.0.0.1, ::1 or localhost)", addr)
	}
	return nil
}

// debugHandler serves pprof and the alertable counters (expvar JSON at /debug/vars).
func debugHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/vars", expvar.Handler())
	return mux
}

// startDebug starts the loopback-only debug listener when EXPORT_PPROF_ADDR is set (off by default).
func startDebug(addr string) {
	if addr == "" {
		return
	}
	if err := loopbackAddr(addr); err != nil {
		log.Fatalf("EXPORT_PPROF_ADDR: %v", err)
	}
	expvar.Publish("export_jobs_abandoned_total", expvar.Func(func() any { return export.Counters.JobsAbandoned.Load() }))
	expvar.Publish("export_lease_lost_total", expvar.Func(func() any { return export.Counters.LeaseLost.Load() }))
	expvar.Publish("export_finish_rejected_total", expvar.Func(func() any { return export.Counters.FinishRejected.Load() }))
	expvar.Publish("export_clickhouse_timeouts_total", expvar.Func(func() any { return export.Counters.CHTimeouts.Load() }))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("EXPORT_PPROF_ADDR: %v", err)
	}
	go func() {
		srv := &http.Server{Handler: debugHandler(), ReadHeaderTimeout: 5 * time.Second}
		log.Printf("debug listener (pprof, /debug/vars) on %s", ln.Addr())
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("debug listener: %v", err)
		}
	}()
}

// metricsHandler serves the Prometheus text exposition at /metrics.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		export.Metrics.WritePrometheus(w)
	})
	return mux
}

// startMetrics starts the loopback-only /metrics listener when EXPORT_METRICS_ADDR is set (off by default).
func startMetrics(addr string) {
	if addr == "" {
		return
	}
	if err := loopbackAddr(addr); err != nil {
		log.Fatalf("EXPORT_METRICS_ADDR: %v", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("EXPORT_METRICS_ADDR: %v", err)
	}
	go func() {
		srv := &http.Server{Handler: metricsHandler(), ReadHeaderTimeout: 5 * time.Second}
		log.Printf("metrics listener on %s", ln.Addr())
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics listener: %v", err)
		}
	}()
}
