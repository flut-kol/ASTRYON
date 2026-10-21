package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// dpopRejects counts rejected DPoP proofs by reason (auth.Config.OnDPoPReject). Reasons come from a
// fixed set inside go-common, so label cardinality is bounded; anything else is folded into "other".
type dpopRejects struct {
	mu sync.Mutex
	n  map[string]int64
}

var knownReasons = map[string]bool{"replay": true, "htm mismatch": true, "htu mismatch": true, "htu missing": true,
	"ath mismatch": true, "jkt mismatch": true, "bad signature": true, "bad header": true, "iat out of window": true,
	"jti": true, "proof count or config": true, "jwk must be a public key": true}

func (d *dpopRejects) Inc(reason string) {
	if !knownReasons[reason] {
		reason = "other"
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.n == nil {
		d.n = map[string]int64{}
	}
	d.n[reason]++
}

func (d *dpopRejects) WritePrometheus(w io.Writer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	fmt.Fprint(w, "# HELP astryon_query_api_dpop_rejected_total Rejected DPoP proofs by reason\n# TYPE astryon_query_api_dpop_rejected_total counter\n")
	reasons := make([]string, 0, len(d.n))
	for r := range d.n {
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	for _, r := range reasons {
		fmt.Fprintf(w, "astryon_query_api_dpop_rejected_total{reason=%q} %d\n", strings.ReplaceAll(r, " ", "_"), d.n[r])
	}
}

// serveMetrics exposes /metrics on QUERY_METRICS_ADDR (unset = off; use a loopback or cluster-internal
// address, the API port stays free of unauthenticated endpoints).
func serveMetrics(d *dpopRejects) {
	addr := os.Getenv("QUERY_METRICS_ADDR")
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		d.WritePrometheus(w)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { log.Printf("query-api metrics: %v", srv.ListenAndServe()) }()
}
