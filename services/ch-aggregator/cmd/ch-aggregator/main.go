// Command ch-aggregator recomputes ClickHouse aggregate tables for a trailing window (DPP-028).
// Env: CH_URL (default http://localhost:8123) CH_DATABASE (astryon) CH_USER CH_PASSWORD
//
//	AGG_LOOKBACK (default 3h; covers late data) AGG_INTERVAL (0 = run once, else repeat).
//
// `ch-aggregator reconcile` reports duplicate rows per batch, aggregate-vs-events drift per hour and empty hours (exit 2 on duplicates or
// aggregate drift; gaps only with AGG_FAIL_ON_GAPS=1).
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/astryon/ch-aggregator/internal/agg"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func dur(k, d string) time.Duration {
	v, err := time.ParseDuration(env(k, d))
	if err != nil {
		log.Fatalf("%s: %v", k, err)
	}
	return v
}

func main() {
	c, err := agg.New(env("CH_URL", "http://localhost:8123"), env("CH_DATABASE", "astryon"), os.Getenv("CH_USER"), os.Getenv("CH_PASSWORD"))
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if len(os.Args) > 1 && os.Args[1] == "reconcile" {
		now := time.Now().UTC()
		w, err := agg.HourWindow(now.Add(-dur("AGG_LOOKBACK", "3h")), now)
		if err != nil {
			log.Fatal(err)
		}
		ctx := context.Background()
		dups, err := c.Reconcile(ctx, w, 100)
		if err != nil {
			log.Fatalf("reconcile: %v", err)
		}
		for _, d := range dups {
			log.Printf("duplicate-rows tenant=%s source=%s batch=%s pv=%d rows=%d unique=%d", d.TenantID, d.SourceID, d.BatchID, d.ProcessingVersion, d.Rows, d.Unique)
		}
		ad, err := c.AggregateDrift(ctx, w, 100)
		if err != nil {
			log.Fatalf("reconcile aggregates: %v", err)
		}
		for _, d := range ad {
			log.Printf("aggregate-drift tenant=%s source=%s pv=%d hour=%s stored=%d actual=%d", d.TenantID, d.SourceID, d.ProcessingVersion, d.Hour, d.Stored, d.Actual)
		}
		gaps, err := c.Gaps(ctx, w, 100)
		if err != nil {
			log.Fatalf("reconcile gaps: %v", err)
		}
		for _, g := range gaps {
			log.Printf("gap (informational) tenant=%s source=%s pv=%d empty-hour=%s", g.TenantID, g.SourceID, g.ProcessingVersion, g.Hour)
		}
		if len(dups)+len(ad) > 0 || (len(gaps) > 0 && os.Getenv("AGG_FAIL_ON_GAPS") == "1") {
			os.Exit(2)
		}
		log.Printf("reconcile: no drift (%d informational gaps)", len(gaps))
		return
	}
	lookback, interval := dur("AGG_LOOKBACK", "3h"), dur("AGG_INTERVAL", "0s")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	for {
		now := time.Now().UTC()
		w, err := agg.HourWindow(now.Add(-lookback), now)
		if err == nil {
			err = c.Recompute(ctx, w, uint64(now.UnixMilli()))
		}
		if err != nil {
			log.Printf("recompute: %v", err)
			if interval == 0 {
				os.Exit(1)
			}
		} else {
			log.Printf("recomputed %s..%s", w.From.Format(time.RFC3339), w.To.Format(time.RFC3339))
		}
		if interval == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
