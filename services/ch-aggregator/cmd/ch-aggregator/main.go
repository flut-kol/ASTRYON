// Command ch-aggregator recomputes ClickHouse aggregate tables for a trailing window (DPP-028).
// Env: CH_URL (default http://localhost:8123) CH_DATABASE (astryon) CH_USER CH_PASSWORD
//
//	AGG_LOOKBACK (default 3h; covers late data) AGG_INTERVAL (0 = run once, else repeat),
//	AGG_METRICS_FILE (optional Prometheus textfile-collector output, rewritten atomically after each run).
//
// `ch-aggregator reconcile [--snapshot file|-] [--compare file]` reports duplicate rows per batch, aggregate-vs-events
// drift per hour, empty hours, ClickHouse-vs-registry batch counts (REGISTRY_PG_URL, optional; REGISTRY_GRACE default 15m)
// and per-partition checksums (exit 2 on duplicates, aggregate drift, persistent registry mismatch or silent checksum
// change; gaps only with AGG_FAIL_ON_GAPS=1).
package main

import (
	"context"
	"flag"
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
		os.Exit(reconcile(c, os.Args[2:]))
	}
	lookback, interval := dur("AGG_LOOKBACK", "3h"), dur("AGG_INTERVAL", "0s")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	stats := &agg.RunStats{Path: os.Getenv("AGG_METRICS_FILE")}
	for {
		now := time.Now().UTC()
		w, err := agg.HourWindow(now.Add(-lookback), now)
		if err == nil {
			err = c.Recompute(ctx, w, uint64(now.UnixMilli()))
		}
		if merr := stats.Record(time.Now(), err); merr != nil {
			log.Printf("metrics file: %v", merr)
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

func reconcile(c *agg.Client, args []string) int {
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	snap := fs.String("snapshot", "", "write the checksum snapshot (JSON) to this file, or - for stdout")
	cmp := fs.String("compare", "", "diff against a previous snapshot file")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	log.SetFlags(0)
	now := time.Now().UTC()
	lb := dur("AGG_LOOKBACK", "3h")
	w, err := agg.HourWindow(now.Add(-lb), now)
	if err != nil {
		log.Print(err)
		return 1
	}
	cw, err := agg.HourWindow(now.Add(-dur("AGG_CHECKSUM_LOOKBACK", lb.String())), now)
	if err != nil {
		log.Print(err)
		return 1
	}
	c.QueryTimeout = dur("AGG_QUERY_TIMEOUT", "120s")
	ctx := context.Background()
	o := agg.RunOptions{Window: w, ChecksumWindow: cw, Now: now, FailOnGaps: os.Getenv("AGG_FAIL_ON_GAPS") == "1",
		Grace: dur("REGISTRY_GRACE", "15m"), SnapshotPath: *snap, ComparePath: *cmp, Limit: 1000, Out: os.Stderr, SnapOut: os.Stdout}
	if u := os.Getenv("REGISTRY_PG_URL"); u != "" {
		r, err := agg.NewPGRegistry(ctx, u)
		if err != nil {
			log.Printf("registry connect: %v", err)
			return 1
		}
		defer r.Close(ctx)
		o.Registry = r
	}
	return c.Run(ctx, o)
}
