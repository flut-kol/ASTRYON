// Command loadgen is the ASTRYON gateway load generator (DPP-009). It posts
// batches to POST /v1/events/batch and reports sent/accepted/quarantined/failed,
// throughput and ack latency percentiles as text and JSON.
//
// Credentials come from the environment only:
//
//	LOADGEN_TOKEN + LOADGEN_TENANT_ID           single identity
//	LOADGEN_TENANTS="<tenant-uuid>=<token>;..."  several identities (for -tenant-skew)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	ec "github.com/astryon/event-contracts"
)

func parseTenants(single, tenantID, multi string) ([]Tenant, error) {
	var ts []Tenant
	if multi != "" {
		for _, p := range strings.Split(multi, ";") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			id, tok, ok := strings.Cut(p, "=")
			if !ok || tok == "" || !ec.IsUUID(strings.TrimSpace(id)) {
				return nil, fmt.Errorf("LOADGEN_TENANTS entries must be <tenant-uuid>=<token>")
			}
			ts = append(ts, Tenant{ID: strings.ToLower(strings.TrimSpace(id)), Token: tok})
		}
	}
	if single != "" {
		if !ec.IsUUID(tenantID) {
			return nil, fmt.Errorf("LOADGEN_TENANT_ID must be a UUID (tenant of LOADGEN_TOKEN)")
		}
		ts = append([]Tenant{{ID: strings.ToLower(tenantID), Token: single}}, ts...)
	}
	if len(ts) == 0 {
		return nil, fmt.Errorf("no credentials: set LOADGEN_TOKEN and LOADGEN_TENANT_ID (or LOADGEN_TENANTS)")
	}
	return ts, nil
}

func main() {
	var cfg RunConfig
	var jsonOut, idsOut string
	flag.StringVar(&cfg.URL, "url", "http://127.0.0.1:8080/v1/events/batch", "gateway batch endpoint")
	flag.Float64Var(&cfg.Rate, "rate", 1000, "target events/s across all workers (0 = unlimited)")
	flag.DurationVar(&cfg.Duration, "duration", 30*time.Second, "test duration")
	flag.IntVar(&cfg.BatchSize, "batch", 50, "events per request (gateway default max 100)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 8, "concurrent workers")
	flag.IntVar(&cfg.Opt.EventSize, "event-size", 1024, "approx serialized event size, bytes")
	flag.Float64Var(&cfg.Opt.TenantSkew, "tenant-skew", 0, "Zipf exponent over tenants (0 = uniform; needs several tenants in LOADGEN_TENANTS)")
	flag.IntVar(&cfg.Opt.Sources, "sources", 4, "distinct source_ids per tenant")
	flag.Float64Var(&cfg.Opt.DupRatio, "dup-ratio", 0, "fraction of events re-sending an earlier event (same event_id and bytes)")
	flag.Float64Var(&cfg.Opt.InvalidRatio, "invalid-ratio", 0, "fraction of deliberately invalid events (expect quarantine)")
	flag.Int64Var(&cfg.Seed, "seed", time.Now().UnixNano(), "random seed")
	flag.DurationVar(&cfg.Timeout, "timeout", 15*time.Second, "per-request timeout")
	flag.StringVar(&jsonOut, "json-out", "", "write JSON report to this file ('-' = stdout)")
	flag.StringVar(&idsOut, "ids-out", "", "write unique accepted event_ids (one per line) to this file")
	var sf scenarioFlags
	flag.StringVar(&sf.profile, "profile", "", "workload profile: "+strings.Join(ProfileNames(), ", ")+" (explicit -batch/-event-size win)")
	flag.Float64Var(&sf.burstRate, "burst-rate", 0, "burst scenario: events/s during the burst (e.g. 120000); enables phases baseline->burst->drain")
	flag.DurationVar(&sf.burstDur, "burst-duration", 30*time.Second, "burst length")
	flag.Float64Var(&sf.baseRate, "baseline-rate", 1000, "events/s before and after the burst (0 = none)")
	flag.DurationVar(&sf.baseDur, "baseline-duration", 15*time.Second, "baseline length before the burst")
	flag.DurationVar(&sf.drainDur, "drain-wait", 120*time.Second, "how long to observe the backlog after the burst")
	flag.StringVar(&sf.promURL, "prom-url", "", "Prometheus base URL for backlog sampling (e.g. http://127.0.0.1:9090)")
	flag.StringVar(&sf.backlogQ, "backlog-query", "", "PromQL returning the backlog (e.g. sum(kafka_consumergroup_lag{group=\"clickhouse-sink\"}))")
	flag.Float64Var(&sf.backlogThreshold, "backlog-threshold", 1000, "backlog at or below this counts as drained")
	flag.StringVar(&sf.queryURL, "query-url", "", "GET URL hammered concurrently with ingest (token: LOADGEN_QUERY_TOKEN)")
	flag.Float64Var(&sf.queryRate, "query-rate", 20, "queries/s (0 = unlimited)")
	flag.IntVar(&sf.queryConc, "query-concurrency", 4, "query workers")
	flag.Float64Var(&sf.ackP95, "slo-ack-p95-ms", 250, "pass limit: ACK p95 (plan s2, LAN)")
	flag.Float64Var(&sf.maxFailed, "slo-max-failed", 0, "pass limit: fraction of sent events not ACKed")
	flag.Float64Var(&sf.minRateFrac, "slo-min-rate-frac", 0.95, "pass limit: achieved/target events/s")
	flag.Float64Var(&sf.queryP95, "slo-query-p95-ms", 0, "pass limit: query p95 (0 = not checked; plan has no fixed number)")
	flag.Float64Var(&sf.maxDrainSec, "slo-max-drain-sec", 0, "pass limit: seconds from burst end to drained (0 = not checked)")
	flag.BoolVar(&sf.enforce, "enforce", false, "exit 3 when a pass criterion fails")
	flag.Parse()
	set := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if sf.profile != "" {
		if err := ApplyProfile(&cfg, sf.profile, set["batch"], set["event-size"]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}

	if cfg.BatchSize < 1 || cfg.Concurrency < 1 || cfg.Duration <= 0 || cfg.Rate < 0 ||
		cfg.Opt.DupRatio < 0 || cfg.Opt.InvalidRatio < 0 || cfg.Opt.DupRatio+cfg.Opt.InvalidRatio > 1 {
		fmt.Fprintln(os.Stderr, "invalid flags: batch/concurrency/duration must be >0, rate>=0, dup+invalid ratios within [0,1]")
		os.Exit(2)
	}
	ts, err := parseTenants(os.Getenv("LOADGEN_TOKEN"), os.Getenv("LOADGEN_TENANT_ID"), os.Getenv("LOADGEN_TENANTS"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	cfg.Tenants = ts
	cfg.TrackIDs = idsOut != ""

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: cfg.Concurrency * 2}}
	if sf.active() {
		os.Exit(runScenario(ctx, cfg, sf, "single", client, jsonOut))
	}
	st, elapsed := Run(ctx, cfg, client)
	rep := st.Report(elapsed)
	rep.Config = map[string]any{"url": cfg.URL, "rate": cfg.Rate, "duration": cfg.Duration.String(), "batch": cfg.BatchSize,
		"concurrency": cfg.Concurrency, "event_size": cfg.Opt.EventSize, "tenants": len(ts), "tenant_skew": cfg.Opt.TenantSkew,
		"dup_ratio": cfg.Opt.DupRatio, "invalid_ratio": cfg.Opt.InvalidRatio, "seed": cfg.Seed}

	fmt.Print(rep.Text())
	if idsOut != "" {
		if err := os.WriteFile(idsOut, []byte(strings.Join(st.IDs(), "\n")+"\n"), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if jsonOut != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if jsonOut == "-" {
			fmt.Println(string(b))
		} else if err := os.WriteFile(jsonOut, append(b, '\n'), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
