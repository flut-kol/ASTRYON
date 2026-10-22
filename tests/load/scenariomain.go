package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// ScenarioReport is the JSON report of a profile/burst/query run. The
// single-phase fields stay in Report for the plain mode.
type ScenarioReport struct {
	Scenario  string         `json:"scenario"`
	Profile   string         `json:"profile,omitempty"`
	Measured  bool           `json:"measured"` // always true when produced by a run; false only in dry-run
	StartedAt string         `json:"started_at"`
	Phases    []PhaseReport  `json:"phases"`
	Query     *QueryReport   `json:"query,omitempty"`
	Backlog   *BacklogReport `json:"backlog,omitempty"`
	Checks    []Check        `json:"checks"`
	Pass      bool           `json:"pass"`
	Config    map[string]any `json:"config"`
}

type scenarioFlags struct {
	profile                       string
	burstRate                     float64
	burstDur, baseDur, drainDur   time.Duration
	baseRate                      float64
	queryURL                      string
	queryRate                     float64
	queryConc                     int
	promURL, backlogQ             string
	backlogThreshold              float64
	maxDrainSec, ackP95, queryP95 float64
	maxFailed, minRateFrac        float64
	enforce                       bool
}

func (f scenarioFlags) active() bool {
	return f.profile != "" || f.burstRate > 0 || f.queryURL != "" || f.enforce
}

func runScenario(ctx context.Context, cfg RunConfig, f scenarioFlags, name string, client *http.Client, jsonOut string) int {
	phases := []Phase{{"steady", cfg.Rate, cfg.Duration}}
	target := cfg.Rate
	if f.burstRate > 0 {
		phases = BurstPhases(f.baseRate, f.baseDur, f.burstRate, f.burstDur, f.drainDur)
		target = f.burstRate
		name = "burst"
	}
	if f.profile != "" && f.burstRate == 0 {
		name = f.profile
	}
	rep := ScenarioReport{Scenario: name, Profile: f.profile, Measured: true, StartedAt: time.Now().UTC().Format(time.RFC3339)}

	runCtx, stopAux := context.WithCancel(ctx)
	defer stopAux()
	var qdone chan QueryReport
	if f.queryURL != "" {
		qdone = make(chan QueryReport, 1)
		qc := QueryConfig{URL: f.queryURL, Token: os.Getenv("LOADGEN_QUERY_TOKEN"), Rate: f.queryRate, Concurrency: f.queryConc, Timeout: cfg.Timeout}
		go func() { qdone <- RunQueries(runCtx, qc, client) }()
	}
	var mon *BacklogMonitor
	var mdone chan struct{}
	if f.promURL != "" && f.backlogQ != "" {
		mon, mdone = NewBacklogMonitor(), make(chan struct{})
		go func() {
			defer close(mdone)
			mon.Run(runCtx, func(c context.Context) (float64, error) { return QueryProm(c, client, f.promURL, f.backlogQ) }, time.Second)
		}()
	}

	marked := false
	rep.Phases = RunPhases(ctx, cfg, phases, client, func(n string) {
		if n == "drain" && mon != nil && !marked {
			mon.MarkBurstEnd()
			marked = true
		}
	})
	if f.burstRate > 0 && mon != nil && !marked {
		mon.MarkBurstEnd()
	}
	// Keep watching the backlog after the load ends, up to drainDur beyond the phases.
	if mon != nil && f.burstRate > 0 {
		deadline := time.Now().Add(f.drainDur)
		for time.Now().Before(deadline) && ctx.Err() == nil {
			if r := mon.Report(f.backlogQ, f.backlogThreshold); r.Drained {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	stopAux()
	if qdone != nil {
		q := <-qdone
		rep.Query = &q
	}
	if mon != nil {
		<-mdone
		b := mon.Report(f.backlogQ, f.backlogThreshold)
		rep.Backlog = &b
	}

	if len(rep.Phases) == 0 {
		fmt.Fprintln(os.Stderr, "interrupted before any phase finished")
		return 1
	}
	mainRep := rep.Phases[0].Report
	for _, p := range rep.Phases {
		if p.Name == "burst" {
			mainRep = p.Report
		}
	}
	rep.Checks = Evaluate(Criteria{AckP95Ms: f.ackP95, MaxFailedFrac: f.maxFailed, MinRateFrac: f.minRateFrac, QueryP95Ms: f.queryP95, MaxDrainSec: f.maxDrainSec},
		mainRep, target, rep.Query, rep.Backlog)
	rep.Pass = AllPass(rep.Checks)
	rep.Config = map[string]any{"url": cfg.URL, "batch": cfg.BatchSize, "concurrency": cfg.Concurrency, "event_size": cfg.Opt.EventSize,
		"tenants": len(cfg.Tenants), "seed": cfg.Seed, "target_events_per_sec": target}

	for _, p := range rep.Phases {
		fmt.Printf("== phase %s (target %.0f ev/s)\n%s", p.Name, p.Target, p.Report.Text())
	}
	if rep.Query != nil {
		fmt.Printf("== queries: %d requests, %d errors, p95=%.1fms\n", rep.Query.Requests, rep.Query.Errors, rep.Query.LatencyMs["p95"])
	}
	for _, c := range rep.Checks {
		fmt.Printf("check %-24s pass=%v value=%.4g limit=%.4g\n", c.Name, c.Pass, c.Value, c.Limit)
	}
	if jsonOut != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if jsonOut == "-" {
			fmt.Println(string(b))
		} else if err := os.WriteFile(jsonOut, append(b, '\n'), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	if f.enforce && !rep.Pass {
		return 3
	}
	return 0
}
