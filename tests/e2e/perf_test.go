package e2e

// First performance measurement of the real chain (DPP-010/030):
// loadgen -> gateway -> raw.events.v1 -> stream-processor -> processed.events.v1
// -> clickhouse-sink -> ClickHouse, with query-api latency measured afterwards.
//
// Gated on ASTRYON_E2E=1 AND ASTRYON_E2E_PERF=1. Knobs:
//
//	ASTRYON_E2E_PERF_RATES      "2000,5000,10000"  target ev/s per step ("0" = unlimited)
//	ASTRYON_E2E_PERF_DURATION   "30s"
//	ASTRYON_E2E_PERF_BATCH      100
//	ASTRYON_E2E_PERF_CONC       16
//	ASTRYON_E2E_PERF_OUT        path of the JSON result file
//	ASTRYON_E2E_CONTAINERS      comma list of docker containers to sample (docker stats)

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type pct struct{ P50, P95, P99, Max float64 }

func pcts(ms []float64) pct {
	if len(ms) == 0 {
		return pct{}
	}
	s := append([]float64{}, ms...)
	sort.Float64s(s)
	at := func(p float64) float64 {
		i := int(p/100*float64(len(s))+0.999999) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(s) {
			i = len(s) - 1
		}
		return s[i]
	}
	return pct{at(50), at(95), at(99), s[len(s)-1]}
}

type stepResult struct {
	TargetRate     float64            `json:"target_rate"`
	Loadgen        map[string]any     `json:"loadgen"`
	UniqueAccepted int64              `json:"unique_accepted"`
	RowsInCH       int64              `json:"rows_in_clickhouse"`
	DrainSec       float64            `json:"drain_sec_after_load_end"`
	EndToEndPerSec float64            `json:"end_to_end_events_per_sec"`
	ProbeMs        pct                `json:"probe_e2e_latency_ms"`
	ProbeN         int                `json:"probe_samples"`
	ProbeTimeouts  int                `json:"probe_timeouts"`
	LagProcMax     int64              `json:"max_group_lag_stream_processor"`
	LagSinkMax     int64              `json:"max_group_lag_clickhouse_sink"`
	LagProcP95     int64              `json:"p95_group_lag_stream_processor"`
	LagSinkP95     int64              `json:"p95_group_lag_clickhouse_sink"`
	FreshnessMs    pct                `json:"freshness_ms_sampled"`
	DedupEntries   int64              `json:"dedup_state_entries_end"`
	DedupBytes     int64              `json:"dedup_state_bytes_approx_end"`
	ProcDuplicates int64              `json:"processor_duplicates_dropped"`
	ProcQuarantine int64              `json:"processor_quarantined"`
	PeakRSSMB      map[string]float64 `json:"peak_rss_mb"`
	PeakCPUPct     map[string]float64 `json:"peak_cpu_pct"`
	Containers     []string           `json:"docker_stats_midrun,omitempty"`
}

type perfReport struct {
	Date     string            `json:"date"`
	Host     map[string]string `json:"host"`
	Config   map[string]any    `json:"config"`
	LoadAvg0 string            `json:"loadavg_start"`
	LoadAvg1 string            `json:"loadavg_end"`
	Steps    []stepResult      `json:"steps"`
	Query    map[string]pct    `json:"query_api_latency_ms"`
	QueryN   map[string]int    `json:"query_api_samples"`
}

func sh(name string, args ...string) string {
	b, _ := exec.Command(name, args...).Output()
	return strings.TrimSpace(string(b))
}

func TestPerfReport(t *testing.T) {
	if os.Getenv("ASTRYON_E2E_PERF") != "1" {
		t.Skip("set ASTRYON_E2E_PERF=1 (with ASTRYON_E2E=1) to run the performance measurement")
	}
	e := getStack(t)
	dur := getenv("ASTRYON_E2E_PERF_DURATION", "30s")
	batch := getenv("ASTRYON_E2E_PERF_BATCH", "100")
	conc := getenv("ASTRYON_E2E_PERF_CONC", "16")
	rates := strings.Split(getenv("ASTRYON_E2E_PERF_RATES", "2000,5000,10000"), ",")
	rep := perfReport{
		Date: time.Now().Format(time.RFC3339),
		Host: map[string]string{
			"cpu": sh("sysctl", "-n", "machdep.cpu.brand_string"), "ncpu": sh("sysctl", "-n", "hw.ncpu"),
			"mem_bytes": sh("sysctl", "-n", "hw.memsize"), "os": sh("uname", "-srm"), "go": sh("go", "version"),
			"docker": sh("docker", "info", "--format", "{{.NCPU}} cpus / {{.MemTotal}} bytes"),
		},
		Config:   map[string]any{"duration": dur, "batch": batch, "concurrency": conc, "dup_ratio": 0.02, "invalid_ratio": 0.01, "event_size": 1024},
		LoadAvg0: sh("sysctl", "-n", "vm.loadavg"),
	}

	for _, rs := range rates {
		rate, _ := strconv.ParseFloat(strings.TrimSpace(rs), 64)
		// fresh dev tenant + gateway per step so counts are exact
		ts := e.startTenant(t, map[string]string{"ASTRYON_GATEWAY_MAX_BATCH_EVENTS": "1000"})
		rep.Steps = append(rep.Steps, e.perfStep(t, ts, rate, dur, batch, conc))
	}
	rep.LoadAvg1 = sh("sysctl", "-n", "vm.loadavg")

	// query-api latency on the data just written (last tenant, hundreds of thousands of rows)
	last := e.lastTenant
	rep.Query, rep.QueryN = map[string]pct{}, map[string]int{}
	measure := func(name string, n int, f func()) {
		var ms []float64
		for i := 0; i < n; i++ {
			t0 := time.Now()
			f()
			ms = append(ms, float64(time.Since(t0).Microseconds())/1000)
		}
		rep.Query[name], rep.QueryN[name] = pcts(ms), n
	}
	src := e.chQuery(t, fmt.Sprintf("SELECT any(source_id) FROM events_normalized_local WHERE tenant_id='%s' AND source_id != '99999999-9999-4999-8999-999999999999' FORMAT TSV", last.tenant))
	now := time.Now().UTC()
	tr := map[string]any{"from": now.Add(-time.Hour).Format(time.RFC3339), "to": now.Add(10 * time.Minute).Format(time.RFC3339)}
	measure("events_limit100", 100, func() {
		var o map[string]any
		httpJSON(t, "POST", last.qa+"/v1/query/events", last.token, map[string]any{"time_range": tr, "limit": 100}, &o)
	})
	measure("events_filtered_source_limit100", 100, func() {
		var o map[string]any
		httpJSON(t, "POST", last.qa+"/v1/query/events", last.token, map[string]any{"time_range": tr, "limit": 100, "filters": map[string]any{"source_ids": []string{src}}}, &o)
	})
	measure("aggregate_count_by_source", 50, func() {
		var o map[string]any
		httpJSON(t, "POST", last.qa+"/v1/query/aggregate", last.token, map[string]any{"time_range": tr, "group_by": []string{"source_id"}, "metrics": []string{"events"}}, &o)
	})
	measure("aggregate_hourly_series", 50, func() {
		var o map[string]any
		httpJSON(t, "POST", last.qa+"/v1/query/aggregate", last.token, map[string]any{"time_range": tr, "group_by": []string{"hour"}, "metrics": []string{"events", "entities"}}, &o)
	})

	b, _ := json.MarshalIndent(rep, "", "  ")
	out := getenv("ASTRYON_E2E_PERF_OUT", "")
	if out != "" {
		_ = os.WriteFile(out, b, 0o644)
	}
	t.Logf("perf report:\n%s", b)
}

func (e *env) perfStep(t *testing.T, ts *tenantSvc, rate float64, dur, batch, conc string) stepResult {
	e.lastTenant = ts
	res := stepResult{TargetRate: rate, PeakRSSMB: map[string]float64{}, PeakCPUPct: map[string]float64{}}
	jsonOut := e.dir + "/loadgen-" + ts.tenant[:8] + ".json"
	pg, sg := "e2e-proc-"+e.runID, "e2e-sink-"+e.runID
	cl := e.client(t)
	var mu sync.Mutex
	var lagP, lagS []int64
	var fresh []float64
	pids := map[string]int{"stream-processor": e.proc.cmd.Process.Pid, "clickhouse-sink": e.sink.cmd.Process.Pid}
	for _, p := range e.procs {
		if strings.HasPrefix(p.name, "gateway-"+ts.tenant[:8]) {
			pids["gateway"] = p.cmd.Process.Pid
		}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	sample := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		lp, err1 := groupLag(ctx, cl, pg, "raw.events.v1")
		ls, err2 := groupLag(ctx, cl, sg, "processed.events.v1")
		fr := e.chQuery(t, fmt.Sprintf("SELECT ifNull(dateDiff('millisecond', max(ingested_at), now64(3,'UTC')), -1) FROM events_normalized_local WHERE tenant_id='%s' AND ingested_at > now() - 600 FORMAT TSV", ts.tenant))
		f, _ := strconv.ParseFloat(fr, 64)
		mu.Lock()
		if err1 == nil {
			lagP = append(lagP, lp)
		}
		if err2 == nil {
			lagS = append(lagS, ls)
		}
		if f >= 0 {
			fresh = append(fresh, f)
		}
		mu.Unlock()
		for n, pid := range pids {
			o := sh("ps", "-o", "rss=,%cpu=", "-p", strconv.Itoa(pid))
			var rss, cpu float64
			fmt.Sscanf(o, "%f %f", &rss, &cpu)
			mu.Lock()
			res.PeakRSSMB[n] = max(res.PeakRSSMB[n], rss/1024)
			res.PeakCPUPct[n] = max(res.PeakCPUPct[n], cpu)
			mu.Unlock()
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
				sample()
			}
		}
	}()

	// canary probes: one event at a time through the same gateway, timed until visible in ClickHouse
	var probes []float64
	probeTimeouts := 0
	stopProbe := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		const probeSource = "99999999-9999-4999-8999-999999999999"
		for i := 0; ; i++ {
			select {
			case <-stopProbe:
				return
			case <-time.After(500 * time.Millisecond):
			}
			id := uuid()
			et := time.Now().UTC()
			ev := mkEvent(ts.tenant, id, 0, 1, "probe", et)
			var m map[string]any
			_ = json.Unmarshal(ev, &m)
			m["source_id"] = probeSource
			ev, _ = json.Marshal(m)
			t0 := time.Now()
			if code, r := ts.post(t, [][]byte{ev}); code != 200 || r.Accepted != 1 {
				continue
			}
			ok := false
			for time.Since(t0) < 60*time.Second {
				if e.chQuery(t, fmt.Sprintf("SELECT count() FROM events_normalized_local WHERE tenant_id='%s' AND source_id='%s' AND event_id='%s' FORMAT TSV", ts.tenant, probeSource, id)) == "1" {
					ok = true
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			mu.Lock()
			if ok {
				probes = append(probes, float64(time.Since(t0).Microseconds())/1000)
			} else {
				probeTimeouts++
			}
			mu.Unlock()
		}
	}()

	var mid sync.WaitGroup
	if cs := os.Getenv("ASTRYON_E2E_CONTAINERS"); cs != "" {
		mid.Add(1)
		go func() {
			defer mid.Done()
			d, _ := time.ParseDuration(dur)
			time.Sleep(d / 2)
			out := sh("docker", append([]string{"stats", "--no-stream", "--format", "{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}"}, strings.Split(cs, ",")...)...)
			res.Containers = strings.Split(out, "\n")
		}()
	}

	p0 := expvarInt(t, e.procMetrics, "streamproc", "duplicates")
	q0 := expvarInt(t, e.procMetrics, "streamproc", "quarantined")
	gwAddr := strings.TrimPrefix(ts.gw, "http://")
	cmd := exec.Command(e.bins["loadgen"], "-url", "http://"+gwAddr+"/v1/events/batch", "-rate", strconv.FormatFloat(rate, 'f', -1, 64),
		"-duration", dur, "-batch", batch, "-concurrency", conc, "-dup-ratio", "0.02", "-invalid-ratio", "0.01",
		"-sources", "4", "-json-out", jsonOut, "-ids-out", jsonOut+".ids")
	cmd.Env = append(os.Environ(), "LOADGEN_TOKEN="+ts.token, "LOADGEN_TENANT_ID="+ts.tenant)
	tStart := time.Now()
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("loadgen: %v\n%s", err, b)
	}
	loadEnd := time.Now()
	var lg map[string]any
	raw, _ := os.ReadFile(jsonOut)
	_ = json.Unmarshal(raw, &lg)
	res.Loadgen = lg
	ua, _ := lg["unique_accepted"].(float64)
	if ua == 0 { // unique tracking only with -ids-out; fall back to accepted - repeats
		a, _ := lg["accepted"].(float64)
		res.UniqueAccepted = int64(a)
	} else {
		res.UniqueAccepted = int64(ua)
	}

	// drain: until ClickHouse rows == unique accepted (+ probes), or timeout
	var rows int64
	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		r, _ := ts.chCount(t, e, "AND source_id != '99999999-9999-4999-8999-999999999999'")
		rows = int64(r)
		if rows >= res.UniqueAccepted {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	drainEnd := time.Now()
	res.DrainSec = drainEnd.Sub(loadEnd).Seconds()
	close(stopProbe)
	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()
	mid.Wait()
	// exact accounting: loadgen's accepted includes probes? (probes are posted outside loadgen) -> subtract nothing,
	// compare rows excluding the probe source.
	r2, _ := ts.chCount(t, e, "AND source_id != '99999999-9999-4999-8999-999999999999'")
	res.RowsInCH = int64(r2)
	res.EndToEndPerSec = float64(res.RowsInCH) / drainEnd.Sub(tStart).Seconds()
	mu.Lock()
	res.ProbeMs, res.ProbeN, res.ProbeTimeouts = pcts(probes), len(probes), probeTimeouts
	res.FreshnessMs = pcts(fresh)
	res.LagProcMax, res.LagProcP95 = lagStats(lagP)
	res.LagSinkMax, res.LagSinkP95 = lagStats(lagS)
	mu.Unlock()
	res.DedupEntries = expvarInt(t, e.procMetrics, "streamproc", "dedup_state_entries")
	res.DedupBytes = expvarInt(t, e.procMetrics, "streamproc", "dedup_state_bytes_approx")
	res.ProcDuplicates = expvarInt(t, e.procMetrics, "streamproc", "duplicates") - p0
	res.ProcQuarantine = expvarInt(t, e.procMetrics, "streamproc", "quarantined") - q0
	t.Logf("step rate=%.0f: unique_accepted=%d rows=%d drain=%.1fs e2e/s=%.0f probe p50/p95/p99=%.0f/%.0f/%.0f ms (n=%d, timeouts=%d)",
		rate, res.UniqueAccepted, res.RowsInCH, res.DrainSec, res.EndToEndPerSec, res.ProbeMs.P50, res.ProbeMs.P95, res.ProbeMs.P99, res.ProbeN, res.ProbeTimeouts)
	return res
}

func lagStats(v []int64) (max, p95 int64) {
	if len(v) == 0 {
		return
	}
	s := append([]int64{}, v...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)-1], s[int(0.95*float64(len(s)-1))]
}
