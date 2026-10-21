package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Phase is one segment of a scenario at a fixed rate.
type Phase struct {
	Name     string
	Rate     float64
	Duration time.Duration
}

// BurstPhases returns baseline -> burst -> drain-observation phases. The drain
// phase keeps a low baseline rate (may be 0 = no load is meaningless for the
// limiter, so a zero baseline is modelled as no phase at all).
func BurstPhases(baseline float64, base time.Duration, burst float64, burstDur time.Duration, drain time.Duration) []Phase {
	var ph []Phase
	if baseline > 0 && base > 0 {
		ph = append(ph, Phase{"baseline", baseline, base})
	}
	ph = append(ph, Phase{"burst", burst, burstDur})
	if baseline > 0 && drain > 0 {
		ph = append(ph, Phase{"drain", baseline, drain})
	}
	return ph
}

// PhaseReport is the report of one phase.
type PhaseReport struct {
	Name   string  `json:"name"`
	Target float64 `json:"target_events_per_sec"`
	Report Report  `json:"report"`
}

// RunPhases runs the phases sequentially, reusing the config otherwise.
func RunPhases(ctx context.Context, cfg RunConfig, phases []Phase, client *http.Client, onPhase func(string)) []PhaseReport {
	var out []PhaseReport
	for i, p := range phases {
		c := cfg
		c.Rate, c.Duration = p.Rate, p.Duration
		c.Seed = cfg.Seed + int64(i)*104729
		if onPhase != nil {
			onPhase(p.Name)
		}
		st, el := Run(ctx, c, client)
		out = append(out, PhaseReport{Name: p.Name, Target: p.Rate, Report: st.Report(el)})
		if ctx.Err() != nil {
			break
		}
	}
	return out
}

// QueryConfig describes concurrent read load (DPP-059).
type QueryConfig struct {
	URL         string
	Token       string
	Rate        float64 // queries/s overall, 0 = unlimited
	Concurrency int
	Timeout     time.Duration
}

// QueryReport summarises read latencies under ingest load.
type QueryReport struct {
	Requests   int64              `json:"requests"`
	Errors     int64              `json:"errors"`
	LatencyMs  map[string]float64 `json:"latency_ms"`
	HTTPStatus map[string]int64   `json:"http_status"`
}

// RunQueries issues GETs until ctx is done.
func RunQueries(ctx context.Context, qc QueryConfig, client *http.Client) QueryReport {
	lim := NewLimiter(qc.Rate)
	var mu sync.Mutex
	var lat []time.Duration
	status := map[string]int64{}
	var reqs, errs int64
	var wg sync.WaitGroup
	for w := 0; w < qc.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for lim.WaitN(ctx, 1) == nil {
				rctx, cancel := context.WithTimeout(context.Background(), qc.Timeout)
				req, _ := http.NewRequestWithContext(rctx, http.MethodGet, qc.URL, nil)
				if qc.Token != "" {
					req.Header.Set("Authorization", "Bearer "+qc.Token)
				}
				t0 := time.Now()
				resp, err := client.Do(req)
				d := time.Since(t0)
				code := 0
				if err == nil {
					_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32<<20))
					resp.Body.Close()
					code = resp.StatusCode
				}
				cancel()
				mu.Lock()
				reqs++
				key := "transport_error"
				if code != 0 {
					key = strconv.Itoa(code)
					lat = append(lat, d)
				}
				status[key]++
				if code == 0 || code >= 500 {
					errs++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	r := QueryReport{Requests: reqs, Errors: errs, HTTPStatus: status, LatencyMs: map[string]float64{
		"p50": ms(Percentile(lat, 50)), "p95": ms(Percentile(lat, 95)), "p99": ms(Percentile(lat, 99))}}
	return r
}

// BacklogSample is one reading of the backlog gauge.
type BacklogSample struct {
	AtSec float64 `json:"t_sec"`
	Value float64 `json:"value"`
}

// BacklogReport describes backlog build-up and drain after a burst.
type BacklogReport struct {
	Query       string          `json:"promql"`
	Peak        float64         `json:"peak"`
	Final       float64         `json:"final"`
	Drained     bool            `json:"drained"`
	DrainSec    *float64        `json:"drain_sec_after_burst_end,omitempty"`
	Samples     []BacklogSample `json:"samples"`
	SampleError string          `json:"sample_error,omitempty"`
}

// QueryProm reads a scalar from a Prometheus instant-query endpoint
// (baseURL/api/v1/query). A vector result is summed.
func QueryProm(ctx context.Context, client *http.Client, baseURL, promql string) (float64, error) {
	u := baseURL + "/api/v1/query?query=" + url.QueryEscape(promql)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return ParsePromValue(body)
}

// ParsePromValue extracts the (summed) value from an instant-query response.
func ParsePromValue(body []byte) (float64, error) {
	var pr struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &pr); err != nil || pr.Status != "success" {
		return 0, fmt.Errorf("prometheus: bad response")
	}
	parse := func(v []any) (float64, error) {
		if len(v) != 2 {
			return 0, fmt.Errorf("prometheus: bad sample")
		}
		s, _ := v[1].(string)
		return strconv.ParseFloat(s, 64)
	}
	if pr.Data.ResultType == "scalar" {
		var v []any
		if err := json.Unmarshal(pr.Data.Result, &v); err != nil {
			return 0, err
		}
		return parse(v)
	}
	var vec []struct {
		Value []any `json:"value"`
	}
	if err := json.Unmarshal(pr.Data.Result, &vec); err != nil {
		return 0, err
	}
	if len(vec) == 0 {
		return 0, fmt.Errorf("prometheus: empty result")
	}
	var sum float64
	for _, e := range vec {
		f, err := parse(e.Value)
		if err != nil {
			return 0, err
		}
		sum += f
	}
	return sum, nil
}

// SampleFunc returns the current backlog.
type SampleFunc func(ctx context.Context) (float64, error)

// BacklogMonitor samples every interval from t0 until ctx ends; mark() records
// the end of the burst so the drain time can be computed.
type BacklogMonitor struct {
	mu       sync.Mutex
	t0       time.Time
	samples  []BacklogSample
	burstEnd *float64
	lastErr  string
}

// NewBacklogMonitor creates a monitor with t0 = now.
func NewBacklogMonitor() *BacklogMonitor { return &BacklogMonitor{t0: time.Now()} }

// MarkBurstEnd records that the burst ended now.
func (m *BacklogMonitor) MarkBurstEnd() {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := time.Since(m.t0).Seconds()
	m.burstEnd = &v
}

// Run samples until ctx is done.
func (m *BacklogMonitor) Run(ctx context.Context, f SampleFunc, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		sctx, cancel := context.WithTimeout(ctx, every*3)
		v, err := f(sctx)
		cancel()
		m.mu.Lock()
		if err != nil {
			m.lastErr = err.Error()
		} else {
			m.samples = append(m.samples, BacklogSample{time.Since(m.t0).Seconds(), v})
		}
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// Report evaluates the samples: drained means a sample after burst end at or
// below threshold; DrainSec is the time of the first such sample minus burst end.
func (m *BacklogMonitor) Report(promql string, threshold float64) BacklogReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	return EvalBacklog(promql, m.samples, m.burstEnd, threshold, m.lastErr)
}

// EvalBacklog is the pure evaluation behind BacklogMonitor.Report.
func EvalBacklog(promql string, s []BacklogSample, burstEnd *float64, threshold float64, sampleErr string) BacklogReport {
	r := BacklogReport{Query: promql, Samples: s, SampleError: sampleErr}
	if len(s) > 0 {
		r.Final = s[len(s)-1].Value
	}
	for _, x := range s {
		if x.Value > r.Peak {
			r.Peak = x.Value
		}
	}
	if burstEnd != nil {
		for _, x := range s {
			if x.AtSec >= *burstEnd && x.Value <= threshold {
				d := x.AtSec - *burstEnd
				r.Drained, r.DrainSec = true, &d
				break
			}
		}
	}
	return r
}

// Check is one pass/fail criterion against an SLO.
type Check struct {
	Name  string  `json:"name"`
	Pass  bool    `json:"pass"`
	Value float64 `json:"value"`
	Limit float64 `json:"limit"`
	Note  string  `json:"note,omitempty"`
}

// Criteria are the pass limits (defaults from plan section 2).
type Criteria struct {
	AckP95Ms      float64 // 250 ms in LAN
	MaxFailedFrac float64 // fraction of sent events not acknowledged
	MinRateFrac   float64 // achieved/target events/s, 0 = skip
	QueryP95Ms    float64 // 0 = skip
	MaxDrainSec   float64 // 0 = skip
}

// Evaluate builds the checks for a finished scenario. target is the intended
// events/s of the main phase (0 = unknown).
func Evaluate(c Criteria, main Report, target float64, q *QueryReport, b *BacklogReport) []Check {
	var out []Check
	out = append(out, Check{Name: "ack_p95_ms", Pass: main.LatencyMs["p95"] <= c.AckP95Ms, Value: main.LatencyMs["p95"], Limit: c.AckP95Ms,
		Note: "plan s2 target, not an SLA"})
	frac := 0.0
	if main.Sent > 0 {
		frac = float64(main.Failed) / float64(main.Sent)
	}
	out = append(out, Check{Name: "failed_fraction", Pass: main.Sent > 0 && frac <= c.MaxFailedFrac, Value: frac, Limit: c.MaxFailedFrac,
		Note: "failed = not ACKed; accepted events must never be lost"})
	if c.MinRateFrac > 0 && target > 0 {
		f := main.EventsPerSec / target
		out = append(out, Check{Name: "achieved_rate_fraction", Pass: f >= c.MinRateFrac, Value: f, Limit: c.MinRateFrac})
	}
	if q != nil && c.QueryP95Ms > 0 {
		out = append(out, Check{Name: "query_p95_ms", Pass: q.LatencyMs["p95"] <= c.QueryP95Ms && q.Errors == 0, Value: q.LatencyMs["p95"], Limit: c.QueryP95Ms})
	}
	if b != nil && c.MaxDrainSec > 0 {
		v := -1.0
		if b.DrainSec != nil {
			v = *b.DrainSec
		}
		out = append(out, Check{Name: "backlog_drain_sec", Pass: b.Drained && v <= c.MaxDrainSec, Value: v, Limit: c.MaxDrainSec})
	}
	return out
}

// AllPass reports whether every check passed.
func AllPass(cs []Check) bool {
	for _, c := range cs {
		if !c.Pass {
			return false
		}
	}
	return true
}
