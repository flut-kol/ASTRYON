package replay

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics are plain counters in Prometheus text exposition (hand-rolled like the sibling services). Labels are bounded
// enums only: never tenant, source or replay id. A nil *Metrics is a no-op.
type Metrics struct {
	Requested, DryRuns, Approved, PublishErrors atomic.Int64

	mu         sync.Mutex
	rejected   map[string]int64 // bounded error code -> count
	activation map[string]int64 // activate|rollback|noop -> count
}

// Rejection codes are the API error codes the handler can answer on POST /v1/replays; anything else is "other".
var rejectCodes = []string{"invalid_request", "audit_unavailable", "replay_conflict", "upstream_error", "unauthenticated", "forbidden", "other"}

func (m *Metrics) inc(mp *map[string]int64, k string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if *mp == nil {
		*mp = map[string]int64{}
	}
	(*mp)[k]++
}

// IncRequested counts an authorized create call (dry runs are counted separately and are not "requested" replays).
func (m *Metrics) IncRequested(dry bool) {
	if m == nil {
		return
	}
	if dry {
		m.DryRuns.Add(1)
		return
	}
	m.Requested.Add(1)
}

// IncApproved counts a replay accepted for execution (202: version reserved, job published).
func (m *Metrics) IncApproved() {
	if m != nil {
		m.Approved.Add(1)
	}
}

// IncRejected counts a refused create call by bounded API error code.
func (m *Metrics) IncRejected(code string) {
	if m == nil {
		return
	}
	for _, c := range rejectCodes {
		if c == code {
			m.inc(&m.rejected, code)
			return
		}
	}
	m.inc(&m.rejected, "other")
}

// IncActivation counts a committed cutover action (activate|rollback|noop).
func (m *Metrics) IncActivation(action string) {
	if m == nil {
		return
	}
	switch action {
	case "activate", "rollback", "noop":
	default:
		action = "other"
	}
	m.inc(&m.activation, action)
}

func (m *Metrics) WritePrometheus(w io.Writer) {
	const p = "astryon_replay_controller_"
	for _, c := range []struct {
		name, help string
		v          int64
	}{
		{"replays_requested_total", "Authorized replay create calls (execute mode)", m.Requested.Load()},
		{"replays_dry_run_total", "Authorized replay create calls in dry_run mode", m.DryRuns.Load()},
		{"replays_approved_total", "Replays accepted for execution (version reserved, job published)", m.Approved.Load()},
		{"publish_errors_total", "Replay job publish failures", m.PublishErrors.Load()},
	} {
		fmt.Fprintf(w, "# HELP %s%s %s\n# TYPE %s%s counter\n%s%s %d\n", p, c.name, c.help, p, c.name, p, c.name, c.v)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Fprintf(w, "# HELP %sreplays_rejected_total Replay create calls refused, by API error code\n# TYPE %sreplays_rejected_total counter\n", p, p)
	for _, c := range rejectCodes {
		fmt.Fprintf(w, "%sreplays_rejected_total{code=%q} %d\n", p, c, m.rejected[c])
	}
	fmt.Fprintf(w, "# HELP %sactivations_total Cutover actions committed\n# TYPE %sactivations_total counter\n", p, p)
	acts := []string{"activate", "rollback", "noop", "other"}
	sort.Strings(acts)
	for _, a := range acts {
		fmt.Fprintf(w, "%sactivations_total{action=%q} %d\n", p, a, m.activation[a])
	}
}

// MeteredPublisher counts publish failures of the wrapped publisher.
type MeteredPublisher struct {
	Next JobPublisher
	M    *Metrics
}

func (p MeteredPublisher) Publish(ctx context.Context, job ReplayJob) error {
	err := p.Next.Publish(ctx, job)
	if err != nil && p.M != nil {
		p.M.PublishErrors.Add(1)
	}
	return err
}

// MetricsListenAddr maps the env value to a listen address: empty = off (ok=false); a bare ":port" binds loopback only,
// an explicit host (e.g. a pod IP or 0.0.0.0) is honoured.
func MetricsListenAddr(v string) (addr string, ok bool) {
	if v == "" {
		return "", false
	}
	host, port, err := net.SplitHostPort(v)
	if err != nil {
		return v, true // let ListenAndServe report it
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), true
}

// MetricsHandler serves GET /metrics.
func MetricsHandler(write func(io.Writer)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		write(w)
	})
	return mux
}

// ServeMetrics starts the opt-in listener (nothing when addr is empty) and returns the server (nil when off).
func ServeMetrics(env string, write func(io.Writer), logf func(string, ...any)) *http.Server {
	addr, ok := MetricsListenAddr(env)
	if !ok {
		return nil
	}
	srv := &http.Server{Addr: addr, Handler: MetricsHandler(write), ReadHeaderTimeout: 5 * time.Second}
	go func() { logf("metrics: %v", srv.ListenAndServe()) }()
	return srv
}
