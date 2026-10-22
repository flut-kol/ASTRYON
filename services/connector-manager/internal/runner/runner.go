// Package runner executes REST pull connectors: claim a due connector (fenced lease), pull one page with the
// persisted cursor, map it to envelopes, emit them, and only then advance the cursor.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/astryon/connector-manager/internal/pullstore"
	"github.com/astryon/connector-manager/internal/restpull"
)

// Store is the subset of *pullstore.Store the runner needs.
type Store interface {
	Claim(ctx context.Context) (pullstore.Claimed, error)
	Succeed(ctx context.Context, c pullstore.Claimed, nextCursor string, every time.Duration, records int64) error
	Fail(ctx context.Context, c pullstore.Claimed, delay time.Duration, code string) error
}

// Emitter publishes envelopes (already marshalled, keyed by event id) to the raw-events path. It must return nil only
// when every event is durably accepted. Events carry stable ids, so a re-emitted page is harmless downstream.
type Emitter interface {
	Emit(ctx context.Context, tenant, source string, events [][]byte) error
}

// Resolver turns a connector's SecretRef into an Authorization header value (empty = no authentication).
type Resolver interface {
	Authorization(ctx context.Context, secretRef string) (string, error)
}

type Metrics struct {
	Pulls, Pages, Records, Skipped, Failures, Fenced atomic.Int64
}

type Config struct {
	Store    Store
	Emitter  Emitter
	Resolver Resolver
	HTTP     *http.Client
	Poll     time.Duration // idle wait, default 2s
	Now      func() time.Time
	Jitter   func() float64 // [0,1], default rand 0..0.2
	// Backoff, when set (CONNECTOR_RETRY_MODE=controller), replaces the built-in 30 s..1 h schedule. failures is the
	// consecutive failure count before this failure; retryAfter is the source's minimum.
	Backoff func(code string, failures int, cause error, retryAfter time.Duration) (time.Duration, error)
}

type Runner struct {
	Config
	M       Metrics
	mu      sync.Mutex
	clients map[string]*restpull.Client
}

func New(c Config) *Runner {
	if c.Poll <= 0 {
		c.Poll = 2 * time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Jitter == nil {
		c.Jitter = func() float64 { return rand.Float64() * 0.2 } // #nosec G404 -- jitter only, not security sensitive
	}
	return &Runner{Config: c, clients: map[string]*restpull.Client{}}
}

func (r *Runner) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		ok, err := r.RunOnce(ctx)
		if err != nil {
			log.Printf("connector runner: %v", err)
		}
		if !ok || err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(r.Poll):
			}
		}
	}
	return ctx.Err()
}

type settings struct {
	rest     restpull.Config
	mapping  Mapping
	interval time.Duration
}

func parseSettings(cfg map[string]any) (settings, error) {
	str := func(k string) string { s, _ := cfg[k].(string); return s }
	num := func(k string, d float64) float64 {
		if v, ok := cfg[k].(float64); ok {
			return v
		}
		return d
	}
	s := settings{
		rest: restpull.Config{Endpoint: str("endpoint"), CursorParameter: str("cursor_parameter"),
			RequestsPerSecond: int(num("requests_per_second", 1)), MaxResponseBytes: int64(num("max_response_bytes", 4<<20))},
		mapping: Mapping{SchemaID: str("schema_id"), SchemaVersion: int(num("schema_version", 1)), RecordsPath: str("records_path"),
			IDField: str("id_field"), EventTimeField: str("event_time_field"), EntityField: str("entity_field"), ProcessingVersion: int(num("processing_version", 1))},
		interval: time.Duration(num("interval_seconds", 300)) * time.Second,
	}
	if s.mapping.SchemaID == "" || s.interval < time.Second {
		return s, errors.New("schema_id is required and interval_seconds must be >= 1")
	}
	return s, nil
}

func (r *Runner) client(c pullstore.Claimed, s settings, auth string) (*restpull.Client, error) {
	key := c.ConnectorID + "|" + c.SecretRef + "|" + fmt.Sprint(s.rest)
	r.mu.Lock()
	defer r.mu.Unlock()
	if cl, ok := r.clients[key]; ok {
		cl.Authorization = auth
		return cl, nil
	}
	cl, err := restpull.NewClient(s.rest, r.HTTP)
	if err != nil {
		return nil, err
	}
	cl.Authorization = auth
	r.clients[key] = cl
	return cl, nil
}

// RunOnce handles at most one connector page. ok=false means nothing was due.
func (r *Runner) RunOnce(ctx context.Context) (bool, error) {
	c, err := r.Store.Claim(ctx)
	if errors.Is(err, pullstore.ErrNoConnector) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	r.M.Pulls.Add(1)
	err = r.page(ctx, c)
	if errors.Is(err, pullstore.ErrFenced) {
		r.M.Fenced.Add(1)
	}
	return true, err
}

func (r *Runner) page(ctx context.Context, c pullstore.Claimed) error {
	fail := func(code string, retryAfter time.Duration, cause error) error {
		r.M.Failures.Add(1)
		now := r.Now()
		if r.Backoff != nil {
			d, berr := r.Backoff(code, c.ConsecutiveFailures, cause, retryAfter)
			if berr != nil {
				return errors.Join(cause, berr)
			}
			if err := r.Store.Fail(context.WithoutCancel(ctx), c, d, code); err != nil {
				return errors.Join(cause, err)
			}
			return cause
		}
		p, perr := restpull.Progress{ConsecutiveFailures: c.ConsecutiveFailures}.Fail(now, 30*time.Second, time.Hour, r.Jitter(), retryAfter)
		if perr != nil {
			return perr
		}
		if err := r.Store.Fail(context.WithoutCancel(ctx), c, p.NextAttemptAt.Sub(now), code); err != nil {
			return errors.Join(cause, err)
		}
		return cause
	}
	s, err := parseSettings(c.Config)
	if err != nil {
		return fail("bad_config", 0, err)
	}
	auth := ""
	if c.SecretRef != "" {
		if r.Resolver == nil {
			return fail("no_secret_resolver", 0, errors.New("connector has a secret_ref but no resolver is configured"))
		}
		if auth, err = r.Resolver.Authorization(ctx, c.SecretRef); err != nil {
			return fail("secret_unavailable", 0, err)
		}
	}
	cl, err := r.client(c, s, auth)
	if err != nil {
		return fail("bad_config", 0, err)
	}
	resp, err := cl.Pull(ctx, c.Cursor)
	if err != nil {
		var he *restpull.HTTPError
		if errors.As(err, &he) {
			return fail(fmt.Sprintf("http_%d", he.StatusCode), he.RetryAfter, err)
		}
		if errors.Is(err, restpull.ErrResponseTooLarge) {
			return fail("response_too_large", 0, err)
		}
		return fail("pull_error", 0, err)
	}
	events, skipped, err := MapPage(resp.Body, s.mapping, c.TenantID, c.SourceID, c.ConnectorID, r.Now())
	if err != nil {
		return fail("bad_response", 0, err)
	}
	if len(events) > 0 {
		if err := r.Emitter.Emit(ctx, c.TenantID, c.SourceID, events); err != nil {
			return fail("emit_failed", 0, err)
		}
	}
	r.M.Pages.Add(1)
	r.M.Records.Add(int64(len(events)))
	r.M.Skipped.Add(int64(skipped))
	every := s.interval
	if resp.NextCursor != "" {
		every = 0 // more pages are waiting: continue immediately (the per-connector rate limiter still applies)
	}
	return r.Store.Succeed(ctx, c, resp.NextCursor, every, int64(len(events)))
}

// WritePrometheus renders astryon_connector_runner_* counters.
func (m *Metrics) WritePrometheus(w interface{ Write([]byte) (int, error) }) {
	var b strings.Builder
	for _, c := range []struct {
		n, h string
		v    int64
	}{
		{"pulls_total", "Connector pulls started", m.Pulls.Load()}, {"pages_total", "Pages emitted and checkpointed", m.Pages.Load()},
		{"records_total", "Envelopes emitted", m.Records.Load()}, {"skipped_records_total", "Records skipped (not objects / invalid envelope)", m.Skipped.Load()},
		{"failures_total", "Failed pulls (retry scheduled)", m.Failures.Load()}, {"fenced_total", "Pulls abandoned: lease lost", m.Fenced.Load()},
	} {
		fmt.Fprintf(&b, "# HELP astryon_connector_runner_%s %s\n# TYPE astryon_connector_runner_%s counter\nastryon_connector_runner_%s %d\n", c.n, c.h, c.n, c.n, c.v)
	}
	_, _ = w.Write([]byte(b.String()))
}
