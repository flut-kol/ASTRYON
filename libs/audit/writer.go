package audit

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrQueueFull is returned by Submit when the bounded queue is full: audit never blocks the request path.
// Callers that must not proceed without a durable record (required-audit mode) should use WriteSync.
var ErrQueueFull = errors.New("audit: queue full, event dropped")

// ErrClosed is returned after Close.
var ErrClosed = errors.New("audit: writer closed")

// TxBeginner is satisfied by *pgxpool.Pool and *pgx.Conn.
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Config bounds the writer. Zero values get defaults.
type Config struct {
	QueueSize     int           // default 1024
	BatchSize     int           // default 100
	FlushInterval time.Duration // default 500ms
	WriteTimeout  time.Duration // per batch attempt, default 5s
	Retries       int           // extra attempts per tenant batch, default 2
	// OnDrop is called (from the writer goroutine or Submit) for every event that is lost, with the reason.
	OnDrop func(e Event, reason error)
}

func (c *Config) defaults() {
	if c.QueueSize <= 0 {
		c.QueueSize = 1024
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 500 * time.Millisecond
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.Retries < 0 {
		c.Retries = 0
	} else if c.Retries == 0 {
		c.Retries = 2
	}
}

// Stats are monotonic counters for metrics.
type Stats struct{ Queued, Written, Dropped, Rejected uint64 }

type sink func(ctx context.Context, tenantID string, evs []Event) error

// Writer asynchronously persists events with a bounded queue, batching per tenant (one transaction per tenant
// with SET LOCAL astryon.tenant_id so RLS WITH CHECK applies).
type Writer struct {
	cfg  Config
	sink sink
	now  func() time.Time
	ch   chan Event
	done chan struct{}

	mu     sync.RWMutex
	closed bool

	queued, written, dropped, rejected atomic.Uint64
}

// NewWriter starts a PostgreSQL-backed writer. Call Close on shutdown to drain.
func NewWriter(db TxBeginner, cfg Config) *Writer { return newWriter(pgSink(db), cfg) }

func newWriter(s sink, cfg Config) *Writer {
	cfg.defaults()
	w := &Writer{cfg: cfg, sink: s, now: time.Now, ch: make(chan Event, cfg.QueueSize), done: make(chan struct{})}
	go w.loop()
	return w
}

// Submit validates and enqueues without blocking. Returns ErrInvalid-wrapped, ErrQueueFull or ErrClosed.
func (w *Writer) Submit(e Event) error {
	e, err := e.Normalize(w.now())
	if err != nil {
		w.rejected.Add(1)
		return err
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return ErrClosed
	}
	select {
	case w.ch <- e:
		w.queued.Add(1)
		return nil
	default:
		w.dropped.Add(1)
		if w.cfg.OnDrop != nil {
			w.cfg.OnDrop(e, ErrQueueFull)
		}
		return ErrQueueFull
	}
}

// WriteSync validates and writes one event in the caller's goroutine (for required-audit paths).
func (w *Writer) WriteSync(ctx context.Context, e Event) error {
	e, err := e.Normalize(w.now())
	if err != nil {
		w.rejected.Add(1)
		return err
	}
	if err := w.sink(ctx, e.TenantID, []Event{e}); err != nil {
		return err
	}
	w.written.Add(1)
	return nil
}

// Stats returns a snapshot of the counters.
func (w *Writer) Stats() Stats {
	return Stats{w.queued.Load(), w.written.Load(), w.dropped.Load(), w.rejected.Load()}
}

// Close stops accepting events and drains the queue, bounded by ctx.
func (w *Writer) Close(ctx context.Context) error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
	w.mu.Unlock()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) loop() {
	defer close(w.done)
	t := time.NewTicker(w.cfg.FlushInterval)
	defer t.Stop()
	batch := make([]Event, 0, w.cfg.BatchSize)
	for {
		select {
		case e, ok := <-w.ch:
			if !ok {
				w.flush(batch)
				return
			}
			batch = append(batch, e)
			if len(batch) >= w.cfg.BatchSize {
				w.flush(batch)
				batch = batch[:0]
			}
		case <-t.C:
			if len(batch) > 0 {
				w.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (w *Writer) flush(batch []Event) {
	by := map[string][]Event{}
	for _, e := range batch {
		by[e.TenantID] = append(by[e.TenantID], e)
	}
	for tenant, evs := range by {
		var err error
		for attempt := 0; attempt <= w.cfg.Retries; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), w.cfg.WriteTimeout)
			err = w.sink(ctx, tenant, evs)
			cancel()
			if err == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
		}
		if err != nil {
			w.dropped.Add(uint64(len(evs)))
			if w.cfg.OnDrop != nil {
				for _, e := range evs {
					w.cfg.OnDrop(e, err)
				}
			}
			continue
		}
		w.written.Add(uint64(len(evs)))
	}
}

const sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`

const sqlInsert = `INSERT INTO audit_log
 (id, tenant_id, occurred_at, category, outcome, action, entity_type, entity_id, resource_ref,
  actor_aas_sub, actor_client_id, auth_acr, request_id, trace_id, details)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
 ON CONFLICT (id) DO NOTHING`

func nz(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func pgSink(db TxBeginner) sink {
	return func(ctx context.Context, tenantID string, evs []Event) (err error) {
		tx, err := db.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(context.Background())
			}
		}()
		if _, err = tx.Exec(ctx, sqlSetTenant, tenantID); err != nil {
			return err
		}
		b := &pgx.Batch{}
		for _, e := range evs {
			d := e.Details
			if d == nil {
				d = map[string]string{}
			}
			js, merr := json.Marshal(d)
			if merr != nil {
				return merr
			}
			eid, ref := e.Resource.split()
			b.Queue(sqlInsert, e.ID, e.TenantID, e.OccurredAt, string(e.Category), string(e.Outcome), e.Action,
				e.Resource.Type, eid, ref, nz(e.Subject), nz(e.ClientID), nz(e.ACR), nz(e.RequestID), nz(e.TraceID), js)
		}
		br := tx.SendBatch(ctx, b)
		for range evs {
			if _, err = br.Exec(); err != nil {
				_ = br.Close()
				return err
			}
		}
		if err = br.Close(); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
}
