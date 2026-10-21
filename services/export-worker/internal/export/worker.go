package export

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClickHouse is the HTTP connection to the (read-only) query endpoint.
type ClickHouse struct {
	URL, Database, User, Password string
	Table                         string // default events_normalized_distributed
	MaxExecutionSeconds           int    // default 600
	HTTP                          *http.Client
	// IdleTimeout fails the stream when ClickHouse delivers no byte for this long while we are waiting for data
	// (a stalled server that keeps the connection open). Default 30s, negative = off. The clock runs only inside
	// Read, so a slow object store never trips it. MaxDuration bounds the whole request incl. the stream (default
	// 15m, negative = off). Both fail the job with ErrCHTimeout (error code clickhouse_timeout).
	IdleTimeout, MaxDuration time.Duration
	// HeaderTimeout bounds the wait for the response headers (a ClickHouse that accepted the query but never answers,
	// e.g. a hung node or a black-holed connection). Default 5m (ClickHouse may legitimately compute a long time before
	// its first byte; keep it above the longest expected query start), negative = off. Fails with ErrCHTimeout.
	HeaderTimeout time.Duration
}

// Defaults of the ClickHouse stream timeouts (env EXPORT_CH_IDLE_TIMEOUT / EXPORT_CH_MAX_DURATION in the binary).
const (
	DefaultCHIdleTimeout   = 30 * time.Second
	DefaultCHMaxDuration   = 15 * time.Minute
	DefaultCHHeaderTimeout = 5 * time.Minute
)

// ErrCHTimeout marks a ClickHouse stream that stalled (idle timeout) or ran past MaxDuration.
var ErrCHTimeout = errors.New("clickhouse stream timeout")

func orDefault(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

// chBody is the streaming response with the idle watchdog; Close releases the request context and timers.
type chBody struct {
	rc     io.ReadCloser
	ctx    context.Context
	cancel context.CancelCauseFunc
	stopT  context.CancelFunc
	idle   time.Duration
	timer  *time.Timer
}

func (b *chBody) Read(p []byte) (int, error) {
	if b.idle > 0 {
		if b.timer == nil {
			b.timer = time.AfterFunc(b.idle, b.fire)
		} else {
			b.timer.Reset(b.idle)
		}
	}
	n, err := b.rc.Read(p)
	if b.timer != nil {
		b.timer.Stop()
	}
	if err != nil && err != io.EOF {
		if cause := context.Cause(b.ctx); errors.Is(cause, ErrCHTimeout) {
			err = cause
		}
	}
	return n, err
}

func (b *chBody) fire() {
	b.cancel(fmt.Errorf("%w: no data from ClickHouse for %s (idle timeout)", ErrCHTimeout, b.idle))
}

func (b *chBody) Close() error {
	if b.timer != nil {
		b.timer.Stop()
	}
	err := b.rc.Close()
	b.cancel(nil)
	b.stopT()
	return err
}

func (c ClickHouse) table() string {
	if c.Table != "" {
		return c.Table
	}
	return "events_normalized_distributed"
}

// open runs the query and returns the streaming body. readonly=1 as in query-api.
func (c ClickHouse) open(ctx context.Context, q string, params url.Values) (io.ReadCloser, error) {
	v := url.Values{}
	for k, vs := range params {
		v[k] = vs
	}
	if c.Database != "" {
		v.Set("database", c.Database)
	}
	v.Set("readonly", "1")
	met := c.MaxExecutionSeconds
	if met <= 0 {
		met = 600
	}
	v.Set("max_execution_time", fmt.Sprint(met))
	v.Set("max_memory_usage", "2000000000")
	cl := c.HTTP
	if cl == nil {
		cl = &http.Client{} // no overall timeout: the stream may be long; ctx bounds it
	}
	parent := ctx
	cctx, cancel := context.WithCancelCause(parent)
	stopT := context.CancelFunc(func() {})
	if md := orDefault(c.MaxDuration, DefaultCHMaxDuration); md > 0 {
		cctx, stopT = context.WithTimeoutCause(cctx, md, fmt.Errorf("%w: ClickHouse request exceeded max duration %s", ErrCHTimeout, md))
	}
	release := func() { cancel(nil); stopT() }
	ctx = cctx
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/?"+v.Encode(), strings.NewReader(q))
	if err != nil {
		release()
		return nil, err
	}
	if c.User != "" {
		req.Header.Set("X-ClickHouse-User", c.User)
		req.Header.Set("X-ClickHouse-Key", c.Password)
	}
	var hdrT *time.Timer
	if ht := orDefault(c.HeaderTimeout, DefaultCHHeaderTimeout); ht > 0 {
		hdrT = time.AfterFunc(ht, func() {
			cancel(fmt.Errorf("%w: no response headers from ClickHouse within %s (header timeout)", ErrCHTimeout, ht))
		})
	}
	resp, err := cl.Do(req)
	if hdrT != nil {
		hdrT.Stop()
	}
	if err != nil {
		cause := context.Cause(ctx)
		release()
		if parent.Err() != nil {
			return nil, err
		}
		if errors.Is(cause, ErrCHTimeout) {
			return nil, cause
		}
		return nil, fmt.Errorf("%w: %v", ErrClickHouse, err)
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		_ = resp.Body.Close()
		release()
		return nil, fmt.Errorf("%w: clickhouse %s: %s", ErrClickHouse, resp.Status, strings.TrimSpace(string(b)))
	}
	return &chBody{rc: resp.Body, ctx: ctx, cancel: cancel, stopT: stopT, idle: orDefault(c.IdleTimeout, DefaultCHIdleTimeout)}, nil
}

// Runner executes exports.
type Runner struct {
	CH       ClickHouse
	Store    ObjectStore
	Presign  Presigner
	Quota    QuotaChecker
	Audit    AuditSink
	MaxBytes int64         // default 1 GiB
	TTL      time.Duration // download URL ttl, default 15 min
	Now      func() time.Time
}

// Result describes a finished export.
type Result struct {
	Key         string
	Rows, Bytes int64
	DownloadURL string
	ExpiresAt   time.Time
	SpecHash    string
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// Run executes one export. The tenant comes only from the verified principal. A record is audited for
// every outcome; partial objects are deleted on failure.
func (r *Runner) Run(ctx context.Context, p Principal, exportID string, spec ExportSpec) (res Result, err error) {
	rec := AuditRecord{TenantID: p.TenantID, Subject: p.Subject, ExportID: exportID}
	emit := func(outcome string, e error) {
		rec.Time, rec.Outcome = r.now(), outcome
		if e != nil {
			rec.Error = e.Error()
		}
		if r.Audit != nil {
			_ = r.Audit.Emit(rec)
		}
	}
	if !uuidRe.MatchString(p.TenantID) {
		err = fmt.Errorf("%w: principal has no valid tenant", ErrInvalidSpec)
		emit(OutcomeInvalid, err)
		return res, err
	}
	if !exportIDRe.MatchString(exportID) {
		err = fmt.Errorf("%w: invalid export id", ErrInvalidSpec)
		emit(OutcomeInvalid, err)
		return res, err
	}
	spec, err = spec.Normalize()
	if err != nil {
		emit(OutcomeInvalid, err)
		return res, err
	}
	rec.SpecHash = spec.Hash()
	if r.Quota != nil {
		release, qerr := r.Quota.Acquire(p.TenantID)
		if qerr != nil {
			emit(OutcomeQuotaExceeded, qerr)
			return res, qerr
		}
		defer release()
	}
	maxBytes := r.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 30
	}
	key := ObjectKey(p.TenantID, exportID, spec.Ext())
	if !KeyBelongsToTenant(key, p.TenantID) {
		err = fmt.Errorf("%w: object key outside tenant prefix", ErrInvalidSpec)
		emit(OutcomeInvalid, err)
		return res, err
	}
	q, params := buildQuery(r.CH.table(), p.TenantID, spec)
	body, err := r.CH.open(ctx, q, params)
	if err != nil {
		emit(OutcomeFailed, err)
		return res, err
	}
	defer body.Close()
	g := newGuard(body, spec.Format == "csv", spec.MaxRows, maxBytes)
	n, err := r.Store.Put(ctx, key, g)
	rec.Rows, rec.Bytes = g.Rows, g.Bytes
	if err == nil && g.err != nil { // store swallowed a guard error
		err = g.err
	}
	if err != nil {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		_ = r.Store.Delete(dctx, key)
		cancel()
		outcome := OutcomeFailed
		switch {
		case errors.Is(err, ErrRowCap):
			outcome = OutcomeRowCap
		case errors.Is(err, ErrByteCap):
			outcome = OutcomeByteCap
		}
		emit(outcome, err)
		return res, err
	}
	rec.Bytes = n
	res = Result{Key: key, Rows: g.Rows, Bytes: n, SpecHash: rec.SpecHash}
	if r.Presign != nil {
		res.DownloadURL, res.ExpiresAt, err = r.Presign.PresignGet(key, r.TTL)
		if err != nil {
			_ = r.Store.Delete(ctx, key)
			emit(OutcomeFailed, err)
			return Result{}, err
		}
	}
	emit(OutcomeSuccess, nil)
	return res, nil
}

// DownloadURL issues a fresh short-lived URL for an existing export; the key must be under the caller's tenant.
func (r *Runner) DownloadURL(p Principal, key string, ttl time.Duration) (string, time.Time, error) {
	if !KeyBelongsToTenant(key, p.TenantID) {
		return "", time.Time{}, errors.New("export not found")
	}
	return r.Presign.PresignGet(key, ttl)
}
