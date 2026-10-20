package sink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"
)

// Options tunes the pipeline.
type Options struct {
	BatchSize       int
	BatchInterval   time.Duration
	MaxRetries      int // attempts after the first; 0 = unlimited until shutdown
	RetryBackoff    time.Duration
	RetryBackoffMax time.Duration
	ShutdownTimeout time.Duration
	Now             func() time.Time
	// Intents, when set, makes chunk boundaries durable: each chunk's offset range is recorded before insert and a
	// restarted pipeline re-cuts the first chunk of a partition to the recorded range (same dedup token).
	Intents IntentStore
	// IntentWait bounds how long a flush is held waiting for a recorded range to be redelivered in full.
	IntentWait time.Duration
}

// Pipeline: Source -> map -> batch -> Sink (+Quarantiner) -> commit.
type Pipeline struct {
	src Source
	snk Sink
	qr  Quarantiner
	opt Options
	M   *Metrics

	buf []Message

	// durable chunk boundaries
	pending   map[partKey]Range // recorded range found for a partition and not yet applied
	next      map[partKey]int64 // next expected offset after the last commit; mismatch => (re)look up intents
	holdSince time.Time
}

type partKey struct {
	topic string
	part  int32
}

func NewPipeline(src Source, snk Sink, qr Quarantiner, opt Options) *Pipeline {
	if opt.BatchSize < 1 {
		opt.BatchSize = 1000
	}
	if opt.BatchInterval <= 0 {
		opt.BatchInterval = time.Second
	}
	if opt.RetryBackoff <= 0 {
		opt.RetryBackoff = 100 * time.Millisecond
	}
	if opt.RetryBackoffMax < opt.RetryBackoff {
		opt.RetryBackoffMax = opt.RetryBackoff
	}
	if opt.ShutdownTimeout <= 0 {
		opt.ShutdownTimeout = 30 * time.Second
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.IntentWait <= 0 {
		opt.IntentWait = 30 * time.Second
	}
	return &Pipeline{src: src, snk: snk, qr: qr, opt: opt, M: &Metrics{},
		pending: map[partKey]Range{}, next: map[partKey]int64{}}
}

// Token returns the deterministic insert_deduplication_token for a chunk:
// sha256 over topic, partition, first and last offset. Identical offset ranges
// always produce the same token, so a retried (or re-consumed) range is dropped
// by ClickHouse block deduplication.
func Token(topic string, partition int32, first, last int64) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("chsink|%s|%d|%d|%d", topic, partition, first, last)))
	return hex.EncodeToString(h[:])
}

// Run consumes until ctx is cancelled (then flushes and returns nil) or a fatal
// error occurs. Messages in an unflushed/unsent batch are never committed.
func (p *Pipeline) Run(ctx context.Context) error {
	deadline := time.Now().Add(p.opt.BatchInterval)
	for {
		if ctx.Err() != nil {
			return p.shutdown()
		}
		// Poll only until the time-based flush is due. While flush() blocks (retries)
		// nothing is polled: this is the backpressure.
		pctx, cancel := context.WithDeadline(ctx, deadline)
		msgs, err := p.src.Poll(pctx)
		cancel()
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("poll: %w", err)
		}
		if len(msgs) > 0 {
			p.buf = append(p.buf, msgs...)
			p.M.Consumed.Add(int64(len(msgs)))
			p.M.Buffered.Store(int64(len(p.buf)))
		}
		if ctx.Err() != nil {
			continue
		}
		if len(p.buf) >= p.opt.BatchSize || (len(p.buf) > 0 && !time.Now().Before(deadline)) {
			held, err := p.flush(ctx)
			if err != nil {
				if ctx.Err() != nil {
					continue // shutdown path flushes with a fresh context
				}
				return err
			}
			if held {
				deadline = time.Now().Add(p.opt.BatchInterval) // keep polling until the recorded range is complete
				continue
			}
		}
		if !time.Now().Before(deadline) || len(p.buf) == 0 && len(msgs) == 0 {
			deadline = time.Now().Add(p.opt.BatchInterval)
		}
	}
}

func (p *Pipeline) shutdown() error {
	defer p.src.Close()
	if len(p.buf) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.opt.ShutdownTimeout)
	defer cancel()
	p.holdSince = time.Time{} // if a recorded range is still incomplete the flush is held: nothing is committed and the buffer is redelivered
	if _, err := p.flush(ctx); err != nil {
		return fmt.Errorf("shutdown flush (offsets not committed, will be redelivered): %w", err)
	}
	return nil
}

// flush maps, inserts, quarantines and commits the current buffer. On error the
// buffer is kept (nothing committed) so the same call can be retried identically.
// held=true means nothing was done because a recorded chunk range is not fully buffered yet.
func (p *Pipeline) flush(ctx context.Context) (held bool, err error) {
	msgs := p.buf
	now := p.opt.Now()

	// Per-partition ordered messages.
	byPart := map[partKey][]Message{}
	var order []partKey
	for _, m := range msgs {
		k := partKey{m.Topic, m.Partition}
		if _, ok := byPart[k]; !ok {
			order = append(order, k)
		}
		byPart[k] = append(byPart[k], m)
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].topic != order[j].topic {
			return order[i].topic < order[j].topic
		}
		return order[i].part < order[j].part
	})
	for _, k := range order {
		sort.Slice(byPart[k], func(i, j int) bool { return byPart[k][i].Offset < byPart[k][j].Offset })
	}

	if p.opt.Intents != nil {
		if held, err := p.prepareIntents(ctx, order, byPart); held || err != nil {
			return held, err
		}
	}
	p.holdSince = time.Time{}

	type segment struct {
		k           partKey
		first, last int64
		rows        [][]byte
		recorded    bool // range came from a durable intent
	}
	var segs []*segment
	var bad []Quarantined
	for _, k := range order {
		ms := byPart[k]
		cut := int64(-1) // last offset of the first segment when an intent applies
		if r, ok := p.pending[k]; ok && ms[0].Offset == r.First {
			cut = r.Last
		}
		delete(p.pending, k)
		var cur *segment
		for _, m := range ms {
			if cur == nil || (cut >= 0 && cur.recorded && m.Offset > cut) {
				cur = &segment{k: k, first: m.Offset, recorded: cut >= 0 && cur == nil}
				segs = append(segs, cur)
			}
			cur.last = m.Offset
			line, code, reason := MapMessage(m, now)
			if code != "" {
				bad = append(bad, Quarantined{Msg: m, Code: code, Reason: reason})
				continue
			}
			cur.rows = append(cur.rows, line)
		}
	}

	for _, sg := range segs {
		if len(sg.rows) == 0 {
			continue
		}
		if p.opt.Intents != nil && !sg.recorded {
			rg := Range{sg.first, sg.last}
			if err := p.retry(ctx, "record intent", func() error { return p.opt.Intents.Record(ctx, sg.k.topic, sg.k.part, rg) }); err != nil {
				return false, err
			}
		}
		c := Chunk{Token: Token(sg.k.topic, sg.k.part, sg.first, sg.last), Topic: sg.k.topic, Partition: sg.k.part,
			FirstOffset: sg.first, LastOffset: sg.last, Rows: sg.rows}
		if err := p.retry(ctx, "insert", func() error { return p.snk.Insert(ctx, c) }); err != nil {
			p.M.InsertFailures.Add(1)
			return false, err
		}
		p.M.Inserted.Add(int64(len(c.Rows)))
	}
	if len(bad) > 0 {
		if err := p.retry(ctx, "quarantine", func() error { return p.qr.Quarantine(ctx, bad) }); err != nil {
			return false, err
		}
		p.M.Quarantined.Add(int64(len(bad)))
	}
	if err := p.retry(ctx, "commit", func() error { return p.src.Commit(ctx, msgs) }); err != nil {
		p.M.CommitFailures.Add(1)
		return false, err
	}
	for _, k := range order {
		ms := byPart[k]
		p.next[k] = ms[len(ms)-1].Offset + 1
	}
	p.M.Commits.Add(1)
	p.M.Batches.Add(1)
	p.buf = nil
	p.M.Buffered.Store(0)
	return false, nil
}

// prepareIntents looks up the latest recorded range of every partition whose buffered offsets do not continue from
// the last commit (first sight, restart, rebalance) and decides whether the buffer holds the whole recorded range.
// A recorded range is applied only if the buffer starts exactly at its first offset; a range that starts earlier
// than the buffer was already committed and is dropped. If the range is not fully buffered the flush is held, up to
// IntentWait, after which the range is abandoned (rows may then be stored twice; readers dedup via LIMIT 1 BY).
func (p *Pipeline) prepareIntents(ctx context.Context, order []partKey, byPart map[partKey][]Message) (bool, error) {
	for _, k := range order {
		ms := byPart[k]
		first, last := ms[0].Offset, ms[len(ms)-1].Offset
		if n, ok := p.next[k]; !ok || n != first {
			r, found, err := p.opt.Intents.Latest(ctx, k.topic, k.part)
			if err != nil {
				return false, fmt.Errorf("load intent: %w", err)
			}
			delete(p.pending, k)
			if found && r.First == first {
				p.pending[k] = r
			}
			p.next[k] = first // looked up for this position; do not repeat while the flush is held
		}
		if r, ok := p.pending[k]; ok && last < r.Last {
			if p.holdSince.IsZero() {
				p.holdSince = time.Now()
			}
			if time.Since(p.holdSince) < p.opt.IntentWait {
				return true, nil
			}
			log.Printf("recorded chunk %s/%d [%d,%d] not redelivered within %s; abandoning (possible duplicate rows)",
				k.topic, k.part, r.First, r.Last, p.opt.IntentWait)
			delete(p.pending, k)
		}
	}
	return false, nil
}

func (p *Pipeline) retry(ctx context.Context, what string, fn func() error) error {
	backoff := p.opt.RetryBackoff
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s: %w (context done: %v)", what, err, ctx.Err())
		}
		if p.opt.MaxRetries > 0 && attempt >= p.opt.MaxRetries {
			return fmt.Errorf("%s failed after %d retries: %w", what, attempt, err)
		}
		p.M.InsertRetries.Add(1)
		log.Printf("%s failed (attempt %d): %v; retrying in %s", what, attempt+1, err, backoff)
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > p.opt.RetryBackoffMax {
			backoff = p.opt.RetryBackoffMax
		}
	}
}
