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
}

// Pipeline: Source -> map -> batch -> Sink (+Quarantiner) -> commit.
type Pipeline struct {
	src Source
	snk Sink
	qr  Quarantiner
	opt Options
	M   *Metrics

	buf []Message
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
	return &Pipeline{src: src, snk: snk, qr: qr, opt: opt, M: &Metrics{}}
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
			if err := p.flush(ctx); err != nil {
				if ctx.Err() != nil {
					continue // shutdown path flushes with a fresh context
				}
				return err
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
	if err := p.flush(ctx); err != nil {
		return fmt.Errorf("shutdown flush (offsets not committed, will be redelivered): %w", err)
	}
	return nil
}

// flush maps, inserts, quarantines and commits the current buffer. On error the
// buffer is kept (nothing committed) so the same call can be retried identically.
func (p *Pipeline) flush(ctx context.Context) error {
	msgs := p.buf
	now := p.opt.Now()

	type group struct {
		topic       string
		part        int32
		first, last int64
		rows        [][]byte
	}
	groups := map[[2]int64]*group{}
	topicIdx := map[string]int64{}
	var bad []Quarantined
	for _, m := range msgs {
		ti, ok := topicIdx[m.Topic]
		if !ok {
			ti = int64(len(topicIdx))
			topicIdx[m.Topic] = ti
		}
		k := [2]int64{ti, int64(m.Partition)}
		g := groups[k]
		if g == nil {
			g = &group{topic: m.Topic, part: m.Partition, first: m.Offset, last: m.Offset}
			groups[k] = g
		}
		if m.Offset < g.first {
			g.first = m.Offset
		}
		if m.Offset > g.last {
			g.last = m.Offset
		}
		line, code, reason := MapMessage(m, now)
		if code != "" {
			bad = append(bad, Quarantined{Msg: m, Code: code, Reason: reason})
			continue
		}
		g.rows = append(g.rows, line)
	}
	keys := make([][2]int64, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})

	for _, k := range keys {
		g := groups[k]
		if len(g.rows) == 0 {
			continue
		}
		c := Chunk{Token: Token(g.topic, g.part, g.first, g.last), Topic: g.topic, Partition: g.part,
			FirstOffset: g.first, LastOffset: g.last, Rows: g.rows}
		if err := p.retry(ctx, "insert", func() error { return p.snk.Insert(ctx, c) }); err != nil {
			p.M.InsertFailures.Add(1)
			return err
		}
		p.M.Inserted.Add(int64(len(c.Rows)))
	}
	if len(bad) > 0 {
		if err := p.retry(ctx, "quarantine", func() error { return p.qr.Quarantine(ctx, bad) }); err != nil {
			return err
		}
		p.M.Quarantined.Add(int64(len(bad)))
	}
	if err := p.retry(ctx, "commit", func() error { return p.src.Commit(ctx, msgs) }); err != nil {
		p.M.CommitFailures.Add(1)
		return err
	}
	p.M.Commits.Add(1)
	p.M.Batches.Add(1)
	p.buf = nil
	p.M.Buffered.Store(0)
	return nil
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
