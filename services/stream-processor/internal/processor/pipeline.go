package processor

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	ec "github.com/astryon/event-contracts"
)

// Record is one record to produce.
type Record struct {
	Topic string
	Key   []byte
	Value []byte
}

// Source is the pull-based input (Kafka in production). Poll returns (nil, nil)
// when ctx expires with nothing to deliver. Commit marks the inputs (and all
// earlier offsets of their partitions) done.
type Source interface {
	Poll(ctx context.Context) ([]Input, error)
	Commit(ctx context.Context, ins []Input) error
	Close()
}

// Producer synchronously produces records with durable (acks=all) semantics.
type Producer interface {
	Produce(ctx context.Context, recs []Record) error
}

// Options configure the runner.
type Options struct {
	Normalizer      Normalizer
	OutputTopic     string
	QuarantineTopic string
	DLQTopic        string
	BatchInterval   time.Duration
	MaxRetries      int // 0 = retry until shutdown
	RetryBackoff    time.Duration
	RetryBackoffMax time.Duration
	ExpireEvery     time.Duration // idle window expiry cadence (default 30s)
}

// Pipeline wires Source -> Plan -> Producer -> Commit.
type Pipeline struct {
	Src  Source
	Prod Producer
	Win  *Window
	Opt  Options
	M    *Metrics
	Now  func() time.Time
}

// NewPipeline builds a pipeline with default clock and metrics.
func NewPipeline(src Source, prod Producer, win *Window, opt Options) *Pipeline {
	return &Pipeline{Src: src, Prod: prod, Win: win, Opt: opt, M: NewMetrics(win), Now: time.Now}
}

// Run loops until ctx is cancelled (clean, returns nil) or a fatal error occurs.
// Offsets are committed only after every output record of the batch is durably
// produced: at-least-once. A crash between produce and commit re-processes the
// batch; the window is not yet updated for it only if the process died, so
// duplicates may reach processed.events.v1 and are absorbed by the ClickHouse sink.
func (p *Pipeline) Run(ctx context.Context) error {
	defer p.Src.Close()
	lastExpire := p.Now()
	every := p.Opt.ExpireEvery
	if every <= 0 {
		every = 30 * time.Second
	}
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, p.Opt.BatchInterval)
		ins, err := p.Src.Poll(pctx)
		cancel()
		if err != nil {
			return err
		}
		if now := p.Now(); now.Sub(lastExpire) >= every {
			p.Win.Expire(now)
			lastExpire = now
		}
		if len(ins) == 0 {
			continue
		}
		if err := p.ProcessBatch(ctx, ins); err != nil {
			if ctx.Err() != nil {
				return nil // shutdown mid-batch: uncommitted, will be re-read
			}
			return err
		}
	}
	return nil
}

// ProcessBatch processes one polled batch end to end.
func (p *Pipeline) ProcessBatch(ctx context.Context, ins []Input) error {
	now := p.Now()
	plan := Plan(p.Opt.Normalizer, p.Win, ins, now)
	var recs []Record
	for _, r := range plan.Results {
		switch r.Kind {
		case KindOut:
			recs = append(recs, Record{Topic: p.Opt.OutputTopic, Key: r.Key, Value: r.Value})
		case KindQuarantine:
			recs = append(recs, Record{Topic: p.Opt.QuarantineTopic, Key: r.In.Key, Value: sideRecord("quarantine", r)})
		case KindDLQ:
			recs = append(recs, Record{Topic: p.Opt.DLQTopic, Key: r.In.Key, Value: sideRecord("dlq", r)})
		}
	}
	if len(recs) > 0 {
		if err := p.produceRetry(ctx, recs); err != nil {
			return err
		}
	}
	plan.Commit(p.Win, now) // only after durable produce
	p.M.observe(plan)
	return p.commitRetry(ctx, ins)
}

func (p *Pipeline) produceRetry(ctx context.Context, recs []Record) error {
	return p.retry(ctx, "produce", func() error { return p.Prod.Produce(ctx, recs) })
}

func (p *Pipeline) commitRetry(ctx context.Context, ins []Input) error {
	return p.retry(ctx, "commit", func() error { return p.Src.Commit(ctx, ins) })
}

func (p *Pipeline) retry(ctx context.Context, what string, f func() error) error {
	backoff := p.Opt.RetryBackoff
	for attempt := 1; ; attempt++ {
		err := f()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p.M.Retries.Add(1)
		if p.Opt.MaxRetries > 0 && attempt >= p.Opt.MaxRetries {
			return errors.New(what + " failed after retries: " + err.Error())
		}
		log.Printf("%s attempt %d failed: %v", what, attempt, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; p.Opt.RetryBackoffMax > 0 && backoff > p.Opt.RetryBackoffMax {
			backoff = p.Opt.RetryBackoffMax
		}
	}
}

// sideRecord builds a quarantine/DLQ value. The original bytes are carried for
// replay; the reason never contains payload content.
func sideRecord(kind string, r Result) []byte {
	b, _ := json.Marshal(map[string]any{
		"reason_code": r.Code, "reason": r.Reason, "stage": "stream-processor", "kind": kind,
		"source_topic": r.In.Topic, "source_partition": r.In.Partition, "source_offset": r.In.Offset,
		"quarantined_at": time.Now().UTC().Format(time.RFC3339Nano), "original": rawOrString(r.In.Value),
	})
	return b
}

func rawOrString(v []byte) json.RawMessage {
	if json.Valid(v) {
		return v
	}
	b, _ := json.Marshal(string(v))
	return b
}

// WarmKey extracts the dedup key from a processed.events.v1 value (for state rebuild).
func WarmKey(value []byte) string {
	var e ec.Envelope
	if json.Unmarshal(value, &e) != nil {
		return ""
	}
	return e.DedupKey()
}
