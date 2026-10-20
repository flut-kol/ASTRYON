package processor

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka is the franz-go implementation of Source and Producer. One client:
// consumer group with autocommit disabled; idempotent producer (franz-go default)
// with acks=all. Offsets are committed explicitly after produce.
type Kafka struct {
	cl      *kgo.Client
	maxPoll int
}

// NewKafka connects. extra options (TLS/SASL) can be appended by the caller.
func NewKafka(c Config, extra ...kgo.Opt) (*Kafka, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.GroupID),
		kgo.ConsumeTopics(c.InputTopic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()), // acks=all (idempotence needs it)
		kgo.ProducerBatchMaxBytes(2 << 20),
		kgo.ProducerLinger(5 * time.Millisecond),
	}
	cl, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, err
	}
	return &Kafka{cl: cl, maxPoll: c.BatchSize}, nil
}

func (k *Kafka) Poll(ctx context.Context) ([]Input, error) {
	fs := k.cl.PollRecords(ctx, k.maxPoll)
	if fs.IsClientClosed() {
		return nil, errors.New("kafka client closed")
	}
	if ctx.Err() == nil {
		if errs := fs.Errors(); len(errs) > 0 && fs.NumRecords() == 0 {
			return nil, errs[0].Err
		}
	}
	var out []Input
	fs.EachRecord(func(r *kgo.Record) {
		out = append(out, Input{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value})
	})
	return out, nil
}

func (k *Kafka) Commit(ctx context.Context, ins []Input) error {
	m := map[string]map[int32]kgo.EpochOffset{}
	for _, x := range ins {
		if m[x.Topic] == nil {
			m[x.Topic] = map[int32]kgo.EpochOffset{}
		}
		if cur, ok := m[x.Topic][x.Partition]; !ok || x.Offset+1 > cur.Offset {
			m[x.Topic][x.Partition] = kgo.EpochOffset{Epoch: -1, Offset: x.Offset + 1}
		}
	}
	var cerr error
	k.cl.CommitOffsetsSync(ctx, m, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			cerr = err
			return
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if e := kerr.ErrorForCode(p.ErrorCode); e != nil {
					cerr = e
				}
			}
		}
	})
	return cerr
}

func (k *Kafka) Close() { k.cl.Close() }

// Produce synchronously produces all records; returns the first error.
func (k *Kafka) Produce(ctx context.Context, recs []Record) error {
	rs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		rs[i] = &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
	}
	return k.cl.ProduceSync(ctx, rs...).FirstErr()
}

// WarmWindow rebuilds dedup state by replaying processed.events.v1 records from
// now-ttl (by Kafka timestamp) until the topic is idle for idle. Because the
// window only ever holds keys of events already emitted, this restores exactly
// the state a never-restarted instance would have (up to partition ordering).
// Returns the number of keys loaded.
func WarmWindow(ctx context.Context, c Config, w *Window, idle time.Duration, extra ...kgo.Opt) (int, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumeTopics(c.OutputTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(time.Now().Add(-c.DedupTTL).UnixMilli())),
	}
	cl, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	n := 0
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, idle)
		fs := cl.PollRecords(pctx, 10000)
		cancel()
		if fs.IsClientClosed() {
			return n, errors.New("kafka client closed")
		}
		if fs.NumRecords() == 0 {
			return n, nil
		}
		fs.EachRecord(func(r *kgo.Record) {
			if k := WarmKey(r.Value); k != "" {
				w.Add(k, r.Timestamp)
				n++
			}
		})
	}
	return n, ctx.Err()
}
