package sink

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaSource consumes with a consumer group, autocommit disabled; offsets are
// committed explicitly after a successful insert.
type KafkaSource struct {
	cl       *kgo.Client
	maxPoll  int
	quarTopc string
}

func NewKafka(c Config) (*KafkaSource, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.GroupID),
		kgo.ConsumeTopics(c.Topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaSource{cl: cl, maxPoll: c.BatchSize, quarTopc: c.QuarantineTopic}, nil
}

func (k *KafkaSource) Poll(ctx context.Context) ([]Message, error) {
	fs := k.cl.PollRecords(ctx, k.maxPoll)
	if fs.IsClientClosed() {
		return nil, errors.New("kafka client closed")
	}
	if ctx.Err() != nil {
		// deadline/cancel: deliver anything already fetched, no error
		var out []Message
		fs.EachRecord(func(r *kgo.Record) { out = append(out, toMsg(r)) })
		return out, nil
	}
	if errs := fs.Errors(); len(errs) > 0 {
		// Partition-level errors are mostly transient; surface only when nothing was fetched.
		if fs.NumRecords() == 0 {
			return nil, errs[0].Err
		}
	}
	var out []Message
	fs.EachRecord(func(r *kgo.Record) { out = append(out, toMsg(r)) })
	return out, nil
}

func toMsg(r *kgo.Record) Message {
	return Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value}
}

func (k *KafkaSource) Commit(ctx context.Context, msgs []Message) error {
	m := map[string]map[int32]kgo.EpochOffset{}
	for _, x := range msgs {
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

func (k *KafkaSource) Close() { k.cl.Close() }

// Quarantine produces rejected messages to the quarantine topic (synchronously, acks=all).
func (k *KafkaSource) Quarantine(ctx context.Context, q []Quarantined) error {
	recs := make([]*kgo.Record, len(q))
	for i, x := range q {
		// Original bytes are carried as-is for replay; reason never contains payload content.
		b, _ := json.Marshal(map[string]any{
			"reason_code": x.Code, "reason": x.Reason, "stage": "clickhouse-sink",
			"source_topic": x.Msg.Topic, "source_partition": x.Msg.Partition, "source_offset": x.Msg.Offset,
			"quarantined_at": time.Now().UTC().Format(time.RFC3339Nano), "original": json.RawMessage(rawOrString(x.Msg.Value)),
		})
		recs[i] = &kgo.Record{Topic: k.quarTopc, Key: x.Msg.Key, Value: b}
	}
	return k.cl.ProduceSync(ctx, recs...).FirstErr()
}

func rawOrString(v []byte) []byte {
	if json.Valid(v) {
		return v
	}
	b, _ := json.Marshal(string(v))
	return b
}
