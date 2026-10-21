package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ec "github.com/astryon/event-contracts"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaSource consumes a Debezium topic as a consumer group with auto-commit disabled; Commit is explicit.
type KafkaSource struct{ cl *kgo.Client }

// NewKafkaSource builds the group consumer. conn comes from kafkaemit.Config.ConnOpts.
func NewKafkaSource(conn []kgo.Opt, group, topic string) (*KafkaSource, error) {
	opts := append(append([]kgo.Opt{}, conn...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("cdc kafka client: %w", err)
	}
	return &KafkaSource{cl: cl}, nil
}

func (k *KafkaSource) Close() { k.cl.Close() }

func (k *KafkaSource) Poll(ctx context.Context, max int) ([]Record, error) {
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	f := k.cl.PollRecords(pctx, max)
	if f.IsClientClosed() {
		return nil, fmt.Errorf("client closed")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var out []Record
	f.EachRecord(func(r *kgo.Record) {
		out = append(out, Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value})
	})
	for _, e := range f.Errors() {
		if e.Err != context.DeadlineExceeded && len(out) == 0 {
			return nil, e.Err
		}
	}
	return out, nil
}

func (k *KafkaSource) Commit(ctx context.Context, recs []Record) error {
	offs := map[string]map[int32]kgo.EpochOffset{}
	for _, r := range recs {
		if offs[r.Topic] == nil {
			offs[r.Topic] = map[int32]kgo.EpochOffset{}
		}
		if cur, ok := offs[r.Topic][r.Partition]; !ok || r.Offset+1 > cur.Offset {
			offs[r.Topic][r.Partition] = kgo.EpochOffset{Epoch: -1, Offset: r.Offset + 1}
		}
	}
	var cerr error
	k.cl.CommitOffsetsSync(ctx, offs, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		cerr = err
		if err != nil || resp == nil {
			return
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if e := kerr.ErrorForCode(p.ErrorCode); e != nil && cerr == nil {
					cerr = e
				}
			}
		}
	})
	return cerr
}

// KafkaQuarantine publishes quarantine records to events.quarantine.v1.
type KafkaQuarantine struct{ cl *kgo.Client }

func NewKafkaQuarantine(conn []kgo.Opt) (*KafkaQuarantine, error) {
	cl, err := kgo.NewClient(append(append([]kgo.Opt{}, conn...), kgo.RequiredAcks(kgo.AllISRAcks()))...)
	if err != nil {
		return nil, err
	}
	return &KafkaQuarantine{cl: cl}, nil
}

func (q *KafkaQuarantine) Close() { q.cl.Close() }

func (q *KafkaQuarantine) Quarantine(ctx context.Context, rec ec.QuarantineRecordV1) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return q.cl.ProduceSync(ctx, &kgo.Record{Topic: ec.TopicQuarantine, Key: []byte(rec.TenantID + "/" + rec.DeviationID), Value: b}).FirstErr()
}
