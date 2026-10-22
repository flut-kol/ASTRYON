package sink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	ec "github.com/astryon/event-contracts"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func (c Config) tlsMaterial() tlsMaterial {
	return tlsMaterial{Prefix: "CHSINK_KAFKA", CAFile: c.KafkaTLSCAFile, CertFile: c.KafkaTLSCertFile, KeyFile: c.KafkaTLSKeyFile, Interval: c.KafkaTLSReloadInterval}
}

// kafkaTLSConfig is the startup snapshot of the client TLS config (chain and hostname verified; never
// InsecureSkipVerify). Connections use kafkaTLSOpt, which also reloads renewed files.
func (c Config) kafkaTLSConfig() (*tls.Config, error) { return staticTLSConfig(c.tlsMaterial()) }

func (c Config) kafkaSASL() sasl.Mechanism {
	switch c.KafkaSASLMech {
	case "PLAIN":
		return plain.Auth{User: c.KafkaSASLUser, Pass: c.KafkaSASLPass}.AsMechanism()
	case "SCRAM-SHA-256":
		return scram.Auth{User: c.KafkaSASLUser, Pass: c.KafkaSASLPass}.AsSha256Mechanism()
	case "SCRAM-SHA-512":
		return scram.Auth{User: c.KafkaSASLUser, Pass: c.KafkaSASLPass}.AsSha512Mechanism()
	}
	return nil
}

// connErrs remembers the latest broker connect/handshake failure (TLS verify, SASL auth),
// which franz-go otherwise retries silently. The configured password is redacted.
// connErrTTL: a recorded connect error older than this is considered recovered.
const connErrTTL = 30 * time.Second

type connErrs struct {
	last atomic.Value // string
	at   atomic.Int64 // unix nanos of the last stored error
	pass string
}

// OnBrokerConnect implements kgo.HookBrokerConnect.
func (h *connErrs) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil {
		return // errors age out (see lastErr): TCP/TLS connect succeeds before SASL runs
	}
	h.store("broker "+meta.Host, err)
}

// Level and Log implement kgo.Logger at error level only: franz-go reports a failed
// SASL handshake solely through its logger.
func (h *connErrs) Level() kgo.LogLevel { return kgo.LogLevelError }

func (h *connErrs) Log(_ kgo.LogLevel, msg string, kv ...any) {
	if msg != "unable to initialize sasl" {
		return
	}
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i] == "err" {
			if e, ok := kv[i+1].(error); ok {
				h.store("sasl", e)
			}
		}
	}
}

func (h *connErrs) store(where string, err error) {
	msg := err.Error()
	if h.pass != "" {
		msg = strings.ReplaceAll(msg, h.pass, "***")
	}
	h.last.Store(where + ": " + msg)
	h.at.Store(time.Now().UnixNano())
}

func (h *connErrs) lastErr() string {
	if h == nil || time.Since(time.Unix(0, h.at.Load())) > connErrTTL {
		return ""
	}
	s, _ := h.last.Load().(string)
	return s
}

// permanent returns the last connect error only when retrying cannot help without operator
// action (rejected credentials, untrusted certificate); refused/reset during a restart is not.
func (h *connErrs) permanent() string {
	s := h.lastErr()
	if strings.HasPrefix(s, "sasl:") || strings.Contains(s, "x509") || strings.Contains(s, "certificate") {
		return s
	}
	return ""
}

// KafkaSource consumes with a consumer group, autocommit disabled; offsets are
// committed explicitly after a successful insert.
type KafkaSource struct {
	cl       *kgo.Client
	maxPoll  int
	quarTopc string
	dlqTopc  string // tenantless rejections (no QuarantineRecordV1 possible)
	conn     *connErrs
	wait     time.Duration // bounds Quarantine's wait for a produce ack
	tls      bool
}

func NewKafka(c Config) (*KafkaSource, error) {
	if err := c.validateKafkaSecurity(); err != nil {
		return nil, err
	}
	ce := &connErrs{pass: c.KafkaSASLPass}
	wait := c.QuarantineTimeout
	if wait <= 0 {
		wait = defaultQuarantineTimeout
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.GroupID),
		kgo.ConsumeTopics(c.Topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(max(wait, time.Second)), // franz-go minimum is 1s
		kgo.WithHooks(ce), kgo.WithLogger(ce),
	}
	if c.KafkaTLS {
		tlsOpt, err := kafkaTLSOpt(c.tlsMaterial())
		if err != nil {
			return nil, err
		}
		opts = append(opts, tlsOpt)
	}
	if m := c.kafkaSASL(); m != nil {
		opts = append(opts, kgo.SASL(m))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &KafkaSource{cl: cl, maxPoll: c.BatchSize, quarTopc: c.QuarantineTopic, dlqTopc: dlqTopic(c), conn: ce, wait: wait, tls: c.KafkaTLS}, nil
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
		if len(out) == 0 {
			// Idle because the broker rejects us (bad credentials, untrusted certificate): fail
			// loudly rather than looking like an empty topic.
			if s := k.conn.permanent(); s != "" {
				return nil, fmt.Errorf("kafka cannot reach broker: %s", s)
			}
		}
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

func dlqTopic(c Config) string {
	if c.DLQTopic != "" {
		return c.DLQTopic
	}
	return ec.TopicDLQ
}

// Quarantine produces rejected messages as QuarantineRecordV1 to the quarantine
// topic, or to the DLQ when no tenant is known (synchronously, acks=all).
func (k *KafkaSource) Quarantine(ctx context.Context, q []Quarantined) error {
	recs, err := quarantineRecords(q, time.Now(), k.quarTopc, k.dlqTopc)
	if err != nil {
		return err
	}
	return k.produce(ctx, recs)
}

const defaultQuarantineTimeout = 15 * time.Second

const tlsTimeoutHint = "a broker that rejects our TLS/mTLS certificate or ACLs often looks like a plain timeout; check the client certificate, CA and topic ACLs"

// produce bounds the wait by ctx and k.wait: ProduceSync alone can block well past both while
// franz-go retries a metadata fetch against an unreachable cluster. On failure nothing is
// acked, so the caller must not commit source offsets; a late write is a duplicate that
// replay tolerates (at-least-once).
func (k *KafkaSource) produce(ctx context.Context, recs []*kgo.Record) error {
	pctx, cancel := context.WithTimeout(ctx, k.wait)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- k.cl.ProduceSync(pctx, recs...).FirstErr() }()
	var err error
	select {
	case err = <-done:
	case <-pctx.Done():
		err = pctx.Err()
	}
	if err == nil {
		return nil
	}
	timeout := errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "timed out")
	if timeout && ctx.Err() == nil {
		err = fmt.Errorf("quarantine produce not acked within %s: %w", k.wait, err)
	} else {
		err = fmt.Errorf("quarantine produce failed: %w", err)
	}
	if timeout {
		if s := k.conn.lastErr(); s != "" {
			err = fmt.Errorf("%w (last broker connect error: %s)", err, s)
		} else if k.tls {
			err = fmt.Errorf("%w (%s)", err, tlsTimeoutHint)
		}
	}
	return err
}
