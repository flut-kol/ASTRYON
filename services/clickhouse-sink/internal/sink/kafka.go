package sink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func (c Config) kafkaTLSConfig() (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12} // chain and hostname verified; never InsecureSkipVerify
	if c.KafkaTLSCAFile != "" {
		pem, err := os.ReadFile(c.KafkaTLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read CHSINK_KAFKA_TLS_CA_FILE: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CHSINK_KAFKA_TLS_CA_FILE: no valid PEM certificates")
		}
		tc.RootCAs = pool
	}
	return tc, nil
}

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
	conn     *connErrs
}

func NewKafka(c Config) (*KafkaSource, error) {
	if err := c.validateKafkaSecurity(); err != nil {
		return nil, err
	}
	ce := &connErrs{pass: c.KafkaSASLPass}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.GroupID),
		kgo.ConsumeTopics(c.Topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.WithHooks(ce), kgo.WithLogger(ce),
	}
	if c.KafkaTLS {
		tc, err := c.kafkaTLSConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	if m := c.kafkaSASL(); m != nil {
		opts = append(opts, kgo.SASL(m))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &KafkaSource{cl: cl, maxPoll: c.BatchSize, quarTopc: c.QuarantineTopic, conn: ce}, nil
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
	err := k.cl.ProduceSync(ctx, recs...).FirstErr()
	if s := k.conn.lastErr(); err != nil && s != "" && (errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "timed out")) {
		return fmt.Errorf("%w (last broker connect error: %s)", err, s)
	}
	return err
}

func rawOrString(v []byte) []byte {
	if json.Valid(v) {
		return v
	}
	b, _ := json.Marshal(string(v))
	return b
}
