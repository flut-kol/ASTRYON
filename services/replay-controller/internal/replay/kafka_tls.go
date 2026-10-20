package replay

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Shared Kafka client security + producer code for KafkaJobPublisher and KafkaAuditSink.

// parseTLSFlag accepts 1/true/0/false/empty (case-insensitive); anything else is an error so a typo
// (e.g. "yes") cannot silently downgrade the connection to plaintext.
func parseTLSFlag(v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "0", "false":
		return false, nil
	case "1", "true":
		return true, nil
	}
	return false, fmt.Errorf("ASTRYON_KAFKA_TLS must be 1/true or 0/false, got %q", v)
}

// tlsMaterial describes the TLS files (CA bundle, optional client certificate) and reload period.
func (c KafkaConfig) tlsMaterial() tlsMaterial {
	return tlsMaterial{Prefix: "ASTRYON_KAFKA", CAFile: c.TLSCAFile, CertFile: c.TLSCertFile, KeyFile: c.TLSKeyFile, Interval: c.TLSReloadInterval}
}

// tlsConfig builds the startup snapshot of the client TLS config: system roots or the given CA bundle,
// optional client certificate; server certificate chain and hostname always verified (no InsecureSkipVerify).
// Connections use kafkaTLSOpt, which also reloads renewed files.
func (c KafkaConfig) tlsConfig() (*tls.Config, error) { return staticTLSConfig(c.tlsMaterial()) }

func (c KafkaConfig) saslMechanism() sasl.Mechanism {
	switch c.SASLMech {
	case "PLAIN":
		return plain.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsMechanism()
	case "SCRAM-SHA-256":
		return scram.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsSha256Mechanism()
	case "SCRAM-SHA-512":
		return scram.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsSha512Mechanism()
	}
	return nil
}

// connErrTTL: a recorded connect error older than this is considered recovered.
const connErrTTL = 30 * time.Second

// connErrs remembers the latest broker connect/handshake failure (TLS verify, SASL auth)
// so a timed-out publish can say why, instead of only "context deadline exceeded".
// The configured password is redacted from the stored text.
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

// Level and Log implement kgo.Logger at error level only: franz-go reports a failed SASL
// handshake (wrong password, mechanism not enabled) solely through its logger.
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

func (h *connErrs) annotate(err error) error {
	if err == nil || h == nil {
		return err
	}
	if s := h.lastErr(); s != "" && (errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "timed out")) {
		return fmt.Errorf("%w (last broker connect error: %s)", err, s)
	}
	return err
}

type franzProducer struct {
	conn *connErrs
	cl   *kgo.Client
	// wait bounds the caller's wait; franz-go can block past RecordDeliveryTimeout and ctx
	// while it retries a metadata fetch against an unreachable cluster.
	wait time.Duration
}

func (p franzProducer) produce(ctx context.Context, topic string, key, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, p.wait)
	defer cancel()
	// ProduceSync blocks until acked or permanently failed; run it aside so the caller gets
	// "no ack" at the deadline. The record may still land later (callers must tolerate duplicates).
	done := make(chan error, 1)
	go func() {
		done <- p.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr()
	}()
	select {
	case err := <-done:
		return p.conn.annotate(err)
	case <-ctx.Done():
		return p.conn.annotate(ctx.Err())
	}
}

func (p franzProducer) close() { p.cl.Close() }

// newFranzProducer builds the acks=all idempotent producer shared by the job publisher and the audit sink
// (same TLS/SASL env config). The broker need not be reachable yet.
func newFranzProducer(c KafkaConfig) (franzProducer, error) {
	if c.DeliveryTimeout <= 0 {
		c.DeliveryTimeout = 30 * time.Second
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(1 << 30), // bounded by RecordDeliveryTimeout instead
		kgo.RecordDeliveryTimeout(c.DeliveryTimeout),
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.RetryBackoffFn(func(n int) time.Duration { return 100 * time.Millisecond << min(n, 5) }),
		kgo.ProducerLinger(0),
	}
	if c.TLS {
		tlsOpt, err := kafkaTLSOpt(c.tlsMaterial())
		if err != nil {
			return franzProducer{}, err
		}
		opts = append(opts, tlsOpt)
	}
	if m := c.saslMechanism(); m != nil {
		opts = append(opts, kgo.SASL(m))
	}
	ce := &connErrs{pass: c.SASLPass}
	opts = append(opts, kgo.WithHooks(ce), kgo.WithLogger(ce))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return franzProducer{}, fmt.Errorf("kafka client: %w", err)
	}
	return franzProducer{conn: ce, cl: cl, wait: c.DeliveryTimeout}, nil
}
