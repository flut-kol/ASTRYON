package gateway

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

// KafkaConfig is the producer configuration, loaded from env only.
type KafkaConfig struct {
	Brokers     []string
	ClientID    string
	TLS         bool
	TLSCAFile   string // optional PEM bundle; system roots if empty
	TLSCertFile string // optional client certificate (mTLS); with TLSKeyFile
	TLSKeyFile  string
	// TLSReloadInterval: how often changed CA/cert/key files are re-read (0 = never).
	TLSReloadInterval time.Duration
	SASLMech          string // "", PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUser          string
	SASLPass          string
	// DeliveryTimeout bounds one record from produce call to ack (incl. retries).
	DeliveryTimeout time.Duration
}

// LoadKafkaConfig reads ASTRYON_KAFKA_* variables:
//
//	ASTRYON_KAFKA_BROKERS (required, comma list), ASTRYON_KAFKA_CLIENT_ID,
//	ASTRYON_KAFKA_TLS=1, ASTRYON_KAFKA_TLS_CA_FILE,
//	ASTRYON_KAFKA_SASL_MECHANISM, ASTRYON_KAFKA_SASL_USERNAME, ASTRYON_KAFKA_SASL_PASSWORD,
//	ASTRYON_KAFKA_DELIVERY_TIMEOUT (Go duration, default 30s).
func LoadKafkaConfig(getenv func(string) string) (KafkaConfig, error) {
	c := KafkaConfig{
		ClientID:        def(getenv("ASTRYON_KAFKA_CLIENT_ID"), "astryon-gateway"),
		TLSCAFile:       getenv("ASTRYON_KAFKA_TLS_CA_FILE"),
		SASLMech:        strings.ToUpper(strings.TrimSpace(getenv("ASTRYON_KAFKA_SASL_MECHANISM"))),
		SASLUser:        getenv("ASTRYON_KAFKA_SASL_USERNAME"),
		SASLPass:        getenv("ASTRYON_KAFKA_SASL_PASSWORD"),
		DeliveryTimeout: 30 * time.Second,
	}
	tlsOn, err := parseTLSFlag(getenv("ASTRYON_KAFKA_TLS"))
	if err != nil {
		return c, err
	}
	c.TLS = tlsOn
	c.TLSCertFile = getenv("ASTRYON_KAFKA_TLS_CERT_FILE")
	c.TLSKeyFile = getenv("ASTRYON_KAFKA_TLS_KEY_FILE")
	if c.TLSReloadInterval, err = parseTLSReloadInterval("ASTRYON_KAFKA", getenv("ASTRYON_KAFKA_TLS_RELOAD_INTERVAL")); err != nil {
		return c, err
	}
	for _, b := range strings.Split(getenv("ASTRYON_KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.Brokers = append(c.Brokers, b)
		}
	}
	if len(c.Brokers) == 0 {
		return c, errors.New("ASTRYON_KAFKA_BROKERS is required (comma-separated host:port list)")
	}
	if v := getenv("ASTRYON_KAFKA_DELIVERY_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return c, errors.New("ASTRYON_KAFKA_DELIVERY_TIMEOUT must be a positive duration")
		}
		c.DeliveryTimeout = d
	}
	switch c.SASLMech {
	case "":
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if c.SASLUser == "" || c.SASLPass == "" {
			return c, errors.New("ASTRYON_KAFKA_SASL_MECHANISM requires ASTRYON_KAFKA_SASL_USERNAME and ASTRYON_KAFKA_SASL_PASSWORD")
		}
	default:
		return c, fmt.Errorf("unsupported ASTRYON_KAFKA_SASL_MECHANISM %q", c.SASLMech)
	}
	// Fail closed: never let a security setting be silently ignored.
	if c.TLSCAFile != "" && !c.TLS {
		return c, errors.New("ASTRYON_KAFKA_TLS_CA_FILE is set but ASTRYON_KAFKA_TLS is off (refusing to fall back to plaintext)")
	}
	if c.SASLMech == "PLAIN" && !c.TLS {
		return c, errors.New("ASTRYON_KAFKA_SASL_MECHANISM=PLAIN requires ASTRYON_KAFKA_TLS=1 (password would be sent in clear)")
	}
	if err := c.tlsMaterial().validate(c.TLS); err != nil {
		return c, err
	}
	return c, nil
}

// parseTLSFlag accepts 1/true/0/false/empty; anything else is an error so a typo
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

// kafkaProducer is the seam between KafkaPublisher and the client library.
// produce must return nil only after the broker acknowledged (acks=all).
type kafkaProducer interface {
	produce(ctx context.Context, topic string, key, value []byte) error
	close()
}

// connErrs remembers the latest broker connect/handshake failure (TLS verify, SASL auth)
// so a timed-out publish can say why, instead of only "context deadline exceeded".
// The configured password is redacted from the stored text.
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
	// wait bounds the caller's wait; franz-go can block past RecordDeliveryTimeout
	// and ctx while it retries a metadata fetch against an unreachable cluster.
	wait time.Duration
}

func (p franzProducer) produce(ctx context.Context, topic string, key, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, p.wait)
	defer cancel()
	// ProduceSync blocks until the record is acked or permanently failed; run it aside so the
	// caller gets "no ack" at the deadline. The record may still land later: clients must retry
	// with the same Idempotency-Key (stable event_id), so a late write is a deduplicated duplicate.
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

// KafkaPublisher is the production Publisher: acks=all, idempotent producer
// (franz-go default; requires acks=all), retries within DeliveryTimeout.
type KafkaPublisher struct {
	p kafkaProducer
}

// NewKafkaPublisher builds the client. It does not require the broker to be
// reachable at construction; Publish fails (no ack) until it is.
func NewKafkaPublisher(c KafkaConfig) (*KafkaPublisher, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotence is on by default; do NOT call DisableIdempotentWrite.
		kgo.RecordRetries(1 << 30), // bounded by RecordDeliveryTimeout instead
		kgo.RecordDeliveryTimeout(c.DeliveryTimeout),
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.RetryBackoffFn(func(n int) time.Duration {
			d := 100 * time.Millisecond << min(n, 5)
			return d
		}),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<10, true, true, nil)),
		kgo.ProducerLinger(0), // latency-first: each Publish is awaited individually
	}
	if c.TLS {
		tlsOpt, err := kafkaTLSOpt(c.tlsMaterial())
		if err != nil {
			return nil, err
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
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &KafkaPublisher{p: franzProducer{conn: ce, cl: cl, wait: c.DeliveryTimeout}}, nil
}

// Publish returns nil only after durable broker acknowledgement.
func (k *KafkaPublisher) Publish(ctx context.Context, topic string, key, value []byte) error {
	if err := k.p.produce(ctx, topic, key, value); err != nil {
		return fmt.Errorf("kafka publish to %s: %w", topic, err)
	}
	return nil
}

// Close flushes buffered records (none are left unacked by Publish, as it is
// synchronous) and releases connections. Safe to call once at shutdown.
func (k *KafkaPublisher) Close() { k.p.close() }

var _ Publisher = (*KafkaPublisher)(nil)
