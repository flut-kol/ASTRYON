package worker

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

// KafkaConfig is loaded from env only (ASTRYON_KAFKA_* as in the gateway).
type KafkaConfig struct {
	Brokers     []string
	ClientID    string
	TLS         bool
	TLSCAFile   string
	TLSCertFile string // optional client certificate (mTLS); with TLSKeyFile
	TLSKeyFile  string
	// TLSReloadInterval: how often changed CA/cert/key files are re-read (0 = never).
	TLSReloadInterval time.Duration
	SASLMech          string
	SASLUser          string
	SASLPass          string
	DeliveryTimeout   time.Duration
}

func LoadKafkaConfig(getenv func(string) string) (KafkaConfig, error) {
	c := KafkaConfig{
		ClientID:        "astryon-replay-worker",
		TLSCAFile:       getenv("ASTRYON_KAFKA_TLS_CA_FILE"),
		SASLMech:        strings.ToUpper(strings.TrimSpace(getenv("ASTRYON_KAFKA_SASL_MECHANISM"))),
		SASLUser:        getenv("ASTRYON_KAFKA_SASL_USERNAME"),
		SASLPass:        getenv("ASTRYON_KAFKA_SASL_PASSWORD"),
		DeliveryTimeout: 30 * time.Second,
	}
	if v := getenv("ASTRYON_KAFKA_CLIENT_ID"); v != "" {
		c.ClientID = v
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

// connErrs remembers the latest broker connect/handshake failure (TLS verify, SASL auth)
// so a timeout or idle poll can say why. The configured password is redacted.
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

func (h *connErrs) annotate(err error) error {
	if s := h.lastErr(); err != nil && s != "" && (errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "timed out")) {
		return fmt.Errorf("%w (last broker connect error: %s)", err, s)
	}
	return err
}

func (c KafkaConfig) baseOpts(ce *connErrs) ([]kgo.Opt, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.ClientID), kgo.WithHooks(ce), kgo.WithLogger(ce)}
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
	return opts, nil
}

// KafkaJobSource is a consumer-group JobSource with auto-commit disabled.
type KafkaJobSource struct {
	cl   *kgo.Client
	conn *connErrs
}

func NewKafkaJobSource(c KafkaConfig, group, topic string) (*KafkaJobSource, error) {
	ce := &connErrs{pass: c.SASLPass}
	opts, err := c.baseOpts(ce)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &KafkaJobSource{cl: cl, conn: ce}, nil
}

func (k *KafkaJobSource) Poll(ctx context.Context) (Delivery, bool, error) {
	fs := k.cl.PollRecords(ctx, 1)
	if fs.IsClientClosed() {
		return Delivery{}, false, errors.New("kafka client closed")
	}
	var got *kgo.Record
	fs.EachRecord(func(r *kgo.Record) {
		if got == nil {
			got = r
		}
	})
	if got == nil {
		if ctx.Err() != nil {
			// An idle poll that ends while the broker rejects us (bad credentials, untrusted
			// certificate) is an error, not "no work": surface it instead of idling silently.
			if s := k.conn.permanent(); s != "" {
				return Delivery{}, false, fmt.Errorf("kafka consumer cannot reach broker: %s", s)
			}
			return Delivery{}, false, nil
		}
		if errs := fs.Errors(); len(errs) > 0 {
			return Delivery{}, false, errs[0].Err
		}
		return Delivery{}, false, nil
	}
	return Delivery{Topic: got.Topic, Partition: got.Partition, Offset: got.Offset, Key: got.Key, Value: got.Value, Ref: got}, true, nil
}

func (k *KafkaJobSource) Commit(ctx context.Context, d Delivery) error {
	r, ok := d.Ref.(*kgo.Record)
	if !ok {
		return errors.New("delivery without kafka record")
	}
	return k.cl.CommitRecords(ctx, r)
}

func (k *KafkaJobSource) Close() { k.cl.Close() }

// KafkaSink produces with acks=all and the idempotent producer; Publish blocks until every record is acked.
type KafkaSink struct {
	cl   *kgo.Client
	conn *connErrs
}

func NewKafkaSink(c KafkaConfig) (*KafkaSink, error) {
	ce := &connErrs{pass: c.SASLPass}
	opts, err := c.baseOpts(ce)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordRetries(1<<30),
		kgo.RecordDeliveryTimeout(c.DeliveryTimeout), kgo.ProduceRequestTimeout(10*time.Second),
		kgo.ProducerLinger(5*time.Millisecond))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &KafkaSink{cl: cl, conn: ce}, nil
}

func (s *KafkaSink) Publish(ctx context.Context, recs []Record) error {
	krs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		krs[i] = &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
	}
	return s.conn.annotate(s.cl.ProduceSync(ctx, krs...).FirstErr())
}

func (s *KafkaSink) Close() { s.cl.Close() }
