package replay

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Live SASL_SSL tests against a secured broker (see docs/operations/kafka-tls-sasl-test.md). Skipped unless
// REPLAY_KAFKA_TLS_IT_BROKER is set. Env: REPLAY_KAFKA_TLS_IT_BROKER=127.0.0.1:29094, _CA=<ca.pem>, _OTHER_CA=<unrelated ca.pem>,
// _USER, _PASS (ALL on topics replay.jobs.v1 and audit.events.v1, SCRAM-SHA-512).
type tlsIT struct{ broker, ca, otherCA, user, pass string }

func loadTLSIT(t *testing.T) tlsIT {
	t.Helper()
	g := os.Getenv
	e := tlsIT{g("REPLAY_KAFKA_TLS_IT_BROKER"), g("REPLAY_KAFKA_TLS_IT_CA"), g("REPLAY_KAFKA_TLS_IT_OTHER_CA"), g("REPLAY_KAFKA_TLS_IT_USER"), g("REPLAY_KAFKA_TLS_IT_PASS")}
	if e.broker == "" {
		t.Skip("REPLAY_KAFKA_TLS_IT_BROKER not set")
	}
	return e
}

// cfg goes through LoadKafkaConfig so the env parsing is part of the test.
func (e tlsIT) cfg(t *testing.T, user, pass, ca string) KafkaConfig {
	t.Helper()
	env := map[string]string{"ASTRYON_KAFKA_BROKERS": e.broker, "ASTRYON_KAFKA_TLS": "true", "ASTRYON_KAFKA_TLS_CA_FILE": ca,
		"ASTRYON_KAFKA_SASL_MECHANISM": "SCRAM-SHA-512", "ASTRYON_KAFKA_SASL_USERNAME": user, "ASTRYON_KAFKA_SASL_PASSWORD": pass,
		"ASTRYON_KAFKA_DELIVERY_TIMEOUT": "8s"}
	c, err := LoadKafkaConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// seen reports whether a record with the given value is on the topic (independent consumer).
func (e tlsIT) seen(t *testing.T, topic, want string) bool {
	t.Helper()
	pem, _ := os.ReadFile(e.ca)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.broker), kgo.DialTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}),
		kgo.SASL(scram.Auth{User: e.user, Pass: e.pass}.AsSha512Mechanism()),
		kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		found := false
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			if strings.Contains(string(r.Value), want) {
				found = true
			}
		})
		if found {
			return true
		}
	}
	return false
}

func TestIT_SASLSSL_RoundTrip_JobPublisherAndAuditSink(t *testing.T) {
	e := loadTLSIT(t)
	cfg := e.cfg(t, e.user, e.pass, e.ca)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := time.Now().Format("150405.000000000")

	jp, err := NewKafkaJobPublisher(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer jp.Close()
	job := ReplayJob{JobID: "tlsit-" + id, TenantID: "11111111-1111-4111-8111-111111111111", SourceID: "22222222-2222-4222-8222-222222222222", ProcessingVersion: 2}
	if err := jp.Publish(ctx, job); err != nil {
		t.Fatalf("job publish: %v", err)
	}
	if !e.seen(t, TopicReplayJobs, job.JobID) {
		t.Fatal("job not found on replay.jobs.v1")
	}

	as, err := NewKafkaAuditSink(cfg, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer as.Close()
	ev := AuditEvent{TenantID: job.TenantID, Action: "replay.tlsit." + id}
	if err := as.Record(ctx, ev); err != nil {
		t.Fatalf("audit record: %v", err)
	}
	if !e.seen(t, TopicAudit, ev.Action) {
		b, _ := json.Marshal(ev)
		t.Fatalf("audit event %s not found on %s", b, TopicAudit)
	}
}

func TestIT_WrongPasswordAndUntrustedCA_FailWithCause(t *testing.T) {
	e := loadTLSIT(t)
	const bad = "definitely-wrong-pw-9f3a"
	job := ReplayJob{JobID: "must-not-land-" + time.Now().Format("150405.000"), TenantID: "t", SourceID: "s"}
	ev := AuditEvent{TenantID: "t", Action: "must-not-land"}
	type tc struct {
		name string
		cfg  KafkaConfig
		want []string // any of
	}
	cases := []tc{{"wrong-password", e.cfg(t, e.user, bad, e.ca), []string{"SASL", "AUTHENTICATION"}}}
	if e.otherCA != "" {
		cases = append(cases, tc{"untrusted-ca", e.cfg(t, e.user, e.pass, e.otherCA), []string{"x509", "certificate"}})
	}
	for _, c := range cases {
		jp, err := NewKafkaJobPublisher(c.cfg)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		err = jp.Publish(context.Background(), job)
		jp.Close()
		check := func(who string, err error) {
			if err == nil {
				t.Fatalf("%s/%s: succeeded", c.name, who)
			}
			if strings.Contains(err.Error(), bad) || strings.Contains(err.Error(), e.pass) {
				t.Fatalf("%s/%s: error leaks a password: %v", c.name, who, err)
			}
			up, ok := strings.ToUpper(err.Error()), false
			for _, w := range c.want {
				ok = ok || strings.Contains(up, strings.ToUpper(w))
			}
			if !ok {
				t.Fatalf("%s/%s: error does not name the cause: %v", c.name, who, err)
			}
			t.Logf("%s/%s -> %v", c.name, who, err)
		}
		check("job", err)
		if d := time.Since(start); d > 11*time.Second {
			t.Fatalf("%s: took %v (hang)", c.name, d)
		}
		as, err := NewKafkaAuditSink(c.cfg, 8*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		err = as.Record(context.Background(), ev)
		as.Close()
		check("audit", err)
	}
	if e.seen(t, TopicReplayJobs, job.JobID) || e.seen(t, TopicAudit, "must-not-land") {
		t.Fatal("a record from a failed client reached the topic")
	}
}
