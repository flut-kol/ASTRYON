package gateway

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Live SASL_SSL tests against a secured broker (see docs/operations/kafka-tls-sasl-test.md).
// Skipped unless GATEWAY_KAFKA_IT_BROKER is set. Env:
//
//	GATEWAY_KAFKA_IT_BROKER=127.0.0.1:29093  GATEWAY_KAFKA_IT_CA=<ca.pem>  GATEWAY_KAFKA_IT_OTHER_CA=<unrelated ca.pem>
//	GATEWAY_KAFKA_IT_USER=app GATEWAY_KAFKA_IT_PASS=...  GATEWAY_KAFKA_IT_DENIED_USER=denied GATEWAY_KAFKA_IT_DENIED_PASS=...
//	GATEWAY_KAFKA_IT_TOPIC=ksec.gw (optional) GATEWAY_KAFKA_IT_CONTAINER=<docker container to restart> (optional)
type itEnv struct {
	broker, ca, otherCA, user, pass, deniedUser, deniedPass, topic, container string
}

func loadIT(t *testing.T) itEnv {
	t.Helper()
	g := os.Getenv
	e := itEnv{g("GATEWAY_KAFKA_IT_BROKER"), g("GATEWAY_KAFKA_IT_CA"), g("GATEWAY_KAFKA_IT_OTHER_CA"), g("GATEWAY_KAFKA_IT_USER"),
		g("GATEWAY_KAFKA_IT_PASS"), g("GATEWAY_KAFKA_IT_DENIED_USER"), g("GATEWAY_KAFKA_IT_DENIED_PASS"), g("GATEWAY_KAFKA_IT_TOPIC"), g("GATEWAY_KAFKA_IT_CONTAINER")}
	if e.broker == "" {
		t.Skip("GATEWAY_KAFKA_IT_BROKER not set")
	}
	if e.topic == "" {
		e.topic = "ksec.gw"
	}
	return e
}

func (e itEnv) cfg(mech, user, pass, ca string) KafkaConfig {
	return KafkaConfig{Brokers: []string{e.broker}, ClientID: "gw-it", TLS: true, TLSCAFile: ca, SASLMech: mech,
		SASLUser: user, SASLPass: pass, DeliveryTimeout: 8 * time.Second}
}

// consumeOne reads the record with the given value from the topic as an independent (admin-ish) client.
func (e itEnv) consumeOne(t *testing.T, want string) bool {
	t.Helper()
	pem, _ := os.ReadFile(e.ca)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.broker), kgo.DialTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}),
		kgo.SASL(scram.Auth{User: e.user, Pass: e.pass}.AsSha512Mechanism()),
		kgo.ConsumeTopics(e.topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		found := false
		fs.EachRecord(func(r *kgo.Record) {
			if string(r.Value) == want {
				found = true
			}
		})
		if found {
			return true
		}
	}
	return false
}

func TestIT_SASLSSL_RoundTrip_AllMechanisms(t *testing.T) {
	e := loadIT(t)
	for _, mech := range []string{"SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN"} {
		t.Run(mech, func(t *testing.T) {
			p, err := NewKafkaPublisher(e.cfg(mech, e.user, e.pass, e.ca))
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			val := "rt-" + mech + "-" + time.Now().Format("150405.000000")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := p.Publish(ctx, e.topic, []byte("k"), []byte(val)); err != nil {
				t.Fatalf("publish: %v", err)
			}
			if !e.consumeOne(t, val) {
				t.Fatalf("record %q not found by consumer", val)
			}
		})
	}
}

func TestIT_WrongPassword_FailsClosedNonLeaky(t *testing.T) {
	e := loadIT(t)
	const bad = "definitely-wrong-pw-9f3a"
	for _, mech := range []string{"SCRAM-SHA-512", "PLAIN"} {
		p, err := NewKafkaPublisher(e.cfg(mech, e.user, bad, e.ca))
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		err = p.Publish(ctx, e.topic, nil, []byte("must-not-land"))
		cancel()
		p.Close()
		if err == nil {
			t.Fatalf("%s: publish with wrong password succeeded", mech)
		}
		if strings.Contains(err.Error(), bad) || strings.Contains(err.Error(), e.pass) {
			t.Fatalf("%s: error leaks a password: %v", mech, err)
		}
		if d := time.Since(start); d > 11*time.Second {
			t.Fatalf("%s: took %v (hang)", mech, d)
		}
		if !strings.Contains(strings.ToUpper(err.Error()), "SASL") && !strings.Contains(strings.ToUpper(err.Error()), "AUTHENTICATION") {
			t.Fatalf("%s: error does not say why (authentication): %v", mech, err)
		}
		t.Logf("%s wrong password -> %v (%v)", mech, err, time.Since(start).Round(time.Millisecond))
	}
	if e.consumeOne(t, "must-not-land") {
		t.Fatal("record published with wrong password reached the topic")
	}
}

func TestIT_NoSASLOrNoTLS_AgainstSecuredBroker_Fails(t *testing.T) {
	e := loadIT(t)
	cases := map[string]KafkaConfig{
		"tls-without-sasl": e.cfg("", "", "", e.ca),
		"sasl-without-tls": func() KafkaConfig { c := e.cfg("SCRAM-SHA-512", e.user, e.pass, ""); c.TLS = false; return c }(),
	}
	for name, c := range cases {
		p, err := NewKafkaPublisher(c)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		err = p.Publish(ctx, e.topic, nil, []byte("x"))
		cancel()
		p.Close()
		if err == nil {
			t.Fatalf("%s: publish unexpectedly succeeded", name)
		}
		t.Logf("%s -> %v", name, err)
	}
}

func TestIT_UntrustedCA_Fails(t *testing.T) {
	e := loadIT(t)
	if e.otherCA == "" {
		t.Skip("GATEWAY_KAFKA_IT_OTHER_CA not set")
	}
	for name, ca := range map[string]string{"other-ca": e.otherCA, "system-roots": ""} {
		p, err := NewKafkaPublisher(e.cfg("SCRAM-SHA-512", e.user, e.pass, ca))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		err = p.Publish(ctx, e.topic, nil, []byte("x"))
		cancel()
		p.Close()
		if err == nil {
			t.Fatalf("%s: publish succeeded against untrusted broker cert", name)
		}
		if !strings.Contains(err.Error(), "certificate") && !strings.Contains(err.Error(), "x509") {
			t.Fatalf("%s: error does not mention the certificate failure: %v", name, err)
		}
		t.Logf("%s -> %v", name, err)
	}
}

func TestIT_NoACL_AuthorizationErrorSurfaced(t *testing.T) {
	e := loadIT(t)
	if e.deniedUser == "" {
		t.Skip("GATEWAY_KAFKA_IT_DENIED_USER not set")
	}
	p, err := NewKafkaPublisher(e.cfg("SCRAM-SHA-512", e.deniedUser, e.deniedPass, e.ca))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = p.Publish(ctx, e.topic, nil, []byte("denied"))
	if err == nil {
		t.Fatal("publish by user without ACL succeeded")
	}
	if d := time.Since(start); d > 12*time.Second {
		t.Fatalf("authorization failure took %v (hang)", d)
	}
	t.Logf("no-ACL -> %v (%v)", err, time.Since(start).Round(time.Millisecond))
	if !strings.Contains(strings.ToUpper(err.Error()), "AUTHORIZATION") && !strings.Contains(strings.ToUpper(err.Error()), "NOT_AUTHORIZED") {
		t.Fatalf("error does not name an authorization failure: %v", err)
	}
}

func TestIT_ReconnectAfterBrokerRestart(t *testing.T) {
	e := loadIT(t)
	if e.container == "" {
		t.Skip("GATEWAY_KAFKA_IT_CONTAINER not set")
	}
	p, err := NewKafkaPublisher(e.cfg("SCRAM-SHA-512", e.user, e.pass, e.ca))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	pub := func(v string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return p.Publish(ctx, e.topic, nil, []byte(v))
	}
	if err := pub("before-restart"); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("docker", "restart", e.container).CombinedOutput(); err != nil {
		t.Fatalf("docker restart: %v %s", err, out)
	}
	var last error
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if last = pub("after-restart"); last == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if last != nil {
		t.Fatalf("no reconnect after restart: %v", last)
	}
	if !e.consumeOne(t, "after-restart") {
		t.Fatal("after-restart record missing")
	}
}
