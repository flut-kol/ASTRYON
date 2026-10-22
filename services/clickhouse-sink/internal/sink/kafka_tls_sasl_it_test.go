package sink

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Live SASL_SSL tests against a secured broker (see docs/operations/kafka-tls-sasl-test.md).
// Skipped unless CHSINK_KAFKA_IT_BROKER is set. Env: CHSINK_KAFKA_IT_{BROKER,CA,OTHER_CA,USER,PASS,
// DENIED_USER,DENIED_PASS,CONTAINER}; topic ksec.sink.quar must exist (used as both source and quarantine).
type kafkaITEnv struct {
	broker, ca, otherCA, user, pass, deniedUser, deniedPass, container string
}

const kitTopic = "ksec.sink.quar"

func loadKafkaIT(t *testing.T) kafkaITEnv {
	t.Helper()
	g := os.Getenv
	e := kafkaITEnv{g("CHSINK_KAFKA_IT_BROKER"), g("CHSINK_KAFKA_IT_CA"), g("CHSINK_KAFKA_IT_OTHER_CA"), g("CHSINK_KAFKA_IT_USER"),
		g("CHSINK_KAFKA_IT_PASS"), g("CHSINK_KAFKA_IT_DENIED_USER"), g("CHSINK_KAFKA_IT_DENIED_PASS"), g("CHSINK_KAFKA_IT_CONTAINER")}
	if e.broker == "" {
		t.Skip("CHSINK_KAFKA_IT_BROKER not set")
	}
	return e
}

func (e kafkaITEnv) cfg(group, mech, user, pass, ca string) Config {
	return Config{Brokers: []string{e.broker}, Topic: kitTopic, QuarantineTopic: kitTopic, GroupID: group, BatchSize: 10,
		KafkaTLS: true, KafkaTLSCAFile: ca, KafkaSASLMech: mech, KafkaSASLUser: user, KafkaSASLPass: pass}
}

func kitQuarantine(c Config, reason string, timeout time.Duration) error {
	k, err := NewKafka(c)
	if err != nil {
		return err
	}
	defer k.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return k.Quarantine(ctx, []Quarantined{{Code: "ERR_IT", Reason: reason, Msg: Message{Topic: "src", Value: []byte(`{"tenant_id":"11111111-1111-4111-8111-111111111111"}`)}}})
}

// kitConsume polls until a record containing want arrives, then commits it.
func kitConsume(c Config, want string, timeout time.Duration) error {
	k, err := NewKafka(c)
	if err != nil {
		return err
	}
	defer k.Close()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		msgs, err := k.Poll(ctx)
		cancel()
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if strings.Contains(string(m.Value), want) {
				cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer ccancel()
				return k.Commit(cctx, []Message{m})
			}
		}
	}
	return context.DeadlineExceeded
}

func TestIT_SASLSSL_RoundTrip_AllMechanisms(t *testing.T) {
	e := loadKafkaIT(t)
	for _, mech := range []string{"SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN"} {
		t.Run(mech, func(t *testing.T) {
			tag := "rt-" + mech + "-" + time.Now().Format("150405.000000")
			c := e.cfg("ksec-sink-"+tag, mech, e.user, e.pass, e.ca)
			if err := kitQuarantine(c, tag, 15*time.Second); err != nil {
				t.Fatalf("produce: %v", err)
			}
			if err := kitConsume(c, tag, 30*time.Second); err != nil {
				t.Fatalf("consume+commit: %v", err)
			}
		})
	}
}

func TestIT_WrongPassword_FailsClosedNonLeaky(t *testing.T) {
	e := loadKafkaIT(t)
	const bad = "definitely-wrong-pw-9f3a"
	for _, mech := range []string{"SCRAM-SHA-512", "PLAIN"} {
		c := e.cfg("ksec-sink-bad-"+mech, mech, e.user, bad, e.ca)
		err := kitConsume(c, "x", 8*time.Second)
		if err == nil || !strings.Contains(strings.ToUpper(err.Error()), "SASL") || strings.Contains(err.Error(), bad) || strings.Contains(err.Error(), e.pass) {
			t.Fatalf("%s consumer: want non-leaky SASL failure, got %v", mech, err)
		}
		t.Logf("source %s -> %v", mech, err)
		perr := kitQuarantine(c, "bad", 8*time.Second)
		if perr == nil || strings.Contains(perr.Error(), bad) || !strings.Contains(strings.ToUpper(perr.Error()), "SASL") {
			t.Fatalf("%s producer: want non-leaky SASL failure, got %v", mech, perr)
		}
		t.Logf("quarantine %s -> %v", mech, perr)
	}
}

func TestIT_NoSASLOrNoTLS_AgainstSecuredBroker_Fails(t *testing.T) {
	e := loadKafkaIT(t)
	noSASL := e.cfg("ksec-sink-nosasl", "", "", "", e.ca)
	noTLS := e.cfg("ksec-sink-notls", "SCRAM-SHA-512", e.user, e.pass, "")
	noTLS.KafkaTLS = false
	for name, c := range map[string]Config{"tls-without-sasl": noSASL, "sasl-without-tls": noTLS} {
		if err := kitQuarantine(c, "x", 8*time.Second); err == nil {
			t.Fatalf("%s: unexpectedly succeeded", name)
		}
	}
}

func TestIT_UntrustedCA_Fails(t *testing.T) {
	e := loadKafkaIT(t)
	if e.otherCA == "" {
		t.Skip("CHSINK_KAFKA_IT_OTHER_CA not set")
	}
	for name, ca := range map[string]string{"other-ca": e.otherCA, "system-roots": ""} {
		c := e.cfg("ksec-sink-ca-"+name, "SCRAM-SHA-512", e.user, e.pass, ca)
		err := kitConsume(c, "x", 8*time.Second)
		if err == nil || !strings.Contains(err.Error(), "certificate") {
			t.Fatalf("%s: want certificate failure, got %v", name, err)
		}
		t.Logf("%s -> %v", name, err)
	}
}

func TestIT_NoACL_AuthorizationErrorSurfaced(t *testing.T) {
	e := loadKafkaIT(t)
	if e.deniedUser == "" {
		t.Skip("CHSINK_KAFKA_IT_DENIED_USER not set")
	}
	c := e.cfg("ksec-sink-denied", "SCRAM-SHA-512", e.deniedUser, e.deniedPass, e.ca)
	start := time.Now()
	err := kitQuarantine(c, "x", 15*time.Second)
	if err == nil || !strings.Contains(strings.ToUpper(err.Error()), "AUTHORIZATION_FAILED") {
		t.Fatalf("producer: want authorization failure, got %v", err)
	}
	if time.Since(start) > 12*time.Second {
		t.Fatal("authorization failure took too long (hang)")
	}
	cerr := kitConsume(c, "x", 10*time.Second)
	if cerr == nil || !strings.Contains(strings.ToUpper(cerr.Error()), "AUTHORIZATION") {
		t.Fatalf("consumer: want authorization failure, got %v", cerr)
	}
	t.Logf("no-ACL producer -> %v; consumer -> %v", err, cerr)
}

func TestIT_ReconnectAfterBrokerRestart(t *testing.T) {
	e := loadKafkaIT(t)
	if e.container == "" {
		t.Skip("CHSINK_KAFKA_IT_CONTAINER not set")
	}
	tag := "reconn-" + time.Now().Format("150405.000000")
	c := e.cfg("ksec-sink-"+tag, "SCRAM-SHA-512", e.user, e.pass, e.ca)
	k, err := NewKafka(c)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	quar := func(reason string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return k.Quarantine(ctx, []Quarantined{{Code: "ERR_IT", Reason: reason, Msg: Message{Topic: "src", Value: []byte(`{"tenant_id":"11111111-1111-4111-8111-111111111111"}`)}}})
	}
	if err := quar("before-" + tag); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("docker", "restart", e.container).CombinedOutput(); err != nil {
		t.Fatalf("docker restart: %v %s", err, out)
	}
	var last error
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		if last = quar("after-" + tag); last == nil {
			break
		}
	}
	if last != nil {
		t.Fatalf("same client did not reconnect: %v", last)
	}
	// the same long-lived consumer-group client must resume polling after the restart
	var got bool
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline) && !got; {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		msgs, err := k.Poll(ctx)
		cancel()
		if err != nil {
			t.Fatalf("poll after restart: %v", err)
		}
		for _, m := range msgs {
			if strings.Contains(string(m.Value), "after-"+tag) {
				got = true
			}
		}
	}
	if !got {
		t.Fatal("same consumer did not see the record produced after the restart")
	}
}
