package worker

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Live SASL_SSL tests against a secured broker (see docs/operations/kafka-tls-sasl-test.md).
// Skipped unless REPLAYW_KAFKA_IT_BROKER is set. Env: REPLAYW_KAFKA_IT_{BROKER,CA,OTHER_CA,USER,PASS,
// DENIED_USER,DENIED_PASS,CONTAINER}; topics ksec.replay.jobs / ksec.replay.out must exist.
type kafkaITEnv struct {
	broker, ca, otherCA, user, pass, deniedUser, deniedPass, container string
}

const (
	kitJobs = "ksec.replay.jobs"
	kitOut  = "ksec.replay.out"
)

func loadKafkaIT(t *testing.T) kafkaITEnv {
	t.Helper()
	g := os.Getenv
	e := kafkaITEnv{g("REPLAYW_KAFKA_IT_BROKER"), g("REPLAYW_KAFKA_IT_CA"), g("REPLAYW_KAFKA_IT_OTHER_CA"), g("REPLAYW_KAFKA_IT_USER"),
		g("REPLAYW_KAFKA_IT_PASS"), g("REPLAYW_KAFKA_IT_DENIED_USER"), g("REPLAYW_KAFKA_IT_DENIED_PASS"), g("REPLAYW_KAFKA_IT_CONTAINER")}
	if e.broker == "" {
		t.Skip("REPLAYW_KAFKA_IT_BROKER not set")
	}
	return e
}

func (e kafkaITEnv) cfg(mech, user, pass, ca string) KafkaConfig {
	return KafkaConfig{Brokers: []string{e.broker}, ClientID: "rw-it", TLS: true, TLSCAFile: ca, SASLMech: mech,
		SASLUser: user, SASLPass: pass, DeliveryTimeout: 8 * time.Second}
}

func kitPublish(c KafkaConfig, topic, val string, timeout time.Duration) error {
	s, err := NewKafkaSink(c)
	if err != nil {
		return err
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Publish(ctx, []Record{{Topic: topic, Key: []byte("k"), Value: []byte(val)}})
}

// consume polls until the wanted value is delivered (and commits it), within timeout.
func kitConsume(c KafkaConfig, group, topic, want string, timeout time.Duration) error {
	src, err := NewKafkaJobSource(c, group, topic)
	if err != nil {
		return err
	}
	defer src.Close()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		d, ok, err := src.Poll(ctx)
		cancel()
		if err != nil {
			return err
		}
		if ok && string(d.Value) == want {
			cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer ccancel()
			return src.Commit(cctx, d)
		}
	}
	return context.DeadlineExceeded
}

func TestIT_SASLSSL_RoundTrip_AllMechanisms(t *testing.T) {
	e := loadKafkaIT(t)
	for _, mech := range []string{"SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN"} {
		t.Run(mech, func(t *testing.T) {
			c := e.cfg(mech, e.user, e.pass, e.ca)
			val := "rt-" + mech + "-" + time.Now().Format("150405.000000")
			if err := kitPublish(c, kitJobs, val, 15*time.Second); err != nil {
				t.Fatalf("publish: %v", err)
			}
			if err := kitConsume(c, "ksec-rw-"+val, kitJobs, val, 30*time.Second); err != nil {
				t.Fatalf("consume+commit: %v", err)
			}
		})
	}
}

func TestIT_WrongPassword_FailsClosedNonLeaky(t *testing.T) {
	e := loadKafkaIT(t)
	const bad = "definitely-wrong-pw-9f3a"
	for _, mech := range []string{"SCRAM-SHA-512", "PLAIN"} {
		c := e.cfg(mech, e.user, bad, e.ca)
		err := kitPublish(c, kitOut, "must-not-land", 12*time.Second)
		if err == nil {
			t.Fatalf("%s: publish with wrong password succeeded", mech)
		}
		if strings.Contains(err.Error(), bad) || strings.Contains(err.Error(), e.pass) {
			t.Fatalf("%s: error leaks a password: %v", mech, err)
		}
		if !strings.Contains(strings.ToUpper(err.Error()), "SASL") {
			t.Fatalf("%s: error does not say why: %v", mech, err)
		}
		t.Logf("sink %s -> %v", mech, err)
		cerr := kitConsume(c, "ksec-rw-bad-"+mech, kitJobs, "x", 6*time.Second)
		if cerr == nil || !strings.Contains(strings.ToUpper(cerr.Error()), "SASL") || strings.Contains(cerr.Error(), bad) {
			t.Fatalf("%s: consumer must surface the SASL failure, got %v", mech, cerr)
		}
		t.Logf("source %s -> %v", mech, cerr)
	}
}

func TestIT_NoSASLOrNoTLS_AgainstSecuredBroker_Fails(t *testing.T) {
	e := loadKafkaIT(t)
	noSASL := e.cfg("", "", "", e.ca)
	noTLS := e.cfg("SCRAM-SHA-512", e.user, e.pass, "")
	noTLS.TLS = false
	for name, c := range map[string]KafkaConfig{"tls-without-sasl": noSASL, "sasl-without-tls": noTLS} {
		if err := kitPublish(c, kitOut, "x", 8*time.Second); err == nil {
			t.Fatalf("%s: publish unexpectedly succeeded", name)
		}
	}
}

func TestIT_UntrustedCA_Fails(t *testing.T) {
	e := loadKafkaIT(t)
	if e.otherCA == "" {
		t.Skip("REPLAYW_KAFKA_IT_OTHER_CA not set")
	}
	for name, ca := range map[string]string{"other-ca": e.otherCA, "system-roots": ""} {
		c := e.cfg("SCRAM-SHA-512", e.user, e.pass, ca)
		err := kitPublish(c, kitOut, "x", 8*time.Second)
		if err == nil || (!strings.Contains(err.Error(), "certificate") && !strings.Contains(err.Error(), "x509")) {
			t.Fatalf("%s: want certificate failure, got %v", name, err)
		}
		cerr := kitConsume(c, "ksec-rw-ca-"+name, kitJobs, "x", 6*time.Second)
		if cerr == nil || !strings.Contains(cerr.Error(), "certificate") {
			t.Fatalf("%s: consumer want certificate failure, got %v", name, cerr)
		}
		t.Logf("%s -> %v", name, err)
	}
}

func TestIT_NoACL_AuthorizationErrorSurfaced(t *testing.T) {
	e := loadKafkaIT(t)
	if e.deniedUser == "" {
		t.Skip("REPLAYW_KAFKA_IT_DENIED_USER not set")
	}
	c := e.cfg("SCRAM-SHA-512", e.deniedUser, e.deniedPass, e.ca)
	start := time.Now()
	err := kitPublish(c, kitOut, "denied", 15*time.Second)
	if err == nil || !strings.Contains(strings.ToUpper(err.Error()), "AUTHORIZATION_FAILED") {
		t.Fatalf("producer: want authorization failure, got %v", err)
	}
	if d := time.Since(start); d > 12*time.Second {
		t.Fatalf("took %v (hang)", d)
	}
	t.Logf("sink no-ACL -> %v", err)
	cerr := kitConsume(c, "ksec-rw-denied", kitJobs, "x", 10*time.Second)
	if cerr == nil || !strings.Contains(strings.ToUpper(cerr.Error()), "AUTHORIZATION") {
		t.Fatalf("consumer: want authorization failure, got %v", cerr)
	}
	t.Logf("source no-ACL -> %v", cerr)
}

func TestIT_ReconnectAfterBrokerRestart(t *testing.T) {
	e := loadKafkaIT(t)
	if e.container == "" {
		t.Skip("REPLAYW_KAFKA_IT_CONTAINER not set")
	}
	c := e.cfg("SCRAM-SHA-512", e.user, e.pass, e.ca)
	s, err := NewKafkaSink(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	pub := func(v string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return s.Publish(ctx, []Record{{Topic: kitOut, Value: []byte(v)}})
	}
	if err := pub("before-restart"); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("docker", "restart", e.container).CombinedOutput(); err != nil {
		t.Fatalf("docker restart: %v %s", err, out)
	}
	var last error
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		if last = pub("after-restart"); last == nil {
			break
		}
	}
	if last != nil {
		t.Fatalf("sink did not reconnect: %v", last)
	}
	// consumer: same client survives a second restart? Keep it simple: a fresh source must read the record.
	if err := kitConsume(c, "ksec-rw-reconn-"+time.Now().Format("150405.000"), kitOut, "after-restart", 30*time.Second); err != nil {
		t.Fatalf("consume after restart: %v", err)
	}
}
