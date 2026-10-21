package worker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Live mTLS + rotation tests against ONE secured broker whose SSL listener has ssl.client.auth=required
// (see docs/operations/kafka-tls-sasl-test.md, "mTLS and rotation"). Skipped unless REPLAYW_KAFKA_MTLS_IT_BROKER is set.
//
//	REPLAYW_KAFKA_MTLS_IT_BROKER=127.0.0.1:29094    SSL listener, client certs required
//	REPLAYW_KAFKA_MTLS_IT_CA=<ca.pem>               CA that signed the broker certificate
//	REPLAYW_KAFKA_MTLS_IT_CERT / _KEY               client certificate (CN=app) signed by the CA the broker trusts for clients
//	REPLAYW_KAFKA_MTLS_IT_TRUST_FILE=<path>         host path of the broker's client-CA truststore (bind-mounted; rotation rewrites it)
//	REPLAYW_KAFKA_MTLS_IT_CONTAINER=<name>          broker container (restarted to make it re-read the truststore)
//	REPLAYW_KAFKA_MTLS_IT_TOPIC=ksec.mtls           optional
//
// The rotation test restarts the broker: run it alone (-run TestIT_MTLS_Rotation).
type mtItEnv struct{ broker, ca, cert, key, trust, container, topic string }

func mtItLoad(t *testing.T) mtItEnv {
	t.Helper()
	g := os.Getenv
	e := mtItEnv{g("REPLAYW_KAFKA_MTLS_IT_BROKER"), g("REPLAYW_KAFKA_MTLS_IT_CA"), g("REPLAYW_KAFKA_MTLS_IT_CERT"), g("REPLAYW_KAFKA_MTLS_IT_KEY"),
		g("REPLAYW_KAFKA_MTLS_IT_TRUST_FILE"), g("REPLAYW_KAFKA_MTLS_IT_CONTAINER"), g("REPLAYW_KAFKA_MTLS_IT_TOPIC")}
	if e.broker == "" {
		t.Skip("REPLAYW_KAFKA_MTLS_IT_BROKER not set")
	}
	if e.topic == "" {
		e.topic = "ksec.mtls"
	}
	return e
}

type mtItClient interface {
	publish(ctx context.Context, val string) error
	close()
}

// mtItNew builds the service's own Kafka client (the code under test) with the given TLS files.
func mtItNew(e mtItEnv, m tlsMaterial) (mtItClient, error) { return mtItNewImpl(e, m) }

func mtItMat(e mtItEnv, ca, cert, key string, interval time.Duration) tlsMaterial {
	return tlsMaterial{Prefix: "REPLAYW_KAFKA", CAFile: ca, CertFile: cert, KeyFile: key, Interval: interval}
}

func mtItPublish(c mtItClient, val string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.publish(ctx, val)
}

// mtItRetry publishes until it succeeds (the client reconnects on its own) or the deadline passes.
func mtItRetry(t *testing.T, c mtItClient, val string, total time.Duration) {
	t.Helper()
	var err error
	for deadline := time.Now().Add(total); time.Now().Before(deadline); time.Sleep(time.Second) {
		if err = mtItPublish(c, val, 10*time.Second); err == nil {
			return
		}
	}
	t.Fatalf("client did not recover without restart: %v", err)
}

// mtItSeen reads the topic with an independent client (own cert files m) and looks for want.
func mtItSeen(t *testing.T, e mtItEnv, m tlsMaterial, want string) bool {
	t.Helper()
	opt, err := kafkaTLSOpt(m)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.broker), opt, kgo.ConsumeTopics(e.topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

func mtItCopy(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// mtItFiles copies the env client files into a private dir the test can rotate.
func mtItFiles(t *testing.T, e mtItEnv) mtFiles {
	t.Helper()
	d := t.TempDir()
	f := mtFiles{dir: d, ca: filepath.Join(d, "ca.pem"), cert: filepath.Join(d, "tls.crt"), key: filepath.Join(d, "tls.key")}
	mtItCopy(t, e.ca, f.ca)
	mtItCopy(t, e.cert, f.cert)
	mtItCopy(t, e.key, f.key)
	return f
}

func mtItTag(s string) string { return s + "-" + time.Now().Format("150405.000000") }

func TestIT_MTLS_RoundTripWithClientCert(t *testing.T) {
	e := mtItLoad(t)
	f := mtItFiles(t, e)
	m := mtItMat(e, f.ca, f.cert, f.key, time.Minute)
	c, err := mtItNew(e, m)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	val := mtItTag("mtls-rt")
	if err := mtItPublish(c, val, 15*time.Second); err != nil {
		t.Fatalf("publish with client cert: %v", err)
	}
	if !mtItSeen(t, e, m, val) {
		t.Fatalf("record %q not found by an independent mTLS consumer", val)
	}
}

func TestIT_MTLS_RejectedWithoutClientCert(t *testing.T) {
	e := mtItLoad(t)
	f := mtItFiles(t, e)
	c, err := mtItNew(e, mtItMat(e, f.ca, "", "", time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	start := time.Now()
	err = mtItPublish(c, mtItTag("must-not-land-nocert"), 10*time.Second)
	if err == nil {
		t.Fatal("broker requiring client certs accepted a client without one")
	}
	if d := time.Since(start); d > 12*time.Second {
		t.Fatalf("took %v (hang)", d)
	}
	t.Logf("no client cert -> %v", err)
}

func TestIT_MTLS_RejectedWithCertFromOtherCA(t *testing.T) {
	e := mtItLoad(t)
	f := mtItFiles(t, e)
	other := mtNewCA(t, "intruder-ca")
	c, k := other.issue(t, "app") // same CN as the real client, wrong CA
	mtWrite(t, f.cert, c)
	mtWrite(t, f.key, k)
	cl, err := mtItNew(e, mtItMat(e, f.ca, f.cert, f.key, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.close()
	err = mtItPublish(cl, mtItTag("must-not-land-othercert"), 10*time.Second)
	if err == nil {
		t.Fatal("client certificate from an untrusted CA was accepted")
	}
	t.Logf("cert from other CA -> %v", err)
}

// A running client whose CA bundle is stale (does not contain the broker's CA) recovers when the file is replaced.
func TestIT_MTLS_Rotation_ClientCABundleReplacedWhileRunning(t *testing.T) {
	e := mtItLoad(t)
	f := mtItFiles(t, e)
	stale := mtNewCA(t, "stale-broker-ca")
	mtWrite(t, f.ca, stale.pem)
	c, err := mtItNew(e, mtItMat(e, f.ca, f.cert, f.key, 500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	if err := mtItPublish(c, mtItTag("must-not-land-stale-ca"), 8*time.Second); err == nil {
		t.Fatal("publish succeeded although the client does not trust the broker CA")
	} else {
		t.Logf("stale CA -> %v", err)
	}
	mtItCopy(t, e.ca, f.ca) // CA renewed on disk; client keeps running
	val := mtItTag("mtls-ca-rotated")
	mtItRetry(t, c, val, 60*time.Second)
	if !mtItSeen(t, e, mtItMat(e, e.ca, e.cert, e.key, 0), val) {
		t.Fatal("record published after CA rotation not found")
	}
}

// Client certificate AND the broker's trusted client CA are replaced while the client runs. The broker is restarted
// to re-read its truststore; the client is not. A control client that does not reload must be rejected afterwards.
func TestIT_MTLS_Rotation_ClientCertAndBrokerTrustReplaced(t *testing.T) {
	e := mtItLoad(t)
	if e.trust == "" || e.container == "" {
		t.Skip("REPLAYW_KAFKA_MTLS_IT_TRUST_FILE / _CONTAINER not set")
	}
	origTrust, err := os.ReadFile(e.trust)
	if err != nil {
		t.Fatal(err)
	}
	restart := func() {
		if out, err := exec.Command("docker", "restart", e.container).CombinedOutput(); err != nil {
			t.Fatalf("docker restart: %v %s", err, out)
		}
	}
	restoreFiles := mtItFiles(t, e)
	t.Cleanup(func() { // restore the broker's original trust and prove the original cert works again
		if err := os.WriteFile(e.trust, origTrust, 0o644); err != nil {
			t.Errorf("restore trust file: %v", err)
			return
		}
		_ = exec.Command("docker", "restart", e.container).Run()
		rc, err := mtItNew(e, mtItMat(e, restoreFiles.ca, restoreFiles.cert, restoreFiles.key, 0))
		if err != nil {
			t.Errorf("restore check: %v", err)
			return
		}
		defer rc.close()
		mtItRetry(t, rc, mtItTag("restored"), 120*time.Second)
	})

	f := mtItFiles(t, e)
	running, err := mtItNew(e, mtItMat(e, f.ca, f.cert, f.key, 500*time.Millisecond)) // reloads
	if err != nil {
		t.Fatal(err)
	}
	defer running.close()
	g := mtItFiles(t, e)
	control, err := mtItNew(e, mtItMat(e, g.ca, g.cert, g.key, 0)) // never reloads
	if err != nil {
		t.Fatal(err)
	}
	defer control.close()
	for name, c := range map[string]mtItClient{"running": running, "control": control} {
		if err := mtItPublish(c, mtItTag("before-"+name), 15*time.Second); err != nil {
			t.Fatalf("%s before rotation: %v", name, err)
		}
	}

	// Rotate: new client CA, new client certificate (same CN), broker trust replaced.
	newCA := mtNewCA(t, "rotated-client-ca")
	nc, nk := newCA.issue(t, "app")
	if err := os.WriteFile(e.trust, newCA.pem, 0o644); err != nil {
		t.Fatal(err)
	}
	mtWrite(t, f.key, nk) // key first, cert second: the client may observe the mismatched pair and must keep the old one
	time.Sleep(1200 * time.Millisecond)
	mtWrite(t, f.cert, nc)
	restart()

	val := mtItTag("after-rotation")
	mtItRetry(t, running, val, 120*time.Second)
	if !mtItSeen(t, e, mtItMat(e, e.ca, f.cert, f.key, 0), val) {
		t.Fatal("record published after rotation not found")
	}
	if err := mtItPublish(control, mtItTag("must-not-land-control"), 10*time.Second); err == nil {
		t.Fatal("control client with the old certificate was accepted after the broker stopped trusting its CA")
	} else {
		t.Logf("control (no reload, old cert) -> %v", err)
	}
}

type mtItRW struct {
	s     *KafkaSink
	topic string
}

func (c mtItRW) publish(ctx context.Context, val string) error {
	// KafkaSink.Publish can block past ctx and RecordDeliveryTimeout while a cached-metadata partition cannot connect
	// (franz-go), so bound the wait here; Close() fails the stuck record.
	done := make(chan error, 1)
	go func() { done <- c.s.Publish(ctx, []Record{{Topic: c.topic, Key: []byte("k"), Value: []byte(val)}}) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c mtItRW) close() { c.s.Close() }

func mtItNewImpl(e mtItEnv, m tlsMaterial) (mtItClient, error) {
	s, err := NewKafkaSink(KafkaConfig{Brokers: []string{e.broker}, ClientID: "rw-mtls-it", TLS: true, TLSCAFile: m.CAFile,
		TLSCertFile: m.CertFile, TLSKeyFile: m.KeyFile, TLSReloadInterval: m.Interval, DeliveryTimeout: 8 * time.Second})
	if err != nil {
		return nil, err
	}
	return mtItRW{s, e.topic}, nil
}
