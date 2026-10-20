package replay

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func envMap(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadKafkaConfig_FailClosed(t *testing.T) {
	base := map[string]string{"ASTRYON_KAFKA_BROKERS": "b:1"}
	with := func(kv ...string) map[string]string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	bad := map[string]map[string]string{
		"typo in TLS flag must not mean plaintext": with("ASTRYON_KAFKA_TLS", "yes"),
		"CA file without TLS":                      with("ASTRYON_KAFKA_TLS_CA_FILE", "/x/ca.pem"),
		"PLAIN without TLS":                        with("ASTRYON_KAFKA_SASL_MECHANISM", "PLAIN", "ASTRYON_KAFKA_SASL_USERNAME", "u", "ASTRYON_KAFKA_SASL_PASSWORD", "p"),
	}
	for name, m := range bad {
		if _, err := LoadKafkaConfig(envMap(m)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	for _, v := range []string{"1", "true", "TRUE"} {
		c, err := LoadKafkaConfig(envMap(with("ASTRYON_KAFKA_TLS", v)))
		if err != nil || !c.TLS {
			t.Errorf("TLS=%q: %v %v", v, c.TLS, err)
		}
	}
	if c, err := LoadKafkaConfig(envMap(with("ASTRYON_KAFKA_TLS", "1", "ASTRYON_KAFKA_SASL_MECHANISM", "plain",
		"ASTRYON_KAFKA_SASL_USERNAME", "u", "ASTRYON_KAFKA_SASL_PASSWORD", "p"))); err != nil || c.SASLMech != "PLAIN" {
		t.Errorf("PLAIN over TLS should load: %v", err)
	}
}

func TestTLSConfig_CAFileHandling(t *testing.T) {
	dir := t.TempDir()
	if _, err := (KafkaConfig{TLSCAFile: filepath.Join(dir, "missing.pem")}).tlsConfig(); err == nil {
		t.Error("missing CA file must error")
	}
	junk := filepath.Join(dir, "junk.pem")
	_ = os.WriteFile(junk, []byte("not pem"), 0o600)
	if _, err := (KafkaConfig{TLSCAFile: junk}).tlsConfig(); err == nil {
		t.Error("junk CA file must error")
	}
	bad := KafkaConfig{Brokers: []string{"127.0.0.1:1"}, TLS: true, TLSCAFile: junk}
	if _, err := NewKafkaJobPublisher(bad); err == nil {
		t.Error("NewKafkaJobPublisher must fail closed on bad CA file")
	}
	if _, err := NewKafkaAuditSink(bad, time.Second); err == nil {
		t.Error("NewKafkaAuditSink must fail closed on bad CA file")
	}
}

// selfCA creates a CA and a server cert for the given DNS names; returns CA PEM path and the server tls.Certificate.
func selfCA(t *testing.T, dir string, dns ...string) (string, tls.Certificate) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "t-ca"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "srv"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), DNSNames: dns, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, _ := x509.CreateCertificate(rand.Reader, tpl, caCert, &k.PublicKey, caKey)
	caPath := filepath.Join(dir, "ca.pem")
	_ = os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600)
	return caPath, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: k}
}

// tlsServer accepts connections and records handshake outcomes.
func tlsServer(t *testing.T, cert tls.Certificate) (addr string, errs chan error) {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	errs = make(chan error, 64)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				errs <- c.(*tls.Conn).Handshake()
				c.Close()
			}(c)
		}
	}()
	return ln.Addr().String(), errs
}

// With a CA the server cert is signed by but a DNS SAN that does not match the dialed address,
// neither the job publisher nor the audit sink may complete a handshake (hostname verification on).
func TestHostnameMismatchRejected(t *testing.T) {
	job := ReplayJob{JobID: "j", TenantID: "t", SourceID: "s"}
	ev := AuditEvent{TenantID: "t", Action: "replay.started"}
	for name, run := range map[string]func(cfg KafkaConfig) error{
		"job-publisher": func(cfg KafkaConfig) error {
			p, err := NewKafkaJobPublisher(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			return p.Publish(context.Background(), job)
		},
		"audit-sink": func(cfg KafkaConfig) error {
			p, err := NewKafkaAuditSink(cfg, 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			return p.Record(context.Background(), ev)
		},
	} {
		t.Run(name, func(t *testing.T) {
			ca, cert := selfCA(t, t.TempDir(), "wrong.example")
			addr, errs := tlsServer(t, cert)
			if err := run(KafkaConfig{Brokers: []string{addr}, TLS: true, TLSCAFile: ca, DeliveryTimeout: 2 * time.Second}); err == nil {
				t.Fatal("publish succeeded against a cert for another hostname")
			}
			select {
			case herr := <-errs:
				if herr == nil || !strings.Contains(herr.Error(), "certificate") {
					t.Fatalf("server saw handshake outcome %v; expected the client to abort on the certificate", herr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("client never attempted TLS")
			}
		})
	}
}

// A CA that did not sign the server cert: the error must name the certificate problem, not only a timeout.
func TestUntrustedCASurfacesCause(t *testing.T) {
	_, cert := selfCA(t, t.TempDir(), "127.0.0.1")
	otherCA, _ := selfCA(t, t.TempDir(), "other")
	addr, _ := tlsServer(t, cert)
	p, err := NewKafkaJobPublisher(KafkaConfig{Brokers: []string{addr}, TLS: true, TLSCAFile: otherCA, DeliveryTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	err = p.Publish(context.Background(), ReplayJob{JobID: "j", TenantID: "t", SourceID: "s"})
	if err == nil || !strings.Contains(err.Error(), "x509") {
		t.Fatalf("want x509 cause in error, got %v", err)
	}
}

func TestParseTLSFlag(t *testing.T) {
	for in, want := range map[string]bool{"": false, "0": false, "false": false, "FALSE": false, "1": true, "true": true, " True ": true} {
		if got, err := parseTLSFlag(in); err != nil || got != want {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
	for _, in := range []string{"yes", "on", "2", "t"} {
		if _, err := parseTLSFlag(in); err == nil {
			t.Errorf("%q must be an error", in)
		}
	}
}

func TestLoadKafkaConfig_UnknownMechanism(t *testing.T) {
	m := map[string]string{"ASTRYON_KAFKA_BROKERS": "b:1", "ASTRYON_KAFKA_TLS": "1", "ASTRYON_KAFKA_SASL_MECHANISM": "GSSAPI",
		"ASTRYON_KAFKA_SASL_USERNAME": "u", "ASTRYON_KAFKA_SASL_PASSWORD": "p"}
	if _, err := LoadKafkaConfig(envMap(m)); err == nil {
		t.Fatal("unknown mechanism must be an error")
	}
}

func TestConnErrs_AnnotatesTimeoutAndRedactsPassword(t *testing.T) {
	h := &connErrs{pass: "s3cret-pw"}
	h.Log(0, "unable to initialize sasl", "broker", "1", "err", context.Canceled)
	if got := h.annotate(context.DeadlineExceeded).Error(); !strings.Contains(got, "context deadline exceeded") || !strings.Contains(got, "sasl:") {
		t.Fatalf("not annotated: %q", got)
	}
	h.Log(0, "unable to initialize sasl", "err", errString("auth failed for s3cret-pw"))
	if got := h.annotate(context.DeadlineExceeded).Error(); strings.Contains(got, "s3cret-pw") || !strings.Contains(got, "***") {
		t.Fatalf("password not redacted: %q", got)
	}
	var none *connErrs
	if none.annotate(context.DeadlineExceeded) == nil {
		t.Fatal("nil receiver must pass errors through")
	}
	h.Log(0, "other message", "err", errString("ignored"))
}

func TestConnErrs_CauseExpires(t *testing.T) {
	h := &connErrs{pass: "pw"}
	h.Log(0, "unable to initialize sasl", "err", errString("bad"))
	h.at.Store(time.Now().Add(-connErrTTL - time.Second).UnixNano())
	if got := h.annotate(context.DeadlineExceeded); strings.Contains(got.Error(), "bad") {
		t.Fatalf("stale cause still attached: %v", got)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
