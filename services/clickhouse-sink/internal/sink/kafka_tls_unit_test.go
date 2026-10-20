package sink

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

func setEnv(t *testing.T, kv ...string) {
	t.Helper()
	for _, k := range []string{"CHSINK_KAFKA_TLS", "CHSINK_KAFKA_TLS_CA_FILE", "CHSINK_KAFKA_SASL_MECHANISM", "CHSINK_KAFKA_SASL_USERNAME", "CHSINK_KAFKA_SASL_PASSWORD"} {
		t.Setenv(k, "")
	}
	for i := 0; i < len(kv); i += 2 {
		t.Setenv(kv[i], kv[i+1])
	}
}

func TestLoadConfig_KafkaSecurityFailClosed(t *testing.T) {
	bad := map[string][]string{
		"typo in TLS flag must not mean plaintext": {"CHSINK_KAFKA_TLS", "yes"},
		"CA file without TLS":                      {"CHSINK_KAFKA_TLS_CA_FILE", "/x/ca.pem"},
		"PLAIN without TLS":                        {"CHSINK_KAFKA_SASL_MECHANISM", "PLAIN", "CHSINK_KAFKA_SASL_USERNAME", "u", "CHSINK_KAFKA_SASL_PASSWORD", "p"},
		"mechanism without credentials":            {"CHSINK_KAFKA_TLS", "1", "CHSINK_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"},
		"unknown mechanism":                        {"CHSINK_KAFKA_TLS", "1", "CHSINK_KAFKA_SASL_MECHANISM", "GSSAPI", "CHSINK_KAFKA_SASL_USERNAME", "u", "CHSINK_KAFKA_SASL_PASSWORD", "p"},
	}
	for name, kv := range bad {
		setEnv(t, kv...)
		if _, err := LoadConfig(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	setEnv(t, "CHSINK_KAFKA_TLS", "true", "CHSINK_KAFKA_SASL_MECHANISM", "scram-sha-256", "CHSINK_KAFKA_SASL_USERNAME", "u", "CHSINK_KAFKA_SASL_PASSWORD", "p")
	c, err := LoadConfig()
	if err != nil || !c.KafkaTLS || c.KafkaSASLMech != "SCRAM-SHA-256" {
		t.Fatalf("good config rejected: %+v %v", c, err)
	}
	// plain default stays plaintext without TLS
	setEnv(t)
	if c, err := LoadConfig(); err != nil || c.KafkaTLS {
		t.Fatalf("default: %v %v", c.KafkaTLS, err)
	}
}

func TestNewKafka_BadCAFileFailsClosed(t *testing.T) {
	dir := t.TempDir()
	junk := filepath.Join(dir, "junk.pem")
	_ = os.WriteFile(junk, []byte("not pem"), 0o600)
	for name, ca := range map[string]string{"junk": junk, "missing": filepath.Join(dir, "none.pem")} {
		if _, err := NewKafka(Config{Brokers: []string{"127.0.0.1:1"}, KafkaTLS: true, KafkaTLSCAFile: ca}); err == nil {
			t.Errorf("%s CA file must error", name)
		}
	}
	if _, err := NewKafka(Config{Brokers: []string{"127.0.0.1:1"}, KafkaTLSCAFile: "/x"}); err == nil {
		t.Error("CA file without TLS must error in NewKafka as well")
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

// A cert for another hostname must be rejected by the source (hostname verification on).
func TestKafkaSource_HostnameMismatchRejected(t *testing.T) {
	ca, cert := selfCA(t, t.TempDir(), "wrong.example")
	addr, errs := tlsServer(t, cert)
	k, err := NewKafka(Config{Brokers: []string{addr}, Topic: "t", GroupID: "g", BatchSize: 1, KafkaTLS: true, KafkaTLSCAFile: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msgs, _ := k.Poll(ctx)
	if len(msgs) != 0 {
		t.Fatal("got messages through a mismatched certificate")
	}
	select {
	case herr := <-errs:
		if herr == nil || !strings.Contains(herr.Error(), "certificate") {
			t.Fatalf("handshake outcome %v; expected client abort on the certificate", herr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client never attempted TLS")
	}
	// the cause is surfaced to the pipeline on an idle poll
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()
	if _, err := k.Poll(ctx2); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("idle poll should surface the certificate failure, got %v", err)
	}
}

func TestConnErrs_RedactsAndClassifies(t *testing.T) {
	h := &connErrs{pass: "s3cret-pw"}
	h.Log(0, "unable to initialize sasl", "err", errString("auth failed for s3cret-pw"))
	got := h.permanent()
	if strings.Contains(got, "s3cret-pw") || !strings.Contains(got, "***") || !strings.HasPrefix(got, "sasl:") {
		t.Fatalf("redaction/classification: %q", got)
	}
	h2 := &connErrs{}
	h2.store("broker x", errString("connection refused"))
	if h2.permanent() != "" || h2.lastErr() == "" {
		t.Fatal("refused connections are transient, not permanent")
	}
	h2.Log(0, "other", "err", errString("x509 whatever"))
}

type errString string

func (e errString) Error() string { return string(e) }
