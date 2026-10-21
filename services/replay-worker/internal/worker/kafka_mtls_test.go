package worker

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type mtCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func mtNewCA(t *testing.T, cn string) mtCA {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	return mtCA{c, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue returns certPEM, keyPEM for a leaf usable as server (localhost/127.0.0.1) and client.
func (ca mtCA) issue(t *testing.T, cn string) ([]byte, []byte) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(12 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	kd, _ := x509.MarshalPKCS8PrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kd})
}

// mtWrite writes a file and gives it a distinct mtime so change detection is deterministic.
func mtWrite(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	mtBump++
	ts := time.Unix(1_700_000_000+mtBump*10, 0)
	if err := os.Chtimes(path, ts, ts); err != nil {
		t.Fatal(err)
	}
}

var mtBump int64

type mtClock struct{ t time.Time }

func (c *mtClock) now() time.Time          { return c.t }
func (c *mtClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type mtLogs struct {
	mu   sync.Mutex
	msgs []string
}

func (l *mtLogs) logf(_ slog.Level, msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := msg
	for _, a := range args {
		s += " " + strings.TrimSpace(strings.ReplaceAll(toStr(a), "\n", " "))
	}
	l.msgs = append(l.msgs, s)
}

func toStr(a any) string {
	if s, ok := a.(string); ok {
		return s
	}
	return ""
}

func (l *mtLogs) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, m := range l.msgs {
		if strings.Contains(m, sub) {
			n++
		}
	}
	return n
}

func mtSerial(t *testing.T, c *tls.Certificate) *big.Int {
	t.Helper()
	x, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return x.SerialNumber
}

type mtFiles struct {
	dir, ca, cert, key string
}

func mtSetup(t *testing.T, ca mtCA, clientCA mtCA) mtFiles {
	t.Helper()
	d := t.TempDir()
	f := mtFiles{dir: d, ca: filepath.Join(d, "ca.pem"), cert: filepath.Join(d, "tls.crt"), key: filepath.Join(d, "tls.key")}
	mtWrite(t, f.ca, ca.pem)
	c, k := clientCA.issue(t, "client-1")
	mtWrite(t, f.cert, c)
	mtWrite(t, f.key, k)
	return f
}

func mtReloader(t *testing.T, f mtFiles, interval time.Duration) (*tlsReloader, *mtClock, *mtLogs) {
	t.Helper()
	r, err := newTLSReloader(tlsMaterial{Prefix: "X_KAFKA", CAFile: f.ca, CertFile: f.cert, KeyFile: f.key, Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	clk := &mtClock{t: time.Unix(1_800_000_000, 0)}
	lg := &mtLogs{}
	r.now, r.logf, r.lastCheck = clk.now, lg.logf, clk.t
	return r, clk, lg
}

// mtServer is an in-process TLS server that requires and verifies client certificates.
func mtServer(t *testing.T, serverCert tls.Certificate, clientCAs *x509.CertPool) string {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 1)
				if _, err := c.Read(buf); err == nil {
					c.Write(buf)
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// mtRoundTrip dials through the reloader and echoes one byte; any TLS rejection (also the
// TLS 1.3 post-handshake client-cert alert) surfaces as an error.
func mtRoundTrip(r *tlsReloader, addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := r.dial(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte{1}); err != nil {
		return err
	}
	_, err = c.Read(make([]byte, 1))
	return err
}

func mtServerCert(t *testing.T, ca mtCA) tls.Certificate {
	t.Helper()
	c, k := ca.issue(t, "broker")
	sc, err := tls.X509KeyPair(c, k)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func TestTLSMaterial_ConfigMatrix(t *testing.T) {
	cases := []struct {
		name      string
		m         tlsMaterial
		tlsOn     bool
		wantError string
	}{
		{"none", tlsMaterial{Prefix: "P"}, false, ""},
		{"cert+key with TLS", tlsMaterial{Prefix: "P", CertFile: "c", KeyFile: "k"}, true, ""},
		{"cert only", tlsMaterial{Prefix: "P", CertFile: "c"}, true, "P_TLS_CERT_FILE and P_TLS_KEY_FILE must be set together"},
		{"key only", tlsMaterial{Prefix: "P", KeyFile: "k"}, true, "must be set together"},
		{"cert+key without TLS", tlsMaterial{Prefix: "P", CertFile: "c", KeyFile: "k"}, false, "P_TLS is off"},
	}
	for _, c := range cases {
		err := c.m.validate(c.tlsOn)
		if c.wantError == "" && err != nil || c.wantError != "" && (err == nil || !strings.Contains(err.Error(), c.wantError)) {
			t.Errorf("%s: err=%v want %q", c.name, err, c.wantError)
		}
	}
	for in, want := range map[string]time.Duration{"": time.Minute, "0": 0, "30s": 30 * time.Second, " 2m ": 2 * time.Minute} {
		if d, err := parseTLSReloadInterval("P", in); err != nil || d != want {
			t.Errorf("interval %q: %v %v", in, d, err)
		}
	}
	for _, in := range []string{"abc", "-1s", "5"} {
		if _, err := parseTLSReloadInterval("P", in); err == nil {
			t.Errorf("interval %q must be rejected", in)
		}
	}
}

func TestTLSReloader_StartupErrorsKeepCause(t *testing.T) {
	dir := t.TempDir()
	junk := filepath.Join(dir, "junk.pem")
	mtWrite(t, junk, []byte("not pem"))
	if _, err := newTLSReloader(tlsMaterial{Prefix: "P", CAFile: filepath.Join(dir, "none.pem")}); err == nil || !os.IsNotExist(unwrapAll(err)) {
		t.Errorf("missing CA must keep the fs cause: %v", err)
	}
	if _, err := newTLSReloader(tlsMaterial{Prefix: "P", CAFile: junk}); err == nil || !strings.Contains(err.Error(), "no valid PEM") {
		t.Errorf("junk CA: %v", err)
	}
	if _, err := newTLSReloader(tlsMaterial{Prefix: "P", CertFile: junk, KeyFile: junk}); err == nil {
		t.Error("junk client cert must fail at startup")
	}
}

func unwrapAll(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		err = u.Unwrap()
	}
}

func TestTLSReloader_ClientCertRenewedWithoutRestart(t *testing.T) {
	ca := mtNewCA(t, "ca")
	f := mtSetup(t, ca, ca)
	r, clk, _ := mtReloader(t, f, time.Minute)
	first, _ := r.config().GetClientCertificate(nil)
	s1 := mtSerial(t, first)

	c2, k2 := ca.issue(t, "client-2")
	mtWrite(t, f.cert, c2)
	mtWrite(t, f.key, k2)
	if got, _ := r.config().GetClientCertificate(nil); mtSerial(t, got).Cmp(s1) != 0 {
		t.Fatal("renewed files must not be read before the interval elapsed")
	}
	clk.advance(time.Minute)
	got, _ := r.config().GetClientCertificate(nil)
	if mtSerial(t, got).Cmp(s1) == 0 {
		t.Fatal("renewed client certificate was not picked up")
	}
}

func TestTLSReloader_ReloadOffNeverRereads(t *testing.T) {
	ca := mtNewCA(t, "ca")
	f := mtSetup(t, ca, ca)
	r, clk, _ := mtReloader(t, f, 0)
	first, _ := r.config().GetClientCertificate(nil)
	c2, k2 := ca.issue(t, "client-2")
	mtWrite(t, f.cert, c2)
	mtWrite(t, f.key, k2)
	clk.advance(24 * time.Hour)
	if got, _ := r.config().GetClientCertificate(nil); mtSerial(t, got).Cmp(mtSerial(t, first)) != 0 {
		t.Fatal("interval 0 must disable reloading")
	}
}

func TestTLSReloader_BrokenNewFilesKeepOldAndLogOncePerMinute(t *testing.T) {
	ca := mtNewCA(t, "ca")
	f := mtSetup(t, ca, ca)
	r, clk, lg := mtReloader(t, f, time.Second)
	first, _ := r.config().GetClientCertificate(nil)
	oldPool := r.currentPool()

	mtWrite(t, f.cert, []byte("garbage"))
	mtWrite(t, f.ca, []byte("garbage"))
	for i := 0; i < 5; i++ { // 5 failing checks within one minute
		clk.advance(2 * time.Second)
		got, err := r.config().GetClientCertificate(nil)
		if err != nil || mtSerial(t, got).Cmp(mtSerial(t, first)) != 0 {
			t.Fatalf("broken cert file must keep the old certificate: %v", err)
		}
		if r.currentPool() != oldPool {
			t.Fatal("broken CA file must keep the old pool")
		}
	}
	if n := lg.count("reload failed"); n != 1 { // the limiter is shared: only the first failure within a minute logs
		t.Fatalf("want a single failure log within a minute, got %d: %v", n, lg.msgs)
	}
	clk.advance(time.Minute)
	r.config()
	if lg.count("reload failed") < 2 {
		t.Fatalf("a failure after a minute must be logged again: %v", lg.msgs)
	}
	for _, m := range lg.msgs {
		if strings.Contains(m, "PRIVATE KEY") || strings.Contains(m, "garbage") {
			t.Fatalf("log leaks file content: %q", m)
		}
	}

	// A half-written rotation (cert renewed, key not yet) must also keep the old pair.
	c2, k2 := ca.issue(t, "client-2")
	mtWrite(t, f.cert, c2)
	clk.advance(2 * time.Second)
	if got, _ := r.config().GetClientCertificate(nil); mtSerial(t, got).Cmp(mtSerial(t, first)) != 0 {
		t.Fatal("mismatched cert/key must keep the old pair")
	}
	// Completing the rotation (and fixing the CA) recovers.
	mtWrite(t, f.key, k2)
	mtWrite(t, f.ca, ca.pem)
	clk.advance(2 * time.Second)
	if got, _ := r.config().GetClientCertificate(nil); mtSerial(t, got).Cmp(mtSerial(t, first)) == 0 {
		t.Fatal("complete new pair must be adopted")
	}
	if r.currentPool() == oldPool {
		t.Fatal("fixed CA file must be adopted")
	}
}

func TestTLSReloader_InProcessServerRequiringClientCerts(t *testing.T) {
	brokerCA := mtNewCA(t, "broker-ca")
	clientCA := mtNewCA(t, "client-ca")
	otherCA := mtNewCA(t, "other-ca")
	clientPool := x509.NewCertPool()
	clientPool.AddCert(clientCA.cert)
	addr := mtServer(t, mtServerCert(t, brokerCA), clientPool)

	// round trip with a valid client certificate
	f := mtSetup(t, brokerCA, clientCA)
	r, clk, _ := mtReloader(t, f, time.Second)
	if err := mtRoundTrip(r, addr); err != nil {
		t.Fatalf("mTLS round trip: %v", err)
	}

	// no client certificate: rejected
	noCert, err := newTLSReloader(tlsMaterial{Prefix: "X", CAFile: f.ca})
	if err != nil {
		t.Fatal(err)
	}
	if err := mtRoundTrip(noCert, addr); err == nil {
		t.Fatal("server requiring client certs must reject a client without one")
	}

	// certificate from another CA: rejected
	oc, ok := otherCA.issue(t, "intruder")
	mtWrite(t, f.cert, oc)
	mtWrite(t, f.key, ok)
	other, err := newTLSReloader(tlsMaterial{Prefix: "X", CAFile: f.ca, CertFile: f.cert, KeyFile: f.key})
	if err != nil {
		t.Fatal(err)
	}
	if err := mtRoundTrip(other, addr); err == nil {
		t.Fatal("client certificate from an untrusted CA must be rejected")
	}

	// renew to a certificate from the trusted CA while `other`-style client runs: recovers without restart
	r2, clk2, _ := mtReloader(t, f, time.Second)
	if err := mtRoundTrip(r2, addr); err == nil {
		t.Fatal("still the untrusted certificate")
	}
	gc, gk := clientCA.issue(t, "renewed")
	mtWrite(t, f.cert, gc)
	mtWrite(t, f.key, gk)
	clk2.advance(2 * time.Second)
	if err := mtRoundTrip(r2, addr); err != nil {
		t.Fatalf("renewed client certificate must be used by the running client: %v", err)
	}
	_ = clk
}

func TestTLSReloader_CARenewedWithoutRestart(t *testing.T) {
	oldBrokerCA := mtNewCA(t, "broker-ca-old")
	newBrokerCA := mtNewCA(t, "broker-ca-new")
	clientCA := mtNewCA(t, "client-ca")
	clientPool := x509.NewCertPool()
	clientPool.AddCert(clientCA.cert)
	addr := mtServer(t, mtServerCert(t, newBrokerCA), clientPool) // broker already re-issued by the new CA

	f := mtSetup(t, oldBrokerCA, clientCA) // client still trusts only the old CA
	r, clk, _ := mtReloader(t, f, time.Second)
	err := mtRoundTrip(r, addr)
	if err == nil || !strings.Contains(err.Error(), "unknown authority") {
		t.Fatalf("server signed by an unknown CA must be rejected, got %v", err)
	}
	mtWrite(t, f.ca, append(append([]byte{}, oldBrokerCA.pem...), newBrokerCA.pem...)) // bundle with both
	clk.advance(2 * time.Second)
	if err := mtRoundTrip(r, addr); err != nil {
		t.Fatalf("renewed CA bundle must be adopted without restart: %v", err)
	}
	// a broken CA update afterwards keeps trusting (and never turns verification off)
	mtWrite(t, f.ca, bytes.Repeat([]byte("x"), 10))
	clk.advance(2 * time.Second)
	if err := mtRoundTrip(r, addr); err != nil {
		t.Fatalf("broken CA file must keep the last good bundle: %v", err)
	}
}

func TestKafkaTLSOpt_NoMaterialUsesSystemRoots(t *testing.T) {
	opt, err := kafkaTLSOpt(tlsMaterial{Prefix: "P"})
	if err != nil || opt == nil {
		t.Fatalf("%v", err)
	}
	tc, err := staticTLSConfig(tlsMaterial{Prefix: "P"})
	if err != nil || tc.RootCAs != nil || tc.InsecureSkipVerify || tc.GetClientCertificate != nil || tc.MinVersion < tls.VersionTLS12 {
		t.Fatalf("unexpected default config: %+v %v", tc, err)
	}
}

func TestLoadKafkaConfig_MTLSMatrix(t *testing.T) {
	load := func(kv ...string) (KafkaConfig, error) {
		m := map[string]string{"ASTRYON_KAFKA_BROKERS": "b:1"}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return LoadKafkaConfig(func(k string) string { return m[k] })
	}
	for name, kv := range map[string][]string{
		"cert without key":  {"ASTRYON_KAFKA_TLS", "1", "ASTRYON_KAFKA_TLS_CERT_FILE", "/c"},
		"key without cert":  {"ASTRYON_KAFKA_TLS", "1", "ASTRYON_KAFKA_TLS_KEY_FILE", "/k"},
		"pair without TLS":  {"ASTRYON_KAFKA_TLS_CERT_FILE", "/c", "ASTRYON_KAFKA_TLS_KEY_FILE", "/k"},
		"bad interval":      {"ASTRYON_KAFKA_TLS", "1", "ASTRYON_KAFKA_TLS_RELOAD_INTERVAL", "soon"},
		"negative interval": {"ASTRYON_KAFKA_TLS", "1", "ASTRYON_KAFKA_TLS_RELOAD_INTERVAL", "-5s"},
	} {
		if _, err := load(kv...); err == nil {
			t.Errorf("%s: expected config error", name)
		}
	}
	c, err := load("ASTRYON_KAFKA_TLS", "1", "ASTRYON_KAFKA_TLS_CERT_FILE", "/c", "ASTRYON_KAFKA_TLS_KEY_FILE", "/k")
	if err != nil || c.TLSCertFile != "/c" || c.TLSKeyFile != "/k" || c.TLSReloadInterval != time.Minute {
		t.Errorf("pair with TLS: %+v %v", c, err)
	}
	if c, err = load("ASTRYON_KAFKA_TLS", "1", "ASTRYON_KAFKA_TLS_RELOAD_INTERVAL", "0"); err != nil || c.TLSReloadInterval != 0 {
		t.Errorf("interval 0 = off: %+v %v", c, err)
	}
	if _, err := load("ASTRYON_KAFKA_TLS", "1", "ASTRYON_KAFKA_TLS_CERT_FILE", "/nonexistent.crt", "ASTRYON_KAFKA_TLS_KEY_FILE", "/nonexistent.key"); err != nil {
		t.Errorf("files are read at client construction, not config load: %v", err)
	}
}
