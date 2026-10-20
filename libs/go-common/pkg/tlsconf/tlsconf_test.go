package tlsconf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newCA(t *testing.T) *ca {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	return &ca{c, k, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue returns cert and key PEM with the given CN.
func (a *ca) issue(t *testing.T, cn string, serial int64) (certPEM, keyPEM []byte) {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: cn},
		DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, a.cert, &k.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(k)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
}

func write(t *testing.T, dir, prefix string, a *ca, cn string, serial int64) Files {
	c, k := a.issue(t, cn, serial)
	f := Files{filepath.Join(dir, prefix+".crt"), filepath.Join(dir, prefix+".key"), filepath.Join(dir, prefix+"-ca.pem")}
	os.WriteFile(f.CertFile, c, 0o600)
	os.WriteFile(f.KeyFile, k, 0o600)
	os.WriteFile(f.CAFile, a.pem, 0o600)
	return f
}

func serve(t *testing.T, f Files) *httptest.Server {
	cfg, err := ServerConfig(f, Options{CheckInterval: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, r.TLS.PeerCertificates[0].Subject.CommonName)
	}))
	srv.TLS = cfg
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, cc Files, url string) (string, error) {
	cfg, err := ClientConfig(cc, Options{CheckInterval: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
	resp, err := hc.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), nil
}

func TestMTLS(t *testing.T) {
	a := newCA(t)
	d := t.TempDir()
	srv := serve(t, write(t, d, "srv", a, "server", 10))
	body, err := get(t, write(t, d, "cli", a, "client-a", 11), srv.URL)
	if err != nil || body != "client-a" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	// client signed by a different CA is rejected
	other := newCA(t)
	if _, err := get(t, write(t, d, "bad", other, "evil", 12), srv.URL); err == nil {
		t.Fatal("expected rejection")
	}
	// client without cert is rejected
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: mustClientNoCert(t, a)}}
	if resp, err := hc.Get(srv.URL); err == nil {
		resp.Body.Close()
		t.Fatal("expected rejection without client cert")
	}
}

func mustClientNoCert(t *testing.T, a *ca) *tls.Config {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(a.pem)
	return &tls.Config{RootCAs: pool, ServerName: "localhost"}
}

func TestHotReload(t *testing.T) {
	a := newCA(t)
	d := t.TempDir()
	srv := serve(t, write(t, d, "srv", a, "server", 10))
	cf := write(t, d, "cli", a, "v1", 11)
	cfg, err := ClientConfig(cf, Options{CheckInterval: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	do := func() string {
		hc := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg, DisableKeepAlives: true}}
		resp, err := hc.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	if got := do(); got != "v1" {
		t.Fatal(got)
	}
	c, k := a.issue(t, "v2-rotated", 99)
	os.WriteFile(cf.CertFile, c, 0o600)
	os.WriteFile(cf.KeyFile, k, 0o600)
	if got := do(); got != "v2-rotated" {
		t.Fatalf("no reload: %s", got)
	}
	// corrupt rotation keeps the previous cert
	os.WriteFile(cf.CertFile, []byte("junk"), 0o600)
	if got := do(); got != "v2-rotated" {
		t.Fatalf("lost cert: %s", got)
	}
}

func TestValidation(t *testing.T) {
	if _, err := ServerConfig(Files{}, Options{}); err == nil {
		t.Fatal("want error")
	}
	if _, err := ClientConfig(Files{CertFile: "/nonexistent", KeyFile: "/x", CAFile: "/y"}, Options{}); err == nil {
		t.Fatal("want error")
	}
	a := newCA(t)
	f := write(t, t.TempDir(), "s", a, "s", 1)
	c, _ := ServerConfig(f, Options{MinTLS13: true})
	if c.MinVersion != 0x0304 {
		t.Fatal("min version")
	}
}
