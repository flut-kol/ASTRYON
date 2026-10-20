// Package tlsconf builds mutual-TLS *tls.Config values from PEM files with
// hot reload: certificates and CA bundles are re-read when files change, so
// rotation (cert-manager / Vault PKI agent) needs no restart.
package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Files are PEM file paths.
type Files struct {
	CertFile string
	KeyFile  string
	CAFile   string // server: client CA bundle; client: server CA bundle
}

// Options tune the config.
type Options struct {
	// MinTLS13 requires TLS 1.3 (default minimum is TLS 1.2; 1.3 is preferred
	// automatically by Go when both sides support it).
	MinTLS13 bool
	// ServerName overrides client-side verification name.
	ServerName string
	// CheckInterval throttles file stat checks (default 5s).
	CheckInterval time.Duration
}

type stamp struct {
	mod  time.Time
	size int64
}

type reloader struct {
	f    Files
	opts Options

	mu      sync.Mutex
	cert    *tls.Certificate
	pool    *x509.CertPool
	stamps  [3]stamp
	checked time.Time
}

func newReloader(f Files, o Options, needCA bool) (*reloader, error) {
	if f.CertFile == "" || f.KeyFile == "" {
		return nil, errors.New("tlsconf: cert and key files required")
	}
	if needCA && f.CAFile == "" {
		return nil, errors.New("tlsconf: CA file required")
	}
	if o.CheckInterval == 0 {
		o.CheckInterval = 5 * time.Second
	}
	r := &reloader{f: f, opts: o}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

func statOf(p string) (stamp, error) {
	if p == "" {
		return stamp{}, nil
	}
	fi, err := os.Stat(p)
	if err != nil {
		return stamp{}, err
	}
	return stamp{fi.ModTime(), fi.Size()}, nil
}

func (r *reloader) load() error {
	var st [3]stamp
	var err error
	for i, p := range []string{r.f.CertFile, r.f.KeyFile, r.f.CAFile} {
		if st[i], err = statOf(p); err != nil {
			return fmt.Errorf("tlsconf: %w", err)
		}
	}
	c, err := tls.LoadX509KeyPair(r.f.CertFile, r.f.KeyFile)
	if err != nil {
		return fmt.Errorf("tlsconf: load keypair: %w", err)
	}
	var pool *x509.CertPool
	if r.f.CAFile != "" {
		pem, err := os.ReadFile(r.f.CAFile)
		if err != nil {
			return fmt.Errorf("tlsconf: read CA: %w", err)
		}
		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return errors.New("tlsconf: no certificates in CA file")
		}
	}
	r.mu.Lock()
	r.cert, r.pool, r.stamps, r.checked = &c, pool, st, time.Now()
	r.mu.Unlock()
	return nil
}

// refresh reloads when files changed; on failure keeps the previous material
// (a half-written rotation must not break live traffic).
func (r *reloader) refresh() {
	r.mu.Lock()
	if time.Since(r.checked) < r.opts.CheckInterval {
		r.mu.Unlock()
		return
	}
	r.checked = time.Now()
	old := r.stamps
	r.mu.Unlock()
	var cur [3]stamp
	for i, p := range []string{r.f.CertFile, r.f.KeyFile, r.f.CAFile} {
		s, err := statOf(p)
		if err != nil {
			return
		}
		cur[i] = s
	}
	if cur != old {
		_ = r.load()
	}
}

func (r *reloader) current() (*tls.Certificate, *x509.CertPool) {
	r.refresh()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cert, r.pool
}

func (r *reloader) base() *tls.Config {
	min := uint16(tls.VersionTLS12)
	if r.opts.MinTLS13 {
		min = tls.VersionTLS13
	}
	return &tls.Config{MinVersion: min}
}

// ServerConfig returns a config requiring and verifying client certificates.
func ServerConfig(f Files, o Options) (*tls.Config, error) {
	r, err := newReloader(f, o, true)
	if err != nil {
		return nil, err
	}
	cfg := r.base()
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	_, cfg.ClientCAs = r.current()
	cfg.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		c, _ := r.current()
		return c, nil
	}
	// Per-handshake clone so a rotated client CA bundle takes effect.
	cfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		_, pool := r.current()
		c := cfg.Clone()
		c.GetConfigForClient = nil
		c.ClientCAs = pool
		return c, nil
	}
	return cfg, nil
}

// ClientConfig returns a config presenting a client certificate and verifying
// the server against the CA file. Both the client certificate and the CA bundle
// hot-reload (verification is done in VerifyConnection against the current pool).
func ClientConfig(f Files, o Options) (*tls.Config, error) {
	r, err := newReloader(f, o, true)
	if err != nil {
		return nil, err
	}
	cfg := r.base()
	cfg.ServerName = o.ServerName
	// Verify manually so the CA pool can rotate too.
	cfg.InsecureSkipVerify = true //nolint:gosec // replaced by VerifyConnection below
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		_, pool := r.current()
		if len(cs.PeerCertificates) == 0 {
			return errors.New("tlsconf: no server certificate")
		}
		opts := x509.VerifyOptions{Roots: pool, DNSName: cfg.ServerName, Intermediates: x509.NewCertPool()}
		if opts.DNSName == "" {
			opts.DNSName = cs.ServerName
		}
		for _, c := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(c)
		}
		_, err := cs.PeerCertificates[0].Verify(opts)
		return err
	}
	cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		c, _ := r.current()
		return c, nil
	}
	return cfg, nil
}
