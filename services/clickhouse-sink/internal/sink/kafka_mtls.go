package sink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka client TLS material with mTLS (client certificate) and hot reload of the client
// certificate and the CA bundle. The same file lives in every service that has its own Kafka
// client (modules are separate, no shared import); keep them identical apart from the package name.

// defaultTLSReloadInterval is used when <PREFIX>_TLS_RELOAD_INTERVAL is unset.
const defaultTLSReloadInterval = time.Minute

// tlsLogEvery rate-limits reload failure logs.
const tlsLogEvery = time.Minute

// tlsMaterial names the TLS files of one Kafka client. Prefix is the env prefix used in
// error texts (e.g. ASTRYON_KAFKA or CHSINK_KAFKA).
type tlsMaterial struct {
	Prefix   string
	CAFile   string        // optional PEM bundle; system roots if empty
	CertFile string        // optional client certificate (PEM chain); with KeyFile
	KeyFile  string        // optional client key (PEM); with CertFile
	Interval time.Duration // reload check period; <= 0 disables reloading
}

// validate fails closed on half-configured or TLS-less client certificates.
func (m tlsMaterial) validate(tlsOn bool) error {
	if (m.CertFile == "") != (m.KeyFile == "") {
		return fmt.Errorf("%s_TLS_CERT_FILE and %s_TLS_KEY_FILE must be set together", m.Prefix, m.Prefix)
	}
	if m.CertFile != "" && !tlsOn {
		return fmt.Errorf("%s_TLS_CERT_FILE is set but %s_TLS is off (refusing to fall back to plaintext)", m.Prefix, m.Prefix)
	}
	return nil
}

// parseTLSReloadInterval parses <PREFIX>_TLS_RELOAD_INTERVAL: empty = 1m, "0" = reload off.
func parseTLSReloadInterval(prefix, v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return defaultTLSReloadInterval, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s_TLS_RELOAD_INTERVAL must be a non-negative duration (0 = off), got %q", prefix, v)
	}
	return d, nil
}

type fileSig struct {
	mod  int64
	size int64
}

func statSig(path string) (fileSig, error) {
	st, err := os.Stat(path)
	if err != nil {
		return fileSig{}, err
	}
	return fileSig{mod: st.ModTime().UnixNano(), size: st.Size()}, nil
}

// tlsReloader holds the current CA pool and client certificate and re-reads them when the files'
// mtime/size change. A failed reload keeps the last good material (never falls back to
// insecure). Checks run lazily from dial/handshake, at most once per interval; no goroutine.
type tlsReloader struct {
	m    tlsMaterial
	now  func() time.Time
	logf func(lvl slog.Level, msg string, args ...any)

	mu        sync.Mutex
	pool      *x509.CertPool
	cert      *tls.Certificate
	caSig     fileSig
	certSig   [2]fileSig
	lastCheck time.Time
	lastLog   time.Time
}

func loadCAPool(m tlsMaterial) (*x509.CertPool, error) {
	pem, err := os.ReadFile(m.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read %s_TLS_CA_FILE: %w", m.Prefix, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s_TLS_CA_FILE: no valid PEM certificates", m.Prefix)
	}
	return pool, nil
}

func loadKeyPair(m tlsMaterial) (*tls.Certificate, error) {
	c, err := tls.LoadX509KeyPair(m.CertFile, m.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load %s_TLS_CERT_FILE/%s_TLS_KEY_FILE: %w", m.Prefix, m.Prefix, err)
	}
	return &c, nil
}

// newTLSReloader loads the material once; any failure here is a startup error.
func newTLSReloader(m tlsMaterial) (*tlsReloader, error) {
	if err := m.validate(true); err != nil {
		return nil, err
	}
	r := &tlsReloader{m: m, now: time.Now, logf: func(lvl slog.Level, msg string, args ...any) { slog.Log(context.Background(), lvl, msg, args...) }}
	r.lastCheck = r.now()
	if m.CAFile != "" {
		sig, _ := statSig(m.CAFile)
		pool, err := loadCAPool(m)
		if err != nil {
			return nil, err
		}
		r.pool, r.caSig = pool, sig
	}
	if m.CertFile != "" {
		s1, _ := statSig(m.CertFile)
		s2, _ := statSig(m.KeyFile)
		c, err := loadKeyPair(m)
		if err != nil {
			return nil, err
		}
		r.cert, r.certSig = c, [2]fileSig{s1, s2}
	}
	return r, nil
}

// warn logs a reload failure at most once per tlsLogEvery. err never carries key material:
// it is an OS error (path only) or a parse error text.
func (r *tlsReloader) warn(what string, err error) {
	if n := r.now(); n.Sub(r.lastLog) >= tlsLogEvery || r.lastLog.IsZero() {
		r.lastLog = n
		r.logf(slog.LevelWarn, "kafka tls reload failed, keeping previous material", "prefix", r.m.Prefix, "what", what, "err", err.Error())
	}
}

// refresh re-reads changed files if the check interval elapsed.
func (r *tlsReloader) refresh() {
	if r.m.Interval <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if now.Sub(r.lastCheck) < r.m.Interval {
		return
	}
	r.lastCheck = now
	if r.m.CAFile != "" {
		if sig, err := statSig(r.m.CAFile); err != nil {
			r.warn("ca", err)
		} else if sig != r.caSig {
			if pool, err := loadCAPool(r.m); err != nil {
				r.warn("ca", err)
			} else {
				r.pool, r.caSig = pool, sig
				r.logf(slog.LevelInfo, "kafka tls CA reloaded", "prefix", r.m.Prefix)
			}
		}
	}
	if r.m.CertFile != "" {
		s1, e1 := statSig(r.m.CertFile)
		s2, e2 := statSig(r.m.KeyFile)
		if err := errors.Join(e1, e2); err != nil {
			r.warn("client cert", err)
		} else if sigs := [2]fileSig{s1, s2}; sigs != r.certSig {
			if c, err := loadKeyPair(r.m); err != nil {
				r.warn("client cert", err)
			} else {
				r.cert, r.certSig = c, sigs
				r.logf(slog.LevelInfo, "kafka tls client certificate reloaded", "prefix", r.m.Prefix)
			}
		}
	}
}

func (r *tlsReloader) currentCert() *tls.Certificate {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cert
}

func (r *tlsReloader) currentPool() *x509.CertPool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pool
}

// config returns a fresh client config with the current material. The server chain and host
// name are always verified (no InsecureSkipVerify); RootCAs nil means system roots.
func (r *tlsReloader) config() *tls.Config {
	r.refresh()
	tc := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: r.currentPool()}
	if r.m.CertFile != "" {
		tc.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			r.refresh()
			if c := r.currentCert(); c != nil {
				return c, nil
			}
			return &tls.Certificate{}, nil
		}
	}
	return tc
}

// dial opens a TLS connection to a broker with the current material (new connections pick up
// renewed certificates/CAs; established ones keep their handshake until the broker or the
// idle timeout closes them).
func (r *tlsReloader) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: r.config()}
	return d.DialContext(ctx, network, addr)
}

// kafkaTLSOpt returns the franz-go option for TLS. With no CA file and no client cert there is
// nothing to reload and the plain system-roots config is used.
func kafkaTLSOpt(m tlsMaterial) (kgo.Opt, error) {
	r, err := newTLSReloader(m)
	if err != nil {
		return nil, err
	}
	if m.CAFile == "" && m.CertFile == "" {
		return kgo.DialTLSConfig(r.config()), nil
	}
	return kgo.Dialer(r.dial), nil
}

// staticTLSConfig is the startup snapshot as a tls.Config (used for validation and tests).
func staticTLSConfig(m tlsMaterial) (*tls.Config, error) {
	r, err := newTLSReloader(m)
	if err != nil {
		return nil, err
	}
	return r.config(), nil
}
