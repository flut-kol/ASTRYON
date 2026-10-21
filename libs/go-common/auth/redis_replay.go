package auth

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RedisReplayStore is a shared ReplayStore for several replicas: `SET key 1 NX EX ttl` over a minimal
// RESP client (stdlib only). Standalone Redis only (no Cluster/Sentinel); any error means reject.
type RedisReplayStore struct {
	addr, user, pass string
	db               int
	tlsCfg           *tls.Config
	timeout          time.Duration
	mu               sync.Mutex
	idle             []net.Conn
}

// NewRedisReplayStore parses redis[s]://[user:pass@]host:port[/db]. caFile (PEM, optional) is the trust root
// for rediss://; TLS verification is never disabled. The password stays in the URL from the environment.
func NewRedisReplayStore(raw, caFile string) (*RedisReplayStore, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "redis" && u.Scheme != "rediss") {
		return nil, errors.New("auth: redis url must be redis[s]://host:port")
	}
	s := &RedisReplayStore{addr: u.Host, timeout: 2 * time.Second}
	if u.Port() == "" {
		s.addr = net.JoinHostPort(u.Hostname(), "6379")
	}
	if u.User != nil {
		s.user = u.User.Username()
		s.pass, _ = u.User.Password()
	}
	if p := strings.Trim(u.Path, "/"); p != "" {
		if s.db, err = strconv.Atoi(p); err != nil || s.db < 0 {
			return nil, errors.New("auth: bad redis db")
		}
	}
	if u.Scheme == "rediss" {
		s.tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()}
		if caFile != "" {
			pem, err := os.ReadFile(caFile)
			if err != nil {
				return nil, err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("auth: no certificates in redis CA file")
			}
			s.tlsCfg.RootCAs = pool
		}
	}
	return s, nil
}

func (s *RedisReplayStore) First(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	secs := int((ttl + time.Second - 1) / time.Second)
	if secs < 1 {
		secs = 1
	}
	c, err := s.conn(ctx)
	if err != nil {
		return false, err
	}
	reply, err := s.roundTrip(c, "SET", key, "1", "NX", "EX", strconv.Itoa(secs))
	if err != nil {
		c.Close()
		return false, err
	}
	s.put(c)
	switch reply {
	case "+OK":
		return true, nil
	case "$-1":
		return false, nil
	}
	return false, fmt.Errorf("auth: unexpected redis reply %q", reply)
}

func (s *RedisReplayStore) conn(ctx context.Context) (net.Conn, error) {
	s.mu.Lock()
	if n := len(s.idle); n > 0 {
		c := s.idle[n-1]
		s.idle = s.idle[:n-1]
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()
	d := &net.Dialer{Timeout: s.timeout}
	var c net.Conn
	var err error
	if s.tlsCfg != nil {
		c, err = (&tls.Dialer{NetDialer: d, Config: s.tlsCfg}).DialContext(ctx, "tcp", s.addr)
	} else {
		c, err = d.DialContext(ctx, "tcp", s.addr)
	}
	if err != nil {
		return nil, err
	}
	if s.pass != "" {
		args := []string{"AUTH", s.pass}
		if s.user != "" {
			args = []string{"AUTH", s.user, s.pass}
		}
		if r, err := s.roundTrip(c, args...); err != nil || r != "+OK" {
			c.Close()
			return nil, errors.New("auth: redis AUTH failed")
		}
	}
	if s.db != 0 {
		if r, err := s.roundTrip(c, "SELECT", strconv.Itoa(s.db)); err != nil || r != "+OK" {
			c.Close()
			return nil, errors.New("auth: redis SELECT failed")
		}
	}
	return c, nil
}

func (s *RedisReplayStore) put(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.idle) < 8 {
		s.idle = append(s.idle, c)
		return
	}
	c.Close()
}

// roundTrip sends one command and returns the first reply line (error replies become errors).
func (s *RedisReplayStore) roundTrip(c net.Conn, args ...string) (string, error) {
	_ = c.SetDeadline(time.Now().Add(s.timeout))
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := c.Write([]byte(b.String())); err != nil {
		return "", err
	}
	line, err := bufio.NewReaderSize(c, 512).ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if strings.HasPrefix(line, "-") {
		return "", errors.New("auth: redis error reply")
	}
	return line, nil
}
