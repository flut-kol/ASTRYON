package bff

import (
	"bufio"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// RedisConfig configures the shared session store. Everything comes from the environment (see cmd/admin-bff);
// URL may carry the password and is never logged. Sessions hold access and refresh tokens, so they are sealed
// with AES-256-GCM (Key, 32 bytes) before they reach Redis: a Redis dump alone does not yield usable tokens.
type RedisConfig struct {
	URL    string // redis://[:password@]host:port[/db] or rediss:// (TLS)
	Key    []byte // 32-byte AES key
	Prefix string // key prefix (default "astryon-bff:")
}

type redisStore struct {
	rc   *redisClient
	aead cipher.AEAD
	pre  string
}

// NewRedisStore connects (PING) and returns a Store backed by Redis.
func NewRedisStore(ctx context.Context, c RedisConfig) (Store, error) {
	if len(c.Key) != 32 {
		return nil, errors.New("bff: the Redis session key must be 32 bytes (base64 in BFF_SESSION_KEY)")
	}
	blk, _ := aes.NewCipher(c.Key)
	aead, _ := cipher.NewGCM(blk)
	rc, err := newRedisClient(c.URL)
	if err != nil {
		return nil, err
	}
	if _, err := rc.do(ctx, "PING"); err != nil {
		return nil, fmt.Errorf("bff: redis: %w", err)
	}
	pre := c.Prefix
	if pre == "" {
		pre = "astryon-bff:"
	}
	return &redisStore{rc: rc, aead: aead, pre: pre}, nil
}

// ParseSessionKey decodes the base64 (std or url, padded or not) value of BFF_SESSION_KEY.
func ParseSessionKey(v string) ([]byte, error) {
	v = strings.TrimSpace(v)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(v); err == nil && len(b) == 32 {
			return b, nil
		}
	}
	return nil, errors.New("must be 32 random bytes, base64 encoded")
}

func (s *redisStore) seal(key string, v any) ([]byte, error) {
	pt, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return s.aead.Seal(nonce, nonce, pt, []byte(key)), nil // the key name is authenticated: values cannot be swapped between keys
}

func (s *redisStore) open(key string, raw []byte, dst any) error {
	n := s.aead.NonceSize()
	if len(raw) < n {
		return errors.New("bff: sealed value too short")
	}
	pt, err := s.aead.Open(nil, raw[:n], raw[n:], []byte(key))
	if err != nil {
		return errors.New("bff: sealed value does not authenticate (wrong BFF_SESSION_KEY?)")
	}
	return json.Unmarshal(pt, dst)
}

func secs(d time.Duration) string {
	n := int64((d + time.Second - 1) / time.Second)
	if n < 1 {
		n = 1
	}
	return strconv.FormatInt(n, 10)
}

func (s *redisStore) SavePending(ctx context.Context, state string, p Pending, ttl time.Duration) error {
	k := s.pre + "p:" + state // bounded by TTL; no count cap (the login endpoint should be rate limited at the edge)
	b, err := s.seal(k, p)
	if err != nil {
		return err
	}
	_, err = s.rc.do(ctx, "SET", k, string(b), "EX", secs(ttl))
	return err
}

func (s *redisStore) TakePending(ctx context.Context, state string) (Pending, bool, error) {
	k := s.pre + "p:" + state
	v, err := s.rc.do(ctx, "GETDEL", k)
	if err != nil || v == nil {
		return Pending{}, false, err
	}
	var p Pending
	if err := s.open(k, v.([]byte), &p); err != nil {
		return Pending{}, false, nil
	}
	return p, true, nil
}

func (s *redisStore) index(ctx context.Context, kind, val, id string, ttl time.Duration) error {
	if val == "" {
		return nil
	}
	k := s.pre + kind + ":" + val
	if _, err := s.rc.do(ctx, "SADD", k, id); err != nil {
		return err
	}
	_, err := s.rc.do(ctx, "EXPIRE", k, secs(ttl))
	return err
}

func (s *redisStore) CreateSession(ctx context.Context, id string, r Record, ttl time.Duration) error {
	k := s.pre + "s:" + id
	b, err := s.seal(k, r)
	if err != nil {
		return err
	}
	if _, err := s.rc.do(ctx, "SET", k, string(b), "EX", secs(ttl)); err != nil {
		return err
	}
	if err := s.index(ctx, "sid", r.SID, id, ttl); err != nil {
		return err
	}
	return s.index(ctx, "sub", r.Sub, id, ttl)
}

func (s *redisStore) GetSession(ctx context.Context, id string) (Record, bool, error) {
	k := s.pre + "s:" + id
	v, err := s.rc.do(ctx, "GET", k)
	if err != nil || v == nil {
		return Record{}, false, err
	}
	var r Record
	if err := s.open(k, v.([]byte), &r); err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

func (s *redisStore) UpdateSession(ctx context.Context, id string, r Record, ttl time.Duration) error {
	k := s.pre + "s:" + id
	b, err := s.seal(k, r)
	if err != nil {
		return err
	}
	_, err = s.rc.do(ctx, "SET", k, string(b), "EX", secs(ttl), "XX") // XX: never resurrect an ended session
	return err
}

func (s *redisStore) DeleteSession(ctx context.Context, id string) error {
	r, ok, err := s.GetSession(ctx, id)
	if _, derr := s.rc.do(ctx, "DEL", s.pre+"s:"+id); derr != nil {
		return derr
	}
	if err == nil && ok { // best effort index cleanup; stale members are harmless and expire with the set
		if r.SID != "" {
			_, _ = s.rc.do(ctx, "SREM", s.pre+"sid:"+r.SID, id)
		}
		if r.Sub != "" {
			_, _ = s.rc.do(ctx, "SREM", s.pre+"sub:"+r.Sub, id)
		}
	}
	return nil
}

func (s *redisStore) deleteIndexed(ctx context.Context, kind, val string) ([]Record, error) {
	ik := s.pre + kind + ":" + val
	v, err := s.rc.do(ctx, "SMEMBERS", ik)
	if err != nil {
		return nil, err
	}
	var deleted []Record
	arr, _ := v.([]any)
	for _, m := range arr {
		b, _ := m.([]byte)
		id := string(b)
		rec, ok, getErr := s.GetSession(ctx, id)
		if getErr != nil {
			return deleted, getErr
		}
		r, err := s.rc.do(ctx, "DEL", s.pre+"s:"+id)
		if err != nil {
			return deleted, err
		}
		if c, _ := r.(int64); c > 0 && ok {
			deleted = append(deleted, rec)
		}
	}
	_, err = s.rc.do(ctx, "DEL", ik)
	return deleted, err
}

func (s *redisStore) DeleteBySID(ctx context.Context, sid string) ([]Record, error) {
	return s.deleteIndexed(ctx, "sid", sid)
}

func (s *redisStore) DeleteBySub(ctx context.Context, sub string) ([]Record, error) {
	return s.deleteIndexed(ctx, "sub", sub)
}

func (s *redisStore) SeenJTI(ctx context.Context, jti string, ttl time.Duration) (bool, error) {
	v, err := s.rc.do(ctx, "SET", s.pre+"jti:"+jti, "1", "NX", "EX", secs(ttl))
	if err != nil {
		return false, err
	}
	return v != nil, nil // nil reply = key existed = replay
}

// --- minimal RESP2 client (stdlib only; the BFF needs a dozen commands, not a dependency) ---

type redisConn struct {
	c    net.Conn
	r    *bufio.Reader
	last time.Time
}

type redisClient struct {
	addr, pass string
	db         int
	useTLS     bool
	host       string
	pool       chan *redisConn
}

func newRedisClient(raw string) (*redisClient, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "redis" && u.Scheme != "rediss") || u.Host == "" {
		return nil, errors.New("bff: BFF_REDIS_URL must look like redis://[:password@]host:port[/db] or rediss://...")
	}
	c := &redisClient{useTLS: u.Scheme == "rediss", host: u.Hostname(), addr: u.Host, pool: make(chan *redisConn, 8)}
	if u.Port() == "" {
		c.addr = net.JoinHostPort(u.Hostname(), "6379")
	}
	c.pass, _ = u.User.Password()
	if p := strings.Trim(u.Path, "/"); p != "" {
		if c.db, err = strconv.Atoi(p); err != nil || c.db < 0 {
			return nil, errors.New("bff: redis db in BFF_REDIS_URL must be a number")
		}
	}
	return c, nil
}

func (c *redisClient) dial(ctx context.Context) (*redisConn, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	var nc net.Conn
	var err error
	if c.useTLS {
		nc, err = (&tls.Dialer{NetDialer: &d, Config: &tls.Config{ServerName: c.host, MinVersion: tls.VersionTLS12}}).DialContext(ctx, "tcp", c.addr)
	} else {
		nc, err = d.DialContext(ctx, "tcp", c.addr)
	}
	if err != nil {
		return nil, err
	}
	rc := &redisConn{c: nc, r: bufio.NewReader(nc)}
	if c.pass != "" {
		if _, err := rc.cmd("AUTH", c.pass); err != nil {
			nc.Close()
			return nil, err
		}
	}
	if c.db != 0 {
		if _, err := rc.cmd("SELECT", strconv.Itoa(c.db)); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return rc, nil
}

func (c *redisClient) do(ctx context.Context, args ...string) (any, error) {
	var rc *redisConn
	for rc == nil {
		select {
		case p := <-c.pool:
			if time.Since(p.last) > 30*time.Second { // idle connections may have been closed by the server
				p.c.Close()
				continue
			}
			rc = p
		default:
			var err error
			if rc, err = c.dial(ctx); err != nil {
				return nil, err
			}
		}
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = rc.c.SetDeadline(dl)
	} else {
		_ = rc.c.SetDeadline(time.Now().Add(5 * time.Second))
	}
	v, err := rc.cmd(args...)
	var re redisError
	if err != nil && !errors.As(err, &re) { // protocol/network error: the connection state is unknown
		rc.c.Close()
		return nil, err
	}
	rc.last = time.Now()
	select {
	case c.pool <- rc:
	default:
		rc.c.Close()
	}
	return v, err
}

type redisError string

func (e redisError) Error() string { return "redis: " + string(e) }

func (rc *redisConn) cmd(args ...string) (any, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := io.WriteString(rc.c, b.String()); err != nil {
		return nil, err
	}
	return readReply(rc.r)
}

func readReply(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, errors.New("redis: short reply")
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, redisError(body)
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		return n, err
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil || n > 16<<20 {
			return nil, errors.New("redis: bad bulk length")
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf[:n], nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil || n > 1<<20 {
			return nil, errors.New("redis: bad array length")
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			v, err := readReply(r)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	return nil, errors.New("redis: unexpected reply type")
}
