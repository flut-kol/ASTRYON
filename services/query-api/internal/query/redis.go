package query

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1" // #nosec G505 -- Redis EVALSHA requires SHA-1 script digests
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LoadShared builds the optional cross-replica limiter store from QUERY_REDIS_URL
// (redis[s]://[user:pass@]host:port[/db]), QUERY_REDIS_NODES (extra cluster seed nodes, comma list host:port),
// QUERY_REDIS_CA_FILE / QUERY_REDIS_TLS_SERVER_NAME (rediss:// only) and QUERY_REDIS_TOPOLOGY_REFRESH_SECONDS (default 60, 0 = off), QUERY_REDIS_SENTINELS + QUERY_REDIS_MASTER_NAME
// (Sentinel) and QUERY_REDIS_FAILURE (local|closed, default local). URL unset -> (nil, FailLocal, nil): limits stay per instance. The password lives only in the environment.
func LoadShared(get func(string) string) (SharedCounter, FailureMode, error) {
	mode := FailureMode(strings.ToLower(strings.TrimSpace(get("QUERY_REDIS_FAILURE"))))
	switch mode {
	case "":
		mode = FailLocal
	case FailLocal, FailClosed:
	default:
		return nil, "", fmt.Errorf("QUERY_REDIS_FAILURE=%q: must be local or closed", mode)
	}
	raw := strings.TrimSpace(get("QUERY_REDIS_URL"))
	if raw == "" {
		return nil, mode, nil
	}
	opts := RedisOptions{CAFile: strings.TrimSpace(get("QUERY_REDIS_CA_FILE")), ServerName: strings.TrimSpace(get("QUERY_REDIS_TLS_SERVER_NAME"))}
	for _, n := range strings.Split(get("QUERY_REDIS_NODES"), ",") {
		if n = strings.TrimSpace(n); n != "" {
			opts.Nodes = append(opts.Nodes, n)
		}
	}
	if v := strings.TrimSpace(get("QUERY_REDIS_TOPOLOGY_REFRESH_SECONDS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return nil, "", fmt.Errorf("QUERY_REDIS_TOPOLOGY_REFRESH_SECONDS=%q: want a non-negative integer (0 = off)", v)
		}
		opts.TopologyRefresh, opts.NoTopologyRefresh = time.Duration(n)*time.Second, n == 0
	}
	for _, n := range strings.Split(get("QUERY_REDIS_SENTINELS"), ",") {
		if n = strings.TrimSpace(n); n != "" {
			opts.Sentinels = append(opts.Sentinels, n)
		}
	}
	opts.MasterName = strings.TrimSpace(get("QUERY_REDIS_MASTER_NAME"))
	rc, err := NewRedisCounterOpts(raw, opts)
	if err != nil {
		return nil, "", err
	}
	return rc, mode, nil
}

// limiterScript atomically enforces the per-tenant concurrency cap (sorted set of lease expiries) and the token
// bucket (hash tokens/ts), using the Redis server clock so replicas with skewed clocks agree.
// KEYS[1]=q:rate:{<tenant>} KEYS[2]=q:conc:{<tenant>} (same hash tag -> same Cluster slot);
// ARGV: rate, burst, maxConcurrent, leaseMs, slotId.
// Returns {0,0,tokensLeft} admitted | {1,1} concurrency cap | {2,retryAfterSeconds,tokens} rate limited. No token is consumed unless admitted.
const limiterScript = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local maxc = tonumber(ARGV[3])
local lease = tonumber(ARGV[4])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if redis.call('ZCARD', KEYS[2]) >= maxc then return {1, 1} end
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil or ts == nil then tokens = burst; ts = now end
tokens = math.min(burst, tokens + math.max(0, now - ts) / 1000 * rate)
local ttl = math.ceil(burst / rate) * 1000 + 1000
if tokens < 1 then
  redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
  redis.call('PEXPIRE', KEYS[1], ttl)
  return {2, math.ceil((1 - tokens) / rate), tostring(tokens)}
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens - 1), 'ts', now)
redis.call('PEXPIRE', KEYS[1], ttl)
redis.call('ZADD', KEYS[2], now + lease, ARGV[5])
redis.call('PEXPIRE', KEYS[2], lease)
return {0, 0, tostring(tokens - 1)}
`

// renewScript extends a live lease. KEYS[1]=q:conc:{<tenant>}; ARGV: leaseMs, slotId. Returns 1 renewed, 0 when the
// slot is gone or already expired (released, or lost): an expired slot is never resurrected, so the cap cannot be
// exceeded by a late renewal.
const renewScript = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local s = redis.call('ZSCORE', KEYS[1], ARGV[2])
if s == false or tonumber(s) <= now then return 0 end
local lease = tonumber(ARGV[1])
redis.call('ZADD', KEYS[1], now + lease, ARGV[2])
redis.call('PEXPIRE', KEYS[1], lease)
return 1
`

func scriptSHA(s string) string { h := sha1.Sum([]byte(s)); return hex.EncodeToString(h[:]) } // #nosec G401 -- Redis EVALSHA requires SHA-1 script digests

var (
	limiterSHA = scriptSHA(limiterScript)
	renewSHA   = scriptSHA(renewScript)
)

// ---- Redis Cluster addressing ----

func crc16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc ^= uint16(c) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// keySlot is the Redis Cluster slot of a key, honouring {hash tags}.
func keySlot(key string) uint16 {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		if j := strings.IndexByte(key[i+1:], '}'); j > 0 {
			key = key[i+1 : i+1+j]
		}
	}
	return crc16([]byte(key)) % 16384
}

var tagEscaper = strings.NewReplacer("%", "%25", "{", "%7B", "}", "%7D")

// tenantTag is the hash-tag content for a tenant. Braces are escaped so a tenant id can never end the tag early
// (tenant ids are UUIDs after validation; this is defence in depth).
func tenantTag(tenant string) string { return tagEscaper.Replace(tenant) }

func rateKey(tenant string) string { return "q:rate:{" + tenantTag(tenant) + "}" }
func concKey(tenant string) string { return "q:conc:{" + tenantTag(tenant) + "}" }

// RedisOptions are the optional TLS and cluster settings.
type RedisOptions struct {
	CAFile     string   // PEM bundle for a private CA (rediss:// only); empty = system roots
	ServerName string   // TLS server name override (rediss:// only); empty = host of each dialled address
	Nodes      []string // extra seed nodes host:port (Redis Cluster); the URL host is always the first seed

	// TopologyRefresh is the periodic CLUSTER SLOTS / Sentinel refresh interval (0 = default 60s). NoTopologyRefresh
	// turns the periodic refresh off; error-triggered refreshes (connection loss, CLUSTERDOWN, READONLY, MOVED storm) stay on.
	TopologyRefresh   time.Duration
	NoTopologyRefresh bool

	// Sentinels + MasterName select Redis Sentinel mode: the master address is asked from the sentinels
	// (SENTINEL get-master-addr-by-name) instead of a fixed URL host. Mutually exclusive with Nodes.
	Sentinels  []string
	MasterName string
}

// RedisCounter is a SharedCounter over a minimal stdlib RESP2 client. Keys are q:rate:{<tenant>} and
// q:conc:{<tenant>}; -MOVED/-ASK redirects are followed (Redis Cluster), single-node use is unaffected.
type RedisCounter struct {
	nodes       []string
	user, pass  string
	db          int
	tlsCfg      *tls.Config // nil = plaintext
	serverName  string
	DialTimeout time.Duration
	CmdTimeout  time.Duration
	mu          sync.Mutex
	pools       map[string]chan *redisConn
	slotAddr    map[uint16]string // slot -> node learned from MOVED (overrides ranges; cleared on every refresh)

	// topology (see redis_topology.go); all guarded by mu
	ranges       []slotRange // master of each slot range from the last CLUSTER SLOTS / Sentinel lookup, sorted
	known        []string    // masters from the last refresh (fallback dial candidates, capped)
	lastRefresh  time.Time
	refreshing   bool
	noCluster    bool // server says cluster support is disabled: standalone, stop refreshing
	movedSince   int
	refreshEvery time.Duration // 0 = periodic refresh off
	minGap       time.Duration // minimum spacing of refresh attempts
	sentinels    []string
	masterName   string
	now          func() time.Time
	refreshes    atomic.Uint64
	moved, asked atomic.Uint64 // redirects followed (observability/tests)
}

// NewRedisCounter parses redis[s]://[user:password@]host[:port][/db] with default options.
func NewRedisCounter(raw string) (*RedisCounter, error) {
	return NewRedisCounterOpts(raw, RedisOptions{})
}

// NewRedisCounterOpts is NewRedisCounter with TLS/cluster options. TLS verification is always on (never
// InsecureSkipVerify); MinVersion is TLS 1.2.
func NewRedisCounterOpts(raw string, o RedisOptions) (*RedisCounter, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, errors.New("QUERY_REDIS_URL: invalid URL (want redis[s]://[user:pass@]host:port[/db])")
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, fmt.Errorf("QUERY_REDIS_URL: unsupported scheme %q (want redis:// or rediss://)", u.Scheme)
	}
	rc := &RedisCounter{DialTimeout: time.Second, CmdTimeout: 500 * time.Millisecond,
		pools: map[string]chan *redisConn{}, slotAddr: map[uint16]string{}, serverName: o.ServerName,
		refreshEvery: defaultTopologyRefresh, minGap: time.Second, now: time.Now, lastRefresh: time.Now()}
	if o.TopologyRefresh > 0 {
		rc.refreshEvery = o.TopologyRefresh
	}
	if o.NoTopologyRefresh {
		rc.refreshEvery = 0
	}
	if len(o.Sentinels) > 0 || o.MasterName != "" {
		if len(o.Sentinels) == 0 || o.MasterName == "" || len(o.Nodes) > 0 {
			return nil, errors.New("QUERY_REDIS_SENTINELS and QUERY_REDIS_MASTER_NAME must be set together and cannot be combined with QUERY_REDIS_NODES")
		}
		for _, n := range o.Sentinels {
			a, err := normalizeNode(n)
			if err != nil {
				return nil, fmt.Errorf("QUERY_REDIS_SENTINELS: %w", err)
			}
			rc.sentinels = append(rc.sentinels, a)
		}
		rc.masterName = o.MasterName
		rc.lastRefresh = time.Time{} // resolve on first use
		if rc.refreshEvery > sentinelRefreshCap {
			rc.refreshEvery = sentinelRefreshCap // a promotion is noticed without waiting for the demoted master to answer -READONLY
		}
	}
	rc.nodes = []string{withPort(u.Host, u)}
	for _, n := range o.Nodes {
		a, err := normalizeNode(n)
		if err != nil {
			return nil, err
		}
		dup := false
		for _, e := range rc.nodes {
			dup = dup || e == a
		}
		if !dup {
			rc.nodes = append(rc.nodes, a)
		}
	}
	if u.Scheme == "rediss" {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if o.CAFile != "" {
			pem, err := os.ReadFile(o.CAFile)
			if err != nil {
				return nil, fmt.Errorf("QUERY_REDIS_CA_FILE: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("QUERY_REDIS_CA_FILE: no PEM certificates found")
			}
			cfg.RootCAs = pool
		}
		rc.tlsCfg = cfg
	} else if o.CAFile != "" || o.ServerName != "" {
		return nil, errors.New("QUERY_REDIS_CA_FILE / QUERY_REDIS_TLS_SERVER_NAME require a rediss:// URL")
	}
	if u.User != nil {
		rc.user = u.User.Username()
		rc.pass, _ = u.User.Password()
		if rc.pass == "" && rc.user != "" { // redis://secret@host style
			rc.pass, rc.user = rc.user, ""
		}
	}
	if p := strings.Trim(u.Path, "/"); p != "" {
		if rc.db, err = strconv.Atoi(p); err != nil || rc.db < 0 {
			return nil, errors.New("QUERY_REDIS_URL: bad db index")
		}
	}
	return rc, nil
}

func withPort(host string, u *url.URL) string {
	if u.Port() == "" {
		return net.JoinHostPort(u.Hostname(), "6379")
	}
	return host
}

func normalizeNode(n string) (string, error) {
	h, p, err := net.SplitHostPort(n)
	if err != nil {
		h, p, err = n, "6379", nil
	}
	if h == "" {
		return "", fmt.Errorf("QUERY_REDIS_NODES: bad node %q (want host:port)", n)
	}
	if _, perr := strconv.Atoi(p); perr != nil {
		return "", fmt.Errorf("QUERY_REDIS_NODES: bad port in %q", n)
	}
	return net.JoinHostPort(h, p), nil
}

// FailureDomain is the master currently serving the tenant's slot (MOVED override, then CLUSTER SLOTS map), else the
// first seed. A Redis error then only pauses Redis for tenants of that node (see Limiter / DomainResolver); a
// single node or Sentinel setup has one domain, i.e. a global cooldown as before.
func (r *RedisCounter) FailureDomain(tenant string) string {
	if a := r.owner(keySlot(rateKey(tenant))); a != "" {
		return a
	}
	return r.nodes[0]
}

// GlobalFailure is true for "CLUSTERDOWN The cluster is down": with cluster-require-full-coverage yes (the default) every
// node answers it while any slot is uncovered, e.g. during a failover, so it is not specific to the tenant's node.
// "CLUSTERDOWN Hash slot not served" (coverage not required) stays per domain.
func (r *RedisCounter) GlobalFailure(err error) bool {
	var re redisError
	return errors.As(err, &re) && strings.HasPrefix(string(re), "CLUSTERDOWN The cluster is down")
}

func (r *RedisCounter) Acquire(tenant string, cfg LimiterConfig) (SharedDecision, error) {
	if tenant == "" {
		return SharedDecision{}, errors.New("empty tenant")
	}
	var id [12]byte
	if _, err := rand.Read(id[:]); err != nil {
		return SharedDecision{}, err
	}
	slot := hex.EncodeToString(id[:])
	cfg = cfg.withDefaults()
	v, err := r.eval(tenant, limiterScript, limiterSHA, []string{rateKey(tenant), concKey(tenant)},
		strconv.FormatFloat(cfg.RatePerSec, 'g', -1, 64), strconv.Itoa(cfg.Burst), strconv.Itoa(cfg.MaxConcurrent),
		strconv.FormatInt(cfg.SlotLease.Milliseconds(), 10), slot)
	if err != nil {
		return SharedDecision{}, err
	}
	arr, ok := v.([]any)
	if !ok || len(arr) < 2 || len(arr) > 3 {
		return SharedDecision{}, fmt.Errorf("unexpected script reply %v", v)
	}
	code, _ := arr[0].(int64)
	ra, _ := arr[1].(int64)
	var toks float64
	var known bool
	if len(arr) == 3 {
		if s, ok := arr[2].(string); ok {
			if f, err := strconv.ParseFloat(s, 64); err == nil && f >= 0 {
				toks, known = f, true
			}
		}
	}
	switch code {
	case 0:
		return SharedDecision{Admitted: true, Slot: slot, Tokens: toks, TokensKnown: known}, nil
	case 1:
		return SharedDecision{Reason: "concurrent", RetryAfter: 1}, nil
	case 2:
		return SharedDecision{Reason: "rate", RetryAfter: int(ra), Tokens: toks, TokensKnown: known}, nil
	}
	return SharedDecision{}, fmt.Errorf("unexpected script code %d", code)
}

// Renew extends the lease of a held slot. It reports false when the slot no longer exists (released or expired).
func (r *RedisCounter) Renew(tenant, slot string, cfg LimiterConfig) (bool, error) {
	cfg = cfg.withDefaults()
	v, err := r.eval(tenant, renewScript, renewSHA, []string{concKey(tenant)},
		strconv.FormatInt(cfg.SlotLease.Milliseconds(), 10), slot)
	if err != nil {
		return false, err
	}
	n, _ := v.(int64)
	return n == 1, nil
}

func (r *RedisCounter) Release(tenant, slot string) error {
	_, err := r.exec(tenant, "ZREM", concKey(tenant), slot)
	return err
}

// eval runs a script by SHA with an EVAL fallback on NOSCRIPT. The caller passes the keys (all in tenant's slot).
func (r *RedisCounter) eval(tenant, script, sha string, keys []string, argv ...string) (any, error) {
	tail := append([]string{strconv.Itoa(len(keys))}, keys...)
	tail = append(tail, argv...)
	v, err := r.exec(tenant, append([]string{"EVALSHA", sha}, tail...)...)
	var re redisError
	if errors.As(err, &re) && strings.HasPrefix(string(re), "NOSCRIPT") {
		v, err = r.exec(tenant, append([]string{"EVAL", script}, tail...)...)
	}
	return v, err
}

// ---- routing: single node by default, Cluster redirects followed ----

const maxRedirects = 3

type dialError struct{ err error }

func (e dialError) Error() string { return e.err.Error() }
func (e dialError) Unwrap() error { return e.err }

// exec routes one command for tenant: to the master owning its slot (CLUSTER SLOTS map, then MOVED overrides), else a
// seed node. -MOVED updates the cache and retries on the target; -ASK retries once on the target preceded by ASKING.
// A command is only re-sent when it provably was not executed (dial failure, or a -MOVED/-ASK/-READONLY/-TRYAGAIN
// rejection); after an I/O error mid-command the error is returned (the script may have run) and the topology refreshed.
func (r *RedisCounter) exec(tenant string, args ...string) (any, error) {
	slot := keySlot(rateKey(tenant))
	r.topologyTick()
	addr := r.owner(slot)
	if addr == "" {
		addr = r.nodes[0]
	}
	asking := false
	tried := map[string]bool{}
	hop, retry := 0, 0
	for {
		tried[addr] = true
		v, err := r.doAt(addr, asking, args...)
		var de dialError
		if errors.As(err, &de) { // node down: forget it, refresh the map, else try the next known node
			r.forget(slot, addr)
			if r.refresh() {
				if a := r.owner(slot); a != "" && !tried[a] {
					addr, asking = a, false
					continue
				}
			}
			next := r.nextCandidate(tried)
			if next == "" {
				return nil, err
			}
			addr, asking = next, false
			continue
		}
		var re redisError
		if !errors.As(err, &re) {
			if err != nil {
				r.refresh() // timeout/reset: outcome unknown, never re-sent; learn the new topology for the next call
			}
			return v, err
		}
		msg := string(re)
		switch {
		case strings.HasPrefix(msg, "READONLY"), strings.HasPrefix(msg, "TRYAGAIN"), strings.HasPrefix(msg, "CLUSTERDOWN"), strings.HasPrefix(msg, "LOADING"), strings.HasPrefix(msg, "MASTERDOWN"):
			// rejected before execution. READONLY = we talk to a demoted master (failover); CLUSTERDOWN = slot not served
			// (yet); TRYAGAIN = slot is mid-migration. Refresh and retry a bounded number of times.
			if retry >= 2 {
				if strings.HasPrefix(msg, "CLUSTERDOWN") || strings.HasPrefix(msg, "READONLY") {
					r.refresh()
				}
				return v, err
			}
			retry++
			if strings.HasPrefix(msg, "TRYAGAIN") {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			r.forget(slot, addr)
			r.refresh()
			if a := r.owner(slot); a != "" {
				addr, asking = a, false
			} else if a := r.nextCandidate(tried); a != "" {
				addr, asking = a, false
			} else {
				return v, err
			}
			continue
		}
		kind, target, ok := parseRedirect(msg, addr)
		if !ok || hop >= maxRedirects {
			return v, err
		}
		hop++
		if kind == "MOVED" {
			r.mu.Lock()
			if len(r.slotAddr) < 16384 {
				r.slotAddr[slot] = target
			}
			r.mu.Unlock()
			r.moved.Add(1)
			r.noteMoved()
		}
		if kind == "ASK" {
			r.asked.Add(1)
		}
		addr, asking = target, kind == "ASK"
	}
}

// parseRedirect parses "MOVED <slot> <host:port>" / "ASK <slot> <host:port>"; an empty host means the current node's.
func parseRedirect(msg, cur string) (kind, target string, ok bool) {
	f := strings.Fields(msg)
	if len(f) != 3 || (f[0] != "MOVED" && f[0] != "ASK") {
		return "", "", false
	}
	h, p, err := net.SplitHostPort(f[2])
	if err != nil {
		return "", "", false
	}
	if _, err := strconv.Atoi(p); err != nil {
		return "", "", false
	}
	if h == "" {
		ch, _, err := net.SplitHostPort(cur)
		if err != nil {
			return "", "", false
		}
		h = ch
	}
	return f[0], net.JoinHostPort(h, p), true
}

// ---- minimal RESP2 client ----

type redisError string

func (e redisError) Error() string { return "redis: " + string(e) }

type redisConn struct {
	c net.Conn
	r *bufio.Reader
}

func (r *RedisCounter) pool(addr string) chan *redisConn {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pools[addr]
	if p == nil {
		p = make(chan *redisConn, 4)
		r.pools[addr] = p
	}
	return p
}

// dialRaw opens a (TLS) connection without AUTH/SELECT (Sentinel lookups use it as is).
func (r *RedisCounter) dialRaw(addr string) (*redisConn, error) {
	d := &net.Dialer{Timeout: r.DialTimeout}
	var c net.Conn
	var err error
	if r.tlsCfg != nil {
		cfg := r.tlsCfg.Clone()
		cfg.ServerName = r.serverName
		if cfg.ServerName == "" {
			cfg.ServerName, _, _ = net.SplitHostPort(addr)
		}
		c, err = tls.DialWithDialer(d, "tcp", addr, cfg) // handshake bounded by DialTimeout; verification always on
	} else {
		c, err = d.Dial("tcp", addr)
	}
	if err != nil {
		return nil, dialError{err}
	}
	return &redisConn{c: c, r: bufio.NewReader(c)}, nil
}

func (r *RedisCounter) dial(addr string) (*redisConn, error) {
	rc, err := r.dialRaw(addr)
	if err != nil {
		return nil, err
	}
	c := rc.c
	if r.pass != "" {
		args := []string{"AUTH", r.pass}
		if r.user != "" {
			args = []string{"AUTH", r.user, r.pass}
		}
		if _, err := rc.cmd(r.CmdTimeout, args...); err != nil {
			_ = c.Close()
			return nil, errors.New("redis auth failed") // never echo credentials
		}
	}
	if r.db != 0 {
		if _, err := rc.cmd(r.CmdTimeout, "SELECT", strconv.Itoa(r.db)); err != nil {
			_ = c.Close()
			return nil, err
		}
	}
	return rc, nil
}

// doAt runs one command on a pooled connection to addr with a per-command deadline (preceded by ASKING when
// asking). Server error replies (redisError) keep the connection; any I/O error discards it.
func (r *RedisCounter) doAt(addr string, asking bool, args ...string) (any, error) {
	p := r.pool(addr)
	var rc *redisConn
	select {
	case rc = <-p:
	default:
		var err error
		if rc, err = r.dial(addr); err != nil {
			return nil, err
		}
	}
	var v any
	var err error
	if asking {
		_, err = rc.cmd(r.CmdTimeout, "ASKING")
	}
	if err == nil {
		v, err = rc.cmd(r.CmdTimeout, args...)
	}
	var re redisError
	if err != nil && !errors.As(err, &re) {
		_ = rc.c.Close()
		return nil, err
	}
	select {
	case p <- rc:
	default:
		_ = rc.c.Close()
	}
	return v, err
}

func (c *redisConn) cmd(timeout time.Duration, args ...string) (any, error) {
	_ = c.c.SetDeadline(time.Now().Add(timeout))
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	if _, err := io.WriteString(c.c, b.String()); err != nil {
		return nil, err
	}
	return readReply(c.r, 0)
}

func readReply(r *bufio.Reader, depth int) (any, error) {
	if depth > 4 {
		return nil, errors.New("reply nested too deep")
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || !strings.HasSuffix(line, "\r\n") {
		return nil, errors.New("malformed reply")
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, redisError(body)
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil || n > 1<<20 {
			return nil, errors.New("bad bulk length")
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil || n > 32768 {
			return nil, errors.New("bad array length")
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := range out {
			if out[i], err = readReply(r, depth+1); err != nil {
				var re redisError
				if errors.As(err, &re) { // error element inside array: keep stream in sync, surface as value
					out[i] = re
					continue
				}
				return nil, err
			}
		}
		return out, nil
	}
	return nil, errors.New("unknown reply type")
}
