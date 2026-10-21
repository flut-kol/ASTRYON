package query

import (
	"errors"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Topology: the slot -> master map is learned from CLUSTER SLOTS (or, in Sentinel mode, from
// SENTINEL get-master-addr-by-name, modelled as one range covering every slot) and refreshed
//   - lazily every refreshEvery (default 60s; no background goroutine, at most one refresh in flight),
//   - immediately on connection errors, -READONLY, -CLUSTERDOWN, and after a -MOVED storm,
//
// with attempts spaced at least minGap apart. Only masters are ever routed to (replicas are never read: the limiter
// must read what it writes). -MOVED overrides stay valid until the next refresh replaces the whole map.

const (
	defaultTopologyRefresh = 60 * time.Second
	sentinelRefreshCap     = 5 * time.Second // periodic Sentinel lookups (one tiny command) are never rarer than this
	movedStorm             = 8               // MOVED replies since the last refresh that force one
	maxKnown               = 64              // cap of remembered master addresses used as dial fallbacks
	maxRefreshCandidates   = 8               // nodes asked per refresh attempt
)

type slotRange struct {
	start, end uint16
	addr       string
}

// owner returns the master for slot: MOVED override first, then the refreshed map; "" when unknown.
func (r *RedisCounter) owner(slot uint16) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if a, ok := r.slotAddr[slot]; ok {
		return a
	}
	i := sort.Search(len(r.ranges), func(i int) bool { return r.ranges[i].start > slot }) - 1
	if i >= 0 && r.ranges[i].end >= slot {
		return r.ranges[i].addr
	}
	return ""
}

// forget drops the MOVED override for slot when it points at the failed addr.
func (r *RedisCounter) forget(slot uint16, addr string) {
	r.mu.Lock()
	if r.slotAddr[slot] == addr {
		delete(r.slotAddr, slot)
	}
	r.mu.Unlock()
}

// nextCandidate returns an untried seed or known master.
func (r *RedisCounter) nextCandidate(tried map[string]bool) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range [][]string{r.nodes, r.known} {
		for _, n := range l {
			if !tried[n] {
				return n
			}
		}
	}
	return ""
}

func (r *RedisCounter) noteMoved() {
	r.mu.Lock()
	r.movedSince++
	storm := r.movedSince >= movedStorm
	r.mu.Unlock()
	if storm {
		r.refresh()
	}
}

// topologyTick starts the periodic refresh (async, single flight) when it is due, and the first Sentinel lookup.
func (r *RedisCounter) topologyTick() {
	r.mu.Lock()
	due := !r.noCluster && !r.refreshing && r.minGapOK() &&
		((len(r.sentinels) > 0 && len(r.ranges) == 0) || (r.refreshEvery > 0 && r.now().Sub(r.lastRefresh) >= r.refreshEvery))
	sync := len(r.sentinels) > 0 && len(r.ranges) == 0
	r.mu.Unlock()
	if !due {
		return
	}
	if sync {
		r.refresh()
		return
	}
	go r.refresh()
}

func (r *RedisCounter) minGapOK() bool { return r.now().Sub(r.lastRefresh) >= r.minGap }

// refresh re-learns the topology. It is rate limited (minGap), single flight, and bounded (maxRefreshCandidates
// nodes, one CmdTimeout/DialTimeout each). It reports whether a new map was installed.
func (r *RedisCounter) refresh() bool {
	r.mu.Lock()
	if r.noCluster || r.refreshing || !r.minGapOK() {
		r.mu.Unlock()
		return false
	}
	r.refreshing = true
	r.lastRefresh = r.now()
	cands := append([]string(nil), r.nodes...)
	cands = append(cands, r.known...)
	sentinels := r.sentinels
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.refreshing = false; r.mu.Unlock() }()

	var ranges []slotRange
	if len(sentinels) > 0 {
		a := r.askSentinels(sentinels)
		if a == "" {
			return false
		}
		ranges = []slotRange{{0, 16383, a}}
	} else {
		seen := map[string]bool{}
		n := 0
		for _, c := range cands {
			if seen[c] || n >= maxRefreshCandidates {
				continue
			}
			seen[c] = true
			n++
			v, err := r.doAt(c, false, "CLUSTER", "SLOTS")
			var re redisError
			if errors.As(err, &re) {
				if strings.Contains(strings.ToLower(string(re)), "cluster support disabled") {
					r.mu.Lock()
					r.noCluster = true
					r.mu.Unlock()
					return false
				}
				continue
			}
			if err != nil {
				continue
			}
			if rs, perr := parseClusterSlots(v, c); perr == nil && len(rs) > 0 {
				ranges = rs
				break
			}
		}
		if ranges == nil {
			return false
		}
	}
	known := make([]string, 0, len(ranges))
	seen := map[string]bool{}
	for _, rg := range ranges {
		if !seen[rg.addr] && len(known) < maxKnown {
			seen[rg.addr] = true
			known = append(known, rg.addr)
		}
	}
	r.mu.Lock()
	r.ranges, r.known = ranges, known
	r.slotAddr = map[uint16]string{}
	r.movedSince = 0
	r.mu.Unlock()
	r.refreshes.Add(1)
	r.dropStalePools(seen)
	return true
}

// dropStalePools closes idle connections to nodes that are neither a current master nor a seed (e.g. a demoted
// master): a stale connection could otherwise keep answering -READONLY.
func (r *RedisCounter) dropStalePools(keep map[string]bool) {
	for _, n := range r.nodes {
		keep[n] = true
	}
	r.mu.Lock()
	var drop []chan *redisConn
	for a, p := range r.pools {
		if !keep[a] {
			drop = append(drop, p)
			delete(r.pools, a)
		}
	}
	r.mu.Unlock()
	for _, p := range drop {
		for {
			select {
			case c := <-p:
				_ = c.c.Close()
				continue
			default:
			}
			break
		}
	}
}

// askSentinels returns the master address reported by the first answering sentinel ("" if none).
func (r *RedisCounter) askSentinels(sentinels []string) string {
	for _, s := range sentinels {
		c, err := r.dialRaw(s)
		if err != nil {
			continue
		}
		v, err := c.cmd(r.CmdTimeout, "SENTINEL", "get-master-addr-by-name", r.masterName)
		_ = c.c.Close()
		arr, ok := v.([]any)
		if err != nil || !ok || len(arr) != 2 {
			continue
		}
		h, _ := arr[0].(string)
		p, _ := arr[1].(string)
		if pn, err := strconv.Atoi(p); h == "" || err != nil || pn <= 0 || pn > 65535 {
			continue
		}
		return net.JoinHostPort(h, p)
	}
	return ""
}

// parseClusterSlots decodes a CLUSTER SLOTS reply keeping only each range's master (first node). An empty, "?" or
// null host (cluster-announce / hostname quirks) means "the node that answered", asked is that node's address.
func parseClusterSlots(v any, asked string) ([]slotRange, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, errors.New("CLUSTER SLOTS: unexpected reply")
	}
	askedHost, _, _ := net.SplitHostPort(asked)
	var out []slotRange
	for _, e := range arr {
		ent, ok := e.([]any)
		if !ok || len(ent) < 3 {
			continue
		}
		s, ok1 := ent[0].(int64)
		en, ok2 := ent[1].(int64)
		node, ok3 := ent[2].([]any)
		if !ok1 || !ok2 || !ok3 || len(node) < 2 || s < 0 || en > 16383 || s > en {
			continue
		}
		host, _ := node[0].(string) // nil (null endpoint) -> ""
		if host == "" || host == "?" {
			host = askedHost
		}
		port, _ := node[1].(int64)
		if host == "" || port <= 0 || port > 65535 {
			continue
		}
		out = append(out, slotRange{uint16(s), uint16(en), net.JoinHostPort(host, strconv.FormatInt(port, 10))}) // #nosec G115 -- small bounded counter or length, reviewed 2026-10-21
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	return out, nil
}
