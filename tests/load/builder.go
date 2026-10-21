package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	mrand "math/rand"
	"strings"
	"time"

	ec "github.com/astryon/event-contracts"
)

// Tenant is one identity the generator can submit as. The gateway takes the
// tenant from the token; the generator mirrors it in the body (a mismatch
// would make the gateway reject the whole batch with 403).
type Tenant struct {
	ID    string
	Token string
}

// Options controls event generation.
type Options struct {
	EventSize    int     // approx serialized envelope size in bytes
	DupRatio     float64 // fraction of events that re-send an earlier valid event
	InvalidRatio float64 // fraction of events that are deliberately invalid
	Sources      int     // number of distinct source_ids per tenant
	TenantSkew   float64 // Zipf exponent over tenants (0 = uniform)
	DupWindow    int     // how many recent valid events may be re-sent

	Shape         string        // payload shape: "" or "small" (flat), "wide" (many fields, nested)
	WideFields    int           // field count for the wide shape (default 120)
	LateRatio     float64       // fraction of valid events with event_time in the past
	LateMax       time.Duration // late events are up to this old (default 24h)
	HotKeyRatio   float64       // fraction of events sharing one entity_key (hot key)
	SchemaInvalid bool          // invalid events are schema-level only (bad schema_id/version), not decode errors
}

// Builder creates event batches. It is NOT safe for concurrent use: one per worker.
type Builder struct {
	opt     Options
	tenants []Tenant
	cdf     []float64
	rng     *mrand.Rand
	now     func() time.Time
	seq     int64
	recent  map[string][]recentEvent // per tenant ring of valid events
	sources map[string][]string
}

type recentEvent struct {
	id  string
	raw []byte
}

// NewBuilder returns a deterministic builder for the given seed.
func NewBuilder(opt Options, tenants []Tenant, seed int64) *Builder {
	if opt.Sources < 1 {
		opt.Sources = 4
	}
	if opt.DupWindow < 1 {
		opt.DupWindow = 1000
	}
	b := &Builder{opt: opt, tenants: tenants, rng: mrand.New(mrand.NewSource(seed)), now: time.Now, // #nosec G404 -- deterministic seeded load data, not security sensitive
		recent: map[string][]recentEvent{}, sources: map[string][]string{}}
	b.cdf = zipfCDF(len(tenants), opt.TenantSkew)
	return b
}

func zipfCDF(n int, s float64) []float64 {
	w := make([]float64, n)
	var sum float64
	for i := range w {
		w[i] = 1 / math.Pow(float64(i+1), s)
		sum += w[i]
	}
	acc := 0.0
	for i := range w {
		acc += w[i] / sum
		w[i] = acc
	}
	w[n-1] = 1
	return w
}

// PickTenant selects a tenant according to the skew.
func (b *Builder) PickTenant() Tenant {
	u := b.rng.Float64()
	for i, c := range b.cdf {
		if u <= c {
			return b.tenants[i]
		}
	}
	return b.tenants[len(b.tenants)-1]
}

// Batch is a built request body plus bookkeeping.
type Batch struct {
	Tenant  Tenant
	Body    []byte
	Events  int
	Valid   int // events expected accepted (valid, incl. duplicates)
	Invalid int
	Dups    int
}

// BuildBatch builds a batch of n events for one tenant.
func (b *Builder) BuildBatch(n int) Batch {
	t := b.PickTenant()
	out := Batch{Tenant: t, Events: n}
	var sb strings.Builder
	sb.WriteString(`{"events":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		u := b.rng.Float64()
		switch {
		case u < b.opt.InvalidRatio:
			sb.Write(b.invalidEvent(t))
			out.Invalid++
		case u < b.opt.InvalidRatio+b.opt.DupRatio && len(b.recent[t.ID]) > 0:
			r := b.recent[t.ID][b.rng.Intn(len(b.recent[t.ID]))]
			sb.Write(r.raw)
			out.Dups++
			out.Valid++
		default:
			raw, id := b.validEvent(t)
			b.remember(t.ID, recentEvent{id, raw})
			sb.Write(raw)
			out.Valid++
		}
	}
	sb.WriteString(`]}`)
	out.Body = []byte(sb.String())
	return out
}

func (b *Builder) remember(tenant string, e recentEvent) {
	r := b.recent[tenant]
	if len(r) < b.opt.DupWindow {
		b.recent[tenant] = append(r, e)
		return
	}
	r[b.rng.Intn(len(r))] = e
}

func (b *Builder) sourceFor(t Tenant) string {
	s := b.sources[t.ID]
	if s == nil {
		for i := 0; i < b.opt.Sources; i++ {
			s = append(s, uuidFromSeed(t.ID, i))
		}
		b.sources[t.ID] = s
	}
	return s[b.rng.Intn(len(s))]
}

// Envelope builds a valid envelope for tenant t (padding the payload so the
// serialized size is close to EventSize).
func (b *Builder) Envelope(t Tenant) ec.Envelope {
	b.seq++
	now := b.now().UTC()
	et := now
	if b.opt.LateRatio > 0 && b.rng.Float64() < b.opt.LateRatio {
		max := b.opt.LateMax
		if max <= 0 {
			max = 24 * time.Hour
		}
		et = now.Add(-time.Duration(b.rng.Int63n(int64(max))) - time.Minute)
	}
	e := ec.Envelope{
		EventID: newUUID(), TenantID: t.ID, SourceID: b.sourceFor(t),
		SchemaID: "loadgen.event", SchemaVersion: 1, EventTime: &et, IngestedAt: now,
		EntityKey: fmt.Sprintf("ent-%d", b.rng.Intn(10000)),
		TraceID:   fmt.Sprintf("loadgen-%d", b.seq), PayloadFormat: ec.FormatJSON, ProcessingVersion: 1,
	}
	if b.opt.HotKeyRatio > 0 && b.rng.Float64() < b.opt.HotKeyRatio {
		e.EntityKey = "ent-hot"
	}
	e.Payload = b.payload()
	return e
}

func (b *Builder) payload() json.RawMessage {
	if b.opt.Shape == "wide" {
		return b.widePayload()
	}
	const overhead = 420 // envelope fields other than payload, roughly
	pad := b.opt.EventSize - overhead - 40
	if pad < 0 {
		pad = 0
	}
	p, _ := json.Marshal(map[string]any{"seq": b.seq, "pad": randText(b.rng, pad)})
	return p
}

func (b *Builder) validEvent(t Tenant) ([]byte, string) {
	e := b.Envelope(t)
	raw, _ := json.Marshal(e)
	return raw, e.EventID
}

// invalidEvent returns a deliberately broken event (never a tenant mismatch,
// which would reject the whole batch).
func (b *Builder) invalidEvent(t Tenant) []byte {
	e := b.Envelope(t)
	raw, _ := json.Marshal(e)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if b.opt.SchemaInvalid {
		switch b.rng.Intn(3) {
		case 0:
			m["schema_version"] = 0
		case 1:
			m["schema_id"] = ""
		default:
			m["schema_version"] = -1
		}
		out, _ := json.Marshal(m)
		return out
	}
	switch b.rng.Intn(5) {
	case 0:
		m["event_id"] = "not-a-uuid"
	case 1:
		delete(m, "event_time")
	case 2:
		m["schema_version"] = 0
	case 3:
		delete(m, "payload")
	default:
		m["unknown_field_xyz"] = 1 // gateway decodes with DisallowUnknownFields
	}
	out, _ := json.Marshal(m)
	return out
}

func randText(r *mrand.Rand, n int) string {
	const alpha = "abcdefghijklmnopqrstuvwxyz0123456789 "
	bs := make([]byte, n)
	for i := range bs {
		bs[i] = alpha[r.Intn(len(alpha))]
	}
	return string(bs)
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func uuidFromSeed(s string, i int) string {
	h := ec.ChecksumOf([]byte(fmt.Sprintf("%s/%d", s, i)))
	return fmt.Sprintf("%s-%s-4%s-8%s-%s", h[0:8], h[8:12], h[12:15], h[15:18], h[18:30])
}

// widePayload builds a payload with many fields and one nested level, sized
// to approximately EventSize (fields are padded evenly).
func (b *Builder) widePayload() json.RawMessage {
	n := b.opt.WideFields
	if n < 1 {
		n = 120
	}
	const overhead = 420
	budget := b.opt.EventSize - overhead - 40
	per := 8
	if budget > 0 {
		if p := budget/n - 16; p > per {
			per = p
		}
	}
	m := make(map[string]any, n+2)
	m["seq"] = b.seq
	nested := map[string]any{}
	for i := 0; i < n; i++ {
		switch i % 4 {
		case 0:
			m[fmt.Sprintf("f%03d", i)] = b.rng.Int63()
		case 1:
			m[fmt.Sprintf("f%03d", i)] = randText(b.rng, per)
		case 2:
			m[fmt.Sprintf("f%03d", i)] = b.rng.Float64() * 1000
		default:
			nested[fmt.Sprintf("n%03d", i)] = randText(b.rng, per/2+1)
		}
	}
	m["nested"] = nested
	p, _ := json.Marshal(m)
	return p
}
