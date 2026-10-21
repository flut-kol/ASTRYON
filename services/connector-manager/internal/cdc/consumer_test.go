package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
)

const (
	tenant = "11111111-1111-4111-8111-111111111111"
	source = "22222222-2222-4222-8222-222222222222"
)

var now0 = time.UnixMilli(1_700_000_100_000)

func dbz(op string, lsn int, id int, extra string) []byte {
	img := fmt.Sprintf(`{"id":%d,"name":"n%d"%s}`, id, id, extra)
	before, after := "null", img
	if op == "d" {
		before, after = img, "null"
	}
	return []byte(fmt.Sprintf(`{"before":%s,"after":%s,"source":{"connector":"postgresql","schema":"public","table":"people","ts_ms":1700000000000,"lsn":%d,"txId":7},"op":%q,"ts_ms":1700000000500}`, before, after, lsn, op))
}

type fakeFeed struct {
	queue      []Record
	committed  int // index of next uncommitted record
	failCommit bool
}

func (f *fakeFeed) Poll(_ context.Context, max int) ([]Record, error) {
	end := f.committed + max
	if end > len(f.queue) {
		end = len(f.queue)
	}
	return append([]Record(nil), f.queue[f.committed:end]...), nil
}
func (f *fakeFeed) Commit(_ context.Context, r []Record) error {
	if f.failCommit {
		return errors.New("commit boom")
	}
	f.committed += len(r)
	return nil
}

type fakeEmit struct {
	got  []ec.Envelope
	fail bool
	seen map[string]bool
}

func (e *fakeEmit) Emit(_ context.Context, tenant, source string, evs [][]byte) error {
	if e.fail {
		return errors.New("emit boom")
	}
	for _, b := range evs {
		var env ec.Envelope
		if err := json.Unmarshal(b, &env); err != nil {
			return err
		}
		if e.seen == nil {
			e.seen = map[string]bool{}
		}
		e.seen[env.EventID] = true
		e.got = append(e.got, env)
	}
	return nil
}

type fakeQ struct{ got []ec.QuarantineRecordV1 }

func (q *fakeQ) Quarantine(_ context.Context, r ec.QuarantineRecordV1) error {
	q.got = append(q.got, r)
	return nil
}

type fakePos struct{ pos string }

func (p *fakePos) SavePosition(_ context.Context, _, pos string) error { p.pos = pos; return nil }

func newC(f *fakeFeed, e *fakeEmit, q *fakeQ) *Consumer {
	return &Consumer{Feed: f, Emitter: e, Quarantine: q, Now: func() time.Time { return now0 }, MaxBatch: 3,
		Map: MapConfig{ConnectorID: "c1", Tenant: tenant, Source: source, SchemaID: "cdc.people", SchemaVersion: 1, ProcessingVersion: 1, PrimaryKey: []string{"id"}}}
}

func rec(off int64, v []byte) Record {
	return Record{Topic: "pg.public.people", Offset: off, Key: []byte(`{"id":1}`), Value: v}
}

func TestMapsOpsAndIgnoresPayloadIdentity(t *testing.T) {
	f := &fakeFeed{queue: []Record{
		rec(0, dbz("c", 10, 1, `,"tenant_id":"evil","source_id":"evil"`)),
		rec(1, dbz("u", 11, 1, "")),
		rec(2, dbz("d", 12, 1, "")),
	}}
	e := &fakeEmit{}
	c := newC(f, e, &fakeQ{})
	if n, err := c.RunOnce(context.Background()); err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if len(e.got) != 3 || f.committed != 3 {
		t.Fatalf("emitted=%d committed=%d", len(e.got), f.committed)
	}
	for _, env := range e.got {
		if env.TenantID != tenant || env.SourceID != source || env.EntityKey != "1" {
			t.Fatalf("bad identity: %+v", env)
		}
		if err := env.ValidateAt(now0); err != nil {
			t.Fatal(err)
		}
	}
	del := string(e.got[2].Payload)
	if !strings.Contains(del, `"deleted":true`) || strings.Contains(del, "n1") || strings.Contains(del, `"after"`) {
		t.Fatalf("delete must be tombstone without row image: %s", del)
	}
	ids := map[string]bool{}
	for _, env := range e.got {
		ids[env.EventID] = true
	}
	if len(ids) != 3 {
		t.Fatal("distinct changes must have distinct event ids")
	}
}

func TestRedeliveryIsIdempotent(t *testing.T) {
	f := &fakeFeed{queue: []Record{rec(0, dbz("c", 10, 1, "")), rec(1, dbz("u", 11, 1, ""))}}
	e := &fakeEmit{}
	c := newC(f, e, &fakeQ{})
	if _, err := c.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.committed = 0 // consumer group rewound / redelivered
	if _, err := c.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(e.got) != 4 || len(e.seen) != 2 {
		t.Fatalf("emits=%d distinct ids=%d (want 4/2)", len(e.got), len(e.seen))
	}
}

func TestCrashBetweenEmitAndCommit(t *testing.T) {
	f := &fakeFeed{queue: []Record{rec(0, dbz("c", 10, 1, "")), rec(1, dbz("c", 11, 2, ""))}, failCommit: true}
	e := &fakeEmit{}
	pos := &fakePos{}
	c := newC(f, e, &fakeQ{})
	c.Positions = pos
	if _, err := c.RunOnce(context.Background()); err == nil {
		t.Fatal("commit failure must surface")
	}
	if f.committed != 0 || len(e.got) != 2 {
		t.Fatalf("committed=%d emitted=%d", f.committed, len(e.got))
	}
	f.failCommit = false
	if _, err := c.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.committed != 2 || len(e.seen) != 2 {
		t.Fatalf("committed=%d distinct=%d", f.committed, len(e.seen))
	}
	if !strings.Contains(pos.pos, "lsn=11") {
		t.Fatalf("position=%q", pos.pos)
	}
}

func TestEmitFailureDoesNotCommit(t *testing.T) {
	f := &fakeFeed{queue: []Record{rec(0, dbz("c", 10, 1, ""))}}
	c := newC(f, &fakeEmit{fail: true}, &fakeQ{})
	if _, err := c.RunOnce(context.Background()); err == nil || f.committed != 0 {
		t.Fatalf("err=%v committed=%d", err, f.committed)
	}
}

func TestMalformedQuarantinedNotCrash(t *testing.T) {
	f := &fakeFeed{queue: []Record{
		rec(0, []byte(`not json`)),
		rec(1, dbz("c", 10, 1, "")),
		rec(2, []byte(`{"op":"x","source":{"table":"t"}}`)),
		{Topic: "pg.public.people", Offset: 3, Value: nil}, // Debezium tombstone
	}}
	e, q := &fakeEmit{}, &fakeQ{}
	c := newC(f, e, q)
	c.MaxBatch = 10
	if _, err := c.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(e.got) != 1 || len(q.got) != 2 || f.committed != 4 || c.M.Tombstones.Load() != 1 {
		t.Fatalf("emitted=%d quarantined=%d committed=%d", len(e.got), len(q.got), f.committed)
	}
	for _, r := range q.got {
		if err := r.Validate(); err != nil {
			t.Fatalf("invalid quarantine record: %v", err)
		}
		if r.TenantID != tenant || *r.Provenance.SourceOffset > 2 {
			t.Fatalf("%+v", r)
		}
	}
}

func TestBoundedBatches(t *testing.T) {
	var qs []Record
	for i := 0; i < 7; i++ {
		qs = append(qs, rec(int64(i), dbz("c", 10+i, i+1, "")))
	}
	f := &fakeFeed{queue: qs}
	e := &fakeEmit{}
	c := newC(f, e, &fakeQ{})
	n, _ := c.RunOnce(context.Background())
	if n != 3 || len(e.got) != 3 {
		t.Fatalf("first batch n=%d", n)
	}
}

func TestKeyFromRecordAndMissingPK(t *testing.T) {
	c := newC(&fakeFeed{}, &fakeEmit{}, &fakeQ{})
	c.Map.PrimaryKey = nil
	ch, _ := ParseChange(dbz("c", 1, 5, ""))
	b, err := Map(ch, []byte(`{"schema":{},"payload":{"id":5}}`), c.Map, now0)
	if err != nil {
		t.Fatal(err)
	}
	var env ec.Envelope
	_ = json.Unmarshal(b, &env)
	if env.EntityKey != "5" {
		t.Fatalf("entity=%q", env.EntityKey)
	}
	if _, err := Map(ch, nil, c.Map, now0); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err=%v", err)
	}
}

func TestSnapshotReadIDStable(t *testing.T) {
	v := []byte(`{"before":null,"after":{"id":1},"source":{"table":"people","ts_ms":1700000000000,"snapshot":"true"},"op":"r","ts_ms":1700000001000}`)
	ch, err := ParseChange(v)
	if err != nil {
		t.Fatal(err)
	}
	a, b := EventID("c1", ch, "1"), EventID("c1", ch, "1")
	if a != b || !ec.IsUUID(a) {
		t.Fatal(a, b)
	}
}

func TestBadConfigFailsLoudly(t *testing.T) {
	f := &fakeFeed{queue: []Record{rec(0, dbz("c", 10, 1, ""))}}
	c := newC(f, &fakeEmit{}, &fakeQ{})
	c.Map.Tenant = "not-a-uuid"
	if _, err := c.RunOnce(context.Background()); err == nil || f.committed != 0 {
		t.Fatalf("err=%v committed=%d", err, f.committed)
	}
}
