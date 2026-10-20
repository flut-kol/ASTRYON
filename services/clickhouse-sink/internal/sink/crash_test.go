package sink

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// recSink emulates ClickHouse block dedup (same token => dropped) and records every stored row.
type recSink struct {
	mu     sync.Mutex
	tokens map[string]bool
	ids    []string
}

func (s *recSink) Insert(_ context.Context, c Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tokens == nil {
		s.tokens = map[string]bool{}
	}
	if s.tokens[c.Token] {
		return nil
	}
	s.tokens[c.Token] = true
	for _, r := range c.Rows {
		var row struct {
			EventID string `json:"event_id"`
		}
		_ = json.Unmarshal(r, &row)
		s.ids = append(s.ids, row.EventID)
	}
	return nil
}

func (s *recSink) unique() int {
	u := map[string]bool{}
	for _, id := range s.ids {
		u[id] = true
	}
	return len(u)
}

// crashAfterInsert runs one pipeline over first, whose commit fails after a successful insert (a crash in the
// insert->commit window), then a restarted pipeline that is redelivered second from the committed offset.
func crashAfterInsert(t *testing.T, first, second []Message, in IntentStore) *recSink {
	t.Helper()
	snk := &recSink{}
	src1 := &fakeSource{queue: [][]Message{first}, commitErr: errors.New("crash before commit")}
	p1 := NewPipeline(src1, snk, &fakeQ{}, Options{BatchSize: len(first), MaxRetries: 1, RetryBackoff: time.Millisecond, Intents: in, Now: func() time.Time { return fixedNow }})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p1.Run(ctx); err == nil {
		t.Fatal("first run should fail at commit")
	}
	src2 := &fakeSource{queue: [][]Message{second}}
	p2 := NewPipeline(src2, snk, &fakeQ{}, Options{BatchSize: len(second), RetryBackoff: time.Millisecond, Intents: in, IntentWait: 200 * time.Millisecond, Now: func() time.Time { return fixedNow }})
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		for src2.nCommits() == 0 {
			time.Sleep(time.Millisecond)
		}
		cancel2()
	}()
	if err := p2.Run(ctx2); err != nil {
		t.Fatal(err)
	}
	return snk
}

// Same redelivered range => same token => ClickHouse drops the block: no duplicates.
func TestCrashBetweenInsertAndCommit_SameBoundaries(t *testing.T) {
	snk := crashAfterInsert(t, msgs(0, 0, 6), msgs(0, 0, 6), nil)
	if len(snk.ids) != 6 || snk.unique() != 6 {
		t.Fatalf("rows=%d unique=%d, want 6/6", len(snk.ids), snk.unique())
	}
}

// WITHOUT durable boundaries (Options.Intents == nil) -- the default. KNOWN LIMITATION: after a crash the restart re-cuts chunks from the committed offset, so
// a different batch size gives a different [first,last] range and a different token. ClickHouse then stores the
// overlap twice. No row is lost; readers and aggregates stay exact via LIMIT 1 BY (tenant, source, event_id,
// processing_version) and `ch-aggregator reconcile` reports the drift. A real fix needs durable chunk boundaries
// (IntentStore, migration 0004), tested below.
func TestCrashBetweenInsertAndCommit_DifferentBoundariesDuplicateRows(t *testing.T) {
	snk := crashAfterInsert(t, msgs(0, 0, 6), msgs(0, 0, 10), nil)
	if snk.unique() != 10 {
		t.Fatalf("lost rows: unique=%d, want 10", snk.unique())
	}
	if len(snk.ids) != 16 {
		t.Fatalf("stored rows=%d; known limitation expects 16 (6 + 10 overlapping). If this is now 10 the limitation was fixed: update docs", len(snk.ids))
	}
}

type memIntents struct {
	mu   sync.Mutex
	last map[partKey]Range
}

func (m *memIntents) Record(_ context.Context, topic string, part int32, r Range) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.last == nil {
		m.last = map[partKey]Range{}
	}
	m.last[partKey{topic, part}] = r
	return nil
}

func (m *memIntents) Latest(_ context.Context, topic string, part int32) (Range, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.last[partKey{topic, part}]
	return r, ok, nil
}

// WITH durable boundaries the restart re-cuts [0,5] exactly, so the token matches and no row is stored twice,
// while the remaining offsets 6..9 are inserted as a new chunk.
func TestCrashBetweenInsertAndCommit_DurableBoundariesNoDuplicates(t *testing.T) {
	snk := crashAfterInsert(t, msgs(0, 0, 6), msgs(0, 0, 10), &memIntents{})
	if snk.unique() != 10 || len(snk.ids) != 10 {
		t.Fatalf("rows=%d unique=%d, want 10/10", len(snk.ids), snk.unique())
	}
}

// If the redelivery stops short of the recorded range the flush is held, then the range is abandoned after
// IntentWait and the rows are still written (never lost, never blocked forever).
func TestDurableBoundariesHoldThenAbandon(t *testing.T) {
	snk := crashAfterInsert(t, msgs(0, 0, 6), msgs(0, 0, 3), &memIntents{})
	if snk.unique() != 6 {
		t.Fatalf("unique=%d, want 6", snk.unique())
	}
}
