package sink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeSource struct {
	mu        sync.Mutex
	queue     [][]Message
	commits   [][]Message
	commitErr error
	closed    bool
}

func (f *fakeSource) Poll(ctx context.Context) ([]Message, error) {
	f.mu.Lock()
	if len(f.queue) > 0 {
		b := f.queue[0]
		f.queue = f.queue[1:]
		f.mu.Unlock()
		return b, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return nil, nil
}
func (f *fakeSource) Commit(ctx context.Context, m []Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitErr != nil {
		return f.commitErr
	}
	f.commits = append(f.commits, m)
	return nil
}
func (f *fakeSource) Close() { f.closed = true }
func (f *fakeSource) nCommits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.commits)
}

type fakeSink struct {
	mu     sync.Mutex
	failN  int // fail the first N calls
	calls  []Chunk
	stored map[string]bool // dedup by token like ClickHouse
	rows   int
}

func (f *fakeSink) Insert(ctx context.Context, c Chunk) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
	if f.failN > 0 {
		f.failN--
		return errors.New("boom")
	}
	if f.stored == nil {
		f.stored = map[string]bool{}
	}
	if !f.stored[c.Token] {
		f.stored[c.Token] = true
		f.rows += len(c.Rows)
	}
	return nil
}

type fakeQ struct {
	mu  sync.Mutex
	got []Quarantined
}

func (q *fakeQ) Quarantine(ctx context.Context, x []Quarantined) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.got = append(q.got, x...)
	return nil
}

var fixedNow = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

func good(i int) []byte {
	return []byte(fmt.Sprintf(`{"event_id":"11111111-1111-4111-8111-%012d","tenant_id":"22222222-2222-4222-8222-222222222222",`+
		`"source_id":"33333333-3333-4333-8333-333333333333","schema_id":"s.v1","schema_version":1,`+
		`"event_time":"2026-10-20T11:00:00Z","ingested_at":"2026-10-20T11:00:01Z","trace_id":"t","payload_format":"json",`+
		`"payload":{"a":%d},"processing_version":1}`, i, i))
}

func msgs(part int32, from, n int) []Message {
	var out []Message
	for i := 0; i < n; i++ {
		out = append(out, Message{Topic: "processed.events.v1", Partition: part, Offset: int64(from + i), Value: good(from + i)})
	}
	return out
}

func opts() Options {
	return Options{BatchSize: 3, BatchInterval: 50 * time.Millisecond, RetryBackoff: time.Millisecond,
		RetryBackoffMax: 2 * time.Millisecond, ShutdownTimeout: time.Second, Now: func() time.Time { return fixedNow }}
}

func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func runBG(p *Pipeline) (cancel func(), done chan error) {
	ctx, c := context.WithCancel(context.Background())
	done = make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	return c, done
}

func TestFlushBySize(t *testing.T) {
	src := &fakeSource{queue: [][]Message{msgs(0, 10, 3)}}
	snk := &fakeSink{}
	o := opts()
	o.BatchInterval = time.Hour
	p := NewPipeline(src, snk, &fakeQ{}, o)
	cancel, done := runBG(p)
	waitFor(t, func() bool { return src.nCommits() == 1 })
	cancel()
	<-done
	if len(snk.calls) != 1 || len(snk.calls[0].Rows) != 3 || snk.calls[0].FirstOffset != 10 || snk.calls[0].LastOffset != 12 {
		t.Fatalf("unexpected chunk: %+v", snk.calls)
	}
}

func TestFlushByTime(t *testing.T) {
	src := &fakeSource{queue: [][]Message{msgs(0, 0, 2)}} // below size 3
	snk := &fakeSink{}
	p := NewPipeline(src, snk, &fakeQ{}, opts())
	cancel, done := runBG(p)
	waitFor(t, func() bool { return src.nCommits() == 1 })
	cancel()
	<-done
	if snk.rows != 2 {
		t.Fatalf("rows=%d", snk.rows)
	}
}

func TestRetrySameTokenAndNoCommitUntilSuccess(t *testing.T) {
	src := &fakeSource{queue: [][]Message{msgs(1, 5, 3)}}
	snk := &fakeSink{failN: 3}
	p := NewPipeline(src, snk, &fakeQ{}, opts())
	cancel, done := runBG(p)
	waitFor(t, func() bool { return src.nCommits() == 1 })
	cancel()
	<-done
	if len(snk.calls) != 4 {
		t.Fatalf("calls=%d", len(snk.calls))
	}
	want := Token("processed.events.v1", 1, 5, 7)
	for _, c := range snk.calls {
		if c.Token != want {
			t.Fatalf("token changed on retry: %s != %s", c.Token, want)
		}
	}
	if snk.rows != 3 {
		t.Fatalf("rows=%d (dedup by token expected)", snk.rows)
	}
}

func TestTokenDeterministicAndDistinct(t *testing.T) {
	if Token("t", 0, 1, 2) != Token("t", 0, 1, 2) {
		t.Fatal("not deterministic")
	}
	for _, o := range []string{Token("t", 1, 1, 2), Token("t", 0, 2, 2), Token("t", 0, 1, 3), Token("u", 0, 1, 2)} {
		if o == Token("t", 0, 1, 2) {
			t.Fatal("collision")
		}
	}
}

func TestNoCommitOnFailure(t *testing.T) {
	src := &fakeSource{queue: [][]Message{msgs(0, 0, 3)}}
	snk := &fakeSink{failN: 1000}
	o := opts()
	o.MaxRetries = 2
	p := NewPipeline(src, snk, &fakeQ{}, o)
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "insert failed after 2 retries") {
		t.Fatalf("err=%v", err)
	}
	if src.nCommits() != 0 || len(snk.calls) != 3 {
		t.Fatalf("commits=%d calls=%d", src.nCommits(), len(snk.calls))
	}
}

func TestCommitFailureNotCounted(t *testing.T) {
	src := &fakeSource{queue: [][]Message{msgs(0, 0, 3)}, commitErr: errors.New("rebalance")}
	o := opts()
	o.MaxRetries = 1
	p := NewPipeline(src, &fakeSink{}, &fakeQ{}, o)
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if p.M.Commits.Load() != 0 {
		t.Fatal("commit counted")
	}
}

func TestQuarantineBadRows(t *testing.T) {
	m := msgs(0, 0, 1)
	m = append(m,
		Message{Topic: "processed.events.v1", Partition: 0, Offset: 1, Value: []byte(`not json`)},
		Message{Topic: "processed.events.v1", Partition: 0, Offset: 2, Value: []byte(`{"event_id":"x"}`)})
	src := &fakeSource{queue: [][]Message{m}}
	snk, q := &fakeSink{}, &fakeQ{}
	p := NewPipeline(src, snk, q, opts())
	cancel, done := runBG(p)
	waitFor(t, func() bool { return src.nCommits() == 1 })
	cancel()
	<-done
	if snk.rows != 1 || len(q.got) != 2 {
		t.Fatalf("rows=%d quarantined=%d", snk.rows, len(q.got))
	}
	if q.got[0].Code != "ERR_JSON_MALFORMED" || !strings.HasPrefix(q.got[1].Code, "ERR_") {
		t.Fatalf("codes: %+v", q.got)
	}
	if len(src.commits[0]) != 3 { // bad rows are committed too (they were quarantined)
		t.Fatalf("commit covers %d msgs", len(src.commits[0]))
	}
	if snk.calls[0].LastOffset != 2 { // token covers the whole offset range, incl. bad rows
		t.Fatalf("last=%d", snk.calls[0].LastOffset)
	}
}

func TestMapMessageColumns(t *testing.T) {
	line, code, reason := MapMessage(Message{Topic: "t", Partition: 2, Offset: 9, Value: good(7)}, fixedNow)
	if code != "" {
		t.Fatal(code, reason)
	}
	s := string(line)
	for _, w := range []string{`"processing_version":1`, `"event_time":"2026-10-20 11:00:00.000"`, `"raw_ref":"kafka:t/2/9"`,
		`"event_type":"s.v1"`, `"batch_id":"00000000-0000-0000-0000-000000000000"`, `"payload":"{\"a\":7}"`, `"quality_flags":[]`} {
		if !strings.Contains(s, w) {
			t.Errorf("missing %s in %s", w, s)
		}
	}
	// ULID event ids cannot go to a UUID column
	ul := strings.Replace(string(good(1)), "11111111-1111-4111-8111-000000000001", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1)
	if _, c, _ := MapMessage(Message{Value: []byte(ul)}, fixedNow); c != "ERR_EVENT_ID_NOT_UUID" {
		t.Fatalf("code=%s", c)
	}
}

func TestShutdownFlushesBuffer(t *testing.T) {
	src := &fakeSource{queue: [][]Message{msgs(0, 0, 2)}}
	snk := &fakeSink{}
	o := opts()
	o.BatchInterval = time.Hour
	p := NewPipeline(src, snk, &fakeQ{}, o)
	cancel, done := runBG(p)
	waitFor(t, func() bool { return p.M.Consumed.Load() == 2 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if snk.rows != 2 || src.nCommits() != 1 || !src.closed {
		t.Fatalf("rows=%d commits=%d closed=%v", snk.rows, src.nCommits(), src.closed)
	}
}

func TestShutdownFlushFailureNoCommit(t *testing.T) {
	src := &fakeSource{queue: [][]Message{msgs(0, 0, 2)}}
	snk := &fakeSink{failN: 1000}
	o := opts()
	o.BatchInterval = time.Hour
	o.MaxRetries = 1
	p := NewPipeline(src, snk, &fakeQ{}, o)
	cancel, done := runBG(p)
	waitFor(t, func() bool { return p.M.Consumed.Load() == 2 })
	cancel()
	if err := <-done; err == nil {
		t.Fatal("expected error")
	}
	if src.nCommits() != 0 {
		t.Fatal("committed despite failure")
	}
}

func TestMultiPartitionChunks(t *testing.T) {
	m := append(msgs(0, 0, 2), msgs(1, 100, 2)...)
	src := &fakeSource{queue: [][]Message{m}}
	snk := &fakeSink{}
	o := opts()
	o.BatchSize = 4
	p := NewPipeline(src, snk, &fakeQ{}, o)
	cancel, done := runBG(p)
	waitFor(t, func() bool { return src.nCommits() == 1 })
	cancel()
	<-done
	if len(snk.calls) != 2 || snk.calls[0].Partition != 0 || snk.calls[1].FirstOffset != 100 {
		t.Fatalf("%+v", snk.calls)
	}
}
