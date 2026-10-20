package processor

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeSrc struct {
	batches    [][]Input
	committed  [][]Input
	failCommit int
	cancel     context.CancelFunc
}

func (f *fakeSrc) Poll(ctx context.Context) ([]Input, error) {
	if len(f.batches) == 0 {
		f.cancel()
		return nil, nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, nil
}
func (f *fakeSrc) Commit(_ context.Context, ins []Input) error {
	if f.failCommit > 0 {
		f.failCommit--
		return errors.New("commit boom")
	}
	f.committed = append(f.committed, ins)
	return nil
}
func (f *fakeSrc) Close() {}

type fakeProd struct {
	fail             int
	got              []Record
	src              *fakeSrc
	commitsAtProduce int
}

func (f *fakeProd) Produce(_ context.Context, r []Record) error {
	if f.fail > 0 {
		f.fail--
		return errors.New("produce boom")
	}
	f.commitsAtProduce = len(f.src.committed)
	f.got = append(f.got, r...)
	return nil
}

func newTestPipeline(t *testing.T, batches [][]Input, prodFail int) (*Pipeline, *fakeSrc, *fakeProd, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	src := &fakeSrc{batches: batches, cancel: cancel}
	prod := &fakeProd{fail: prodFail, src: src}
	w := NewWindow(time.Hour, 1000)
	p := NewPipeline(src, prod, w, Options{Normalizer: nz, OutputTopic: "processed.events.v1", QuarantineTopic: "events.quarantine.v1",
		DLQTopic: "events.dlq.v1", BatchInterval: time.Millisecond, RetryBackoff: time.Millisecond, RetryBackoffMax: time.Millisecond})
	p.Now = func() time.Time { return now }
	return p, src, prod, ctx
}

func TestPipelineRouting(t *testing.T) {
	a := in(evJSON(t, id1, nil))
	a.Offset = 1
	dup := a
	dup.Offset = 2
	bad := in([]byte("junk"))
	bad.Offset = 3
	p, src, prod, ctx := newTestPipeline(t, [][]Input{{a, dup, bad}}, 0)
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, r := range prod.got {
		counts[r.Topic]++
	}
	if counts["processed.events.v1"] != 1 || counts["events.quarantine.v1"] != 1 || counts["events.dlq.v1"] != 0 {
		t.Fatalf("%v", counts)
	}
	if len(src.committed) != 1 || len(src.committed[0]) != 3 {
		t.Fatal("whole batch (incl. dropped dup) must be committed")
	}
	if prod.commitsAtProduce != 0 {
		t.Fatal("commit must follow produce")
	}
	if p.M.Duplicates.Load() != 1 || p.M.Quarantined.Load() != 1 || p.Win.Stats().Entries != 1 {
		t.Fatal("metrics/state")
	}
}

// A failed produce must not poison the window: the retry still emits the event.
func TestPipelineProduceRetryDoesNotLoseEvent(t *testing.T) {
	a := in(evJSON(t, id1, nil))
	p, src, prod, ctx := newTestPipeline(t, [][]Input{{a}}, 2)
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(prod.got) != 1 || len(src.committed) != 1 || p.M.Retries.Load() != 2 {
		t.Fatalf("got=%d committed=%d retries=%d", len(prod.got), len(src.committed), p.M.Retries.Load())
	}
}

func TestPipelineCommitRetryAndMaxRetries(t *testing.T) {
	a := in(evJSON(t, id1, nil))
	p, src, _, ctx := newTestPipeline(t, [][]Input{{a}}, 0)
	src.failCommit = 1
	if err := p.Run(ctx); err != nil || len(src.committed) != 1 {
		t.Fatalf("err=%v committed=%d", err, len(src.committed))
	}
	p, src, prod, ctx := newTestPipeline(t, [][]Input{{a}}, 100)
	p.Opt.MaxRetries = 3
	if err := p.Run(ctx); err == nil {
		t.Fatal("expected fatal error")
	}
	if len(src.committed) != 0 || len(prod.got) != 0 {
		t.Fatal("must not commit on failed produce")
	}
}
