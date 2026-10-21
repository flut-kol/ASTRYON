package retryclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// memStore mimics PGDispatchStore semantics: per-claim token, lease expiry, fenced Complete.
type memStore struct {
	mu    sync.Mutex
	now   time.Time
	seq   int
	items map[string]*memItem
}
type memItem struct {
	m     Message
	token string
	until time.Time
	done  bool
}

func newMem(n int) *memStore {
	s := &memStore{now: time.Unix(1000, 0), items: map[string]*memItem{}}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("job-%d", i)
		s.items[id] = &memItem{m: Message{TenantID: "t1", JobID: id, Kind: "k", Attempt: 1}}
	}
	return s
}
func (s *memStore) advance(d time.Duration) { s.mu.Lock(); s.now = s.now.Add(d); s.mu.Unlock() }
func (s *memStore) Claim(_ context.Context, owner string, limit int, lease time.Duration) ([]Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Claim
	for _, it := range s.items {
		if len(out) >= limit {
			break
		}
		if it.done || (it.token != "" && it.until.After(s.now)) {
			continue
		}
		s.seq++
		it.token, it.until = fmt.Sprintf("%s#%d", owner, s.seq), s.now.Add(lease)
		out = append(out, Claim{Message: it.m, Token: it.token})
	}
	return out, nil
}
func (s *memStore) Complete(_ context.Context, c Claim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.items[c.JobID]
	if it.done || it.token != c.Token || !it.until.After(s.now) {
		return ErrFenced
	}
	it.done, it.token = true, ""
	return nil
}
func (s *memStore) Release(_ context.Context, c Claim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if it := s.items[c.JobID]; it.token == c.Token {
		it.token = ""
	}
	return nil
}

type recPub struct {
	mu   sync.Mutex
	got  map[string]int
	fail error
}

func (p *recPub) Publish(_ context.Context, m Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return p.fail
	}
	p.got[m.JobID]++
	return nil
}

func disp(s DispatchStore, p Publisher, owner string) Dispatcher {
	return Dispatcher{Store: s, Publisher: p, Owner: owner, Batch: 10, Lease: time.Minute}
}

func TestConcurrentClaimDispatchesEachOnce(t *testing.T) {
	s, p := newMem(50), &recPub{got: map[string]int{}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := disp(s, p, fmt.Sprintf("r%d", i))
			d.Batch = 5
			for k := 0; k < 20; k++ {
				if _, err := d.Once(context.Background()); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	for id, n := range p.got {
		if n != 1 {
			t.Fatalf("%s published %d times", id, n)
		}
	}
	if len(p.got) != 50 {
		t.Fatalf("published %d of 50", len(p.got))
	}
}

func TestCrashBetweenClaimAndDispatchRecoversAfterLease(t *testing.T) {
	s, p := newMem(1), &recPub{got: map[string]int{}}
	// Replica A claims and "crashes" (never publishes or completes).
	crashed, _ := s.Claim(context.Background(), "A", 10, time.Minute)
	if len(crashed) != 1 {
		t.Fatal("expected claim")
	}
	b := disp(s, p, "B")
	if n, _ := b.Once(context.Background()); n != 0 {
		t.Fatal("B must not steal a live lease")
	}
	s.advance(2 * time.Minute)
	if n, err := b.Once(context.Background()); n != 1 || err != nil {
		t.Fatalf("B after expiry: n=%d err=%v", n, err)
	}
	// The zombie A waking up is fenced out.
	if err := s.Complete(context.Background(), crashed[0]); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale complete: %v", err)
	}
	if p.got["job-0"] != 1 {
		t.Fatalf("published %d", p.got["job-0"])
	}
}

func TestPublishFailureReleasesForImmediateRetry(t *testing.T) {
	s, p := newMem(1), &recPub{got: map[string]int{}, fail: errors.New("kafka down")}
	d := disp(s, p, "A")
	if _, err := d.Once(context.Background()); err == nil {
		t.Fatal("expected publish error")
	}
	p.fail = nil
	if n, err := d.Once(context.Background()); n != 1 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestCrashAfterPublishDuplicatesAtLeastOnce(t *testing.T) {
	s, p := newMem(1), &recPub{got: map[string]int{}}
	cl, _ := s.Claim(context.Background(), "A", 1, time.Minute)
	_ = p.Publish(context.Background(), cl[0].Message) // published, crashed before Complete
	s.advance(2 * time.Minute)
	disp(s, p, "B").Once(context.Background())
	if p.got["job-0"] != 2 {
		t.Fatalf("expected documented at-least-once duplicate, got %d", p.got["job-0"])
	}
}

type fakeProducer struct {
	recs []*kgo.Record
	err  error
}

func (f *fakeProducer) ProduceSync(_ context.Context, r ...*kgo.Record) error {
	f.recs = append(f.recs, r...)
	return f.err
}
func (f *fakeProducer) Close() {}

func TestKafkaPublisherKeyAndPayload(t *testing.T) {
	fp := &fakeProducer{}
	k := KafkaPublisher{P: fp, Topic: "retries.due"}
	if err := k.Publish(context.Background(), Message{TenantID: "t", JobID: "j", Kind: "k", Attempt: 2}); err != nil {
		t.Fatal(err)
	}
	r := fp.recs[0]
	if r.Topic != "retries.due" || string(r.Key) != "t/j" || string(r.Headers[0].Value) != "2" {
		t.Fatalf("%+v", r)
	}
	fp.err = errors.New("nack")
	if k.Publish(context.Background(), Message{}) == nil {
		t.Fatal("want error")
	}
}

func TestClientConfigValidation(t *testing.T) {
	pol := Policy{BaseDelay: time.Second, MaxDelay: time.Minute, MaxAttempts: 3}
	cases := []Config{{TenantID: "bad", WorkerID: "w", Policy: pol}, {TenantID: "11111111-1111-1111-1111-111111111111", Policy: pol}, {TenantID: "11111111-1111-1111-1111-111111111111", WorkerID: "w"}}
	for i, c := range cases {
		if _, err := New(nil, c); err == nil {
			t.Fatalf("case %d nil db accepted", i)
		}
	}
	if Classify(Transientf(errors.New("x"))) != Transient || Classify(errors.New("x")) != Permanent {
		t.Fatal("classification")
	}
}

func Classify(err error) Class {
	var c interface{ RetryClass() Class }
	if errors.As(err, &c) {
		return c.RetryClass()
	}
	return Permanent
}
