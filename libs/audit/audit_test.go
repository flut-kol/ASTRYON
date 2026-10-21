package audit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

const tid = "11111111-1111-4111-8111-111111111111"

func claims() Claims { return Claims{TenantID: tid, Subject: "user-1", ACR: "urn:astryon:acr:mfa"} }

func ev() Event {
	return NewEvent(claims(), CategoryAudit, "source.create", OutcomeSuccess, Resource{Type: "source", ID: "22222222-2222-4222-8222-222222222222"})
}

func TestNormalize(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("x", 3600))
	e, err := ev().Normalize(now)
	if err != nil || e.ID == "" || e.OccurredAt.Location() != time.UTC {
		t.Fatalf("%v %+v", err, e)
	}
	cases := map[string]func(*Event){
		"tenant":   func(e *Event) { e.TenantID = "" },
		"tenant2":  func(e *Event) { e.TenantID = "acme" },
		"category": func(e *Event) { e.Category = "x" },
		"outcome":  func(e *Event) { e.Outcome = "" },
		"action":   func(e *Event) { e.Action = "Create" },
		"actor":    func(e *Event) { e.Subject, e.ClientID = "", "" },
		"resource": func(e *Event) { e.Resource.Type = "" },
		"secret":   func(e *Event) { e.Details = map[string]string{"api_key": "x"} },
		"payload":  func(e *Event) { e.Details = map[string]string{"payload": "x"} },
		"bigval":   func(e *Event) { e.Details = map[string]string{"k": strings.Repeat("a", 300)} },
		"id":       func(e *Event) { e.ID = "nope" },
	}
	for n, mut := range cases {
		x := ev()
		mut(&x)
		if _, err := x.Normalize(now); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: want ErrInvalid, got %v", n, err)
		}
	}
	many := map[string]string{}
	for i := 0; i < MaxDetailKeys+1; i++ {
		many[strings.Repeat("k", i+1)] = "v"
	}
	if _, err := ev().WithDetails(many).Normalize(now); err == nil {
		t.Error("too many keys accepted")
	}
}

func TestResourceSplit(t *testing.T) {
	if id, ref := (Resource{ID: "a/b"}).split(); id != nil || *ref != "a/b" {
		t.Fatal("ref")
	}
	if id, ref := (Resource{ID: tid}).split(); ref != nil || *id != tid {
		t.Fatal("uuid")
	}
}

func TestWriterBatchesPerTenantAndDrains(t *testing.T) {
	var mu sync.Mutex
	got := map[string]int{}
	w := newWriter(func(_ context.Context, tenant string, evs []Event) error {
		mu.Lock()
		got[tenant] += len(evs)
		mu.Unlock()
		for _, e := range evs {
			if e.TenantID != tenant {
				t.Errorf("mixed tenants in batch")
			}
		}
		return nil
	}, Config{FlushInterval: time.Hour})
	other := ev()
	other.TenantID = "33333333-3333-4333-8333-333333333333"
	for i := 0; i < 5; i++ {
		if err := w.Submit(ev()); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Submit(other); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got[tid] != 5 || got[other.TenantID] != 1 || w.Stats().Written != 6 {
		t.Fatalf("%v %+v", got, w.Stats())
	}
	if err := w.Submit(ev()); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestWriterQueueFullNeverBlocks(t *testing.T) {
	block := make(chan struct{})
	var dropped int
	w := newWriter(func(context.Context, string, []Event) error { <-block; return nil },
		Config{QueueSize: 2, BatchSize: 1, OnDrop: func(Event, error) { dropped++ }})
	full := 0
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			if errors.Is(w.Submit(ev()), ErrQueueFull) {
				full++
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Submit blocked")
	}
	if full == 0 || dropped != full {
		t.Fatalf("full=%d dropped=%d", full, dropped)
	}
	close(block)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = w.Close(ctx)
}

func TestWriterRetriesThenDrops(t *testing.T) {
	calls := 0
	var reason error
	w := newWriter(func(context.Context, string, []Event) error { calls++; return errors.New("pg down") },
		Config{Retries: 1, OnDrop: func(_ Event, err error) { reason = err }})
	_ = w.Submit(ev())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = w.Close(ctx)
	if calls != 2 || reason == nil || w.Stats().Dropped != 1 {
		t.Fatalf("calls=%d reason=%v %+v", calls, reason, w.Stats())
	}
}

func TestWriteSyncAndInvalid(t *testing.T) {
	w := newWriter(func(context.Context, string, []Event) error { return nil }, Config{})
	if err := w.WriteSync(context.Background(), ev()); err != nil {
		t.Fatal(err)
	}
	bad := ev()
	bad.TenantID = ""
	if err := w.Submit(bad); !errors.Is(err, ErrInvalid) || w.Stats().Rejected != 1 {
		t.Fatal(err)
	}
	_ = w.Close(context.Background())
}
