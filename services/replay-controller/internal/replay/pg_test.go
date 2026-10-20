package replay

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDB emulates the advisory lock + replay_requests table with a mutex held for the tx duration.
type fakeDB struct {
	mu     sync.Mutex // stands in for pg_advisory_xact_lock (coarser: global)
	rows   []fakeRow
	status map[string]string
	n      int
}
type fakeRow struct {
	tenant, source string
	v              int32
	id             string
}
type fakeTx struct {
	db     *fakeDB
	tenant string
	locked bool
	log    []string
	done   bool
}
type fakeScan struct{ f func(dest ...any) error }

func (s fakeScan) Scan(d ...any) error { return s.f(d...) }

func (d *fakeDB) Begin(context.Context) (Tx, error) { return &fakeTx{db: d}, nil }
func (t *fakeTx) Exec(_ context.Context, sql string, a ...any) error {
	t.log = append(t.log, sql)
	switch {
	case sql == sqlSetTenant:
		t.tenant = a[0].(string)
	case sql == sqlLock:
		t.db.mu.Lock()
		t.locked = true
	case sql == sqlStatus:
		t.db.mu.Lock()
		defer t.db.mu.Unlock()
		if t.db.status == nil {
			t.db.status = map[string]string{}
		}
		t.db.status[a[0].(string)] = a[1].(string)
	}
	return nil
}
func (t *fakeTx) QueryRow(_ context.Context, sql string, a ...any) Row {
	return fakeScan{func(dest ...any) error {
		switch sql {
		case sqlMax:
			var m int32
			for _, r := range t.db.rows {
				if r.tenant == t.tenant && r.tenant == a[0].(string) && r.source == a[1].(string) && r.v > m {
					m = r.v
				}
			}
			*dest[0].(*int32) = m
		case sqlIns:
			t.db.n++
			id := "id-" + string(rune('a'+t.db.n%26))
			t.db.rows = append(t.db.rows, fakeRow{a[0].(string), a[1].(string), a[2].(int32), id})
			*dest[0].(*string) = id
		}
		return nil
	}}
}
func (t *fakeTx) end() {
	if t.locked {
		t.locked = false
		t.db.mu.Unlock()
	}
	t.done = true
}
func (t *fakeTx) Commit(context.Context) error { t.end(); return nil }
func (t *fakeTx) Rollback(context.Context) error {
	if !t.done {
		t.end()
	}
	return nil
}

type fixedPeek uint32

func (f fixedPeek) Peek(context.Context, string, string) (uint32, error)     { return uint32(f), nil }
func (f fixedPeek) Allocate(context.Context, string, string) (uint32, error) { panic("not used") }

func TestPGAllocatorUsesGreatestOfPGAndCH(t *testing.T) {
	db := &fakeDB{}
	a := &PGAllocator{DB: db, CH: fixedPeek(7)}
	ctx := context.Background()
	for want := uint32(8); want < 11; want++ {
		if v, err := a.Allocate(ctx, tenantA, srcX); err != nil || v != want {
			t.Fatalf("got %d,%v want %d", v, err, want)
		}
	}
	// PG max (10) now beats CH max (7).
	a2 := &PGAllocator{DB: db, CH: fixedPeek(2)}
	if v, _ := a2.Allocate(ctx, tenantA, srcX); v != 11 {
		t.Fatal(v)
	}
	if v, _ := a2.Peek(ctx, tenantA, srcX); v != 11 {
		t.Fatalf("peek %d", v)
	}
	if v, _ := a2.Peek(ctx, tenantB, srcX); v != 2 {
		t.Fatalf("other tenant sees our rows: %d", v)
	}
}

func TestPGAllocatorTxOrderAndTenantValidation(t *testing.T) {
	db := &fakeDB{}
	a := &PGAllocator{DB: db}
	if _, err := a.Allocate(context.Background(), "not-a-uuid", srcX); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := a.Allocate(context.Background(), tenantA, "x"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if len(db.rows) != 0 {
		t.Fatal("row written for invalid input")
	}
	if !strings.Contains(sqlLock, "pg_advisory_xact_lock(hashtextextended(") || !strings.Contains(sqlSetTenant, "true)") {
		t.Fatal("lock/tenant must be transaction scoped")
	}
}

func TestPGAllocatorConcurrentDistinct(t *testing.T) {
	db := &fakeDB{}
	a, b := &PGAllocator{DB: db}, &PGAllocator{DB: db}
	var wg sync.WaitGroup
	res := make(chan uint32, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		al := a
		if i%2 == 1 {
			al = b
		}
		go func() {
			defer wg.Done()
			v, err := al.Allocate(context.Background(), tenantA, srcX)
			if err != nil {
				t.Error(err)
			}
			res <- v
		}()
	}
	wg.Wait()
	close(res)
	seen := map[uint32]bool{}
	for v := range res {
		if seen[v] {
			t.Fatalf("dup %d", v)
		}
		seen[v] = true
	}
}

func TestPlannerMarksFailedOnPublishError(t *testing.T) {
	db := &fakeDB{}
	pub := &MemPublisher{Err: errors.New("kafka down")}
	pl := &Planner{Alloc: &PGAllocator{DB: db}, Counter: FixedCounter{}, Publisher: pub}
	_, err := pl.Plan(context.Background(), tenantA, Request{SourceID: srcX, From: time.Unix(0, 0), To: time.Unix(3600, 0), PipelineVersion: 1})
	if err == nil {
		t.Fatal("expected error")
	}
	if len(db.rows) != 1 || db.status[db.rows[0].id] != StatusFailed {
		t.Fatalf("rows=%v status=%v", db.rows, db.status)
	}
	pub.Err = nil
	if _, err := pl.Plan(context.Background(), tenantA, Request{SourceID: srcX, From: time.Unix(0, 0), To: time.Unix(3600, 0), PipelineVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if pub.Jobs[0].ProcessingVersion != 2 {
		t.Fatal("version reused")
	}
}
