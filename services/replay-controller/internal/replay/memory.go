package replay

import (
	"context"
	"sync"
	"time"
)

// MemAllocator is an in-memory VersionAllocator for tests/dev.
type MemAllocator struct {
	mu sync.Mutex
	m  map[[2]string]uint32
}

func NewMemAllocator() *MemAllocator { return &MemAllocator{m: map[[2]string]uint32{}} }

// Seed records an existing version (as if already present in ClickHouse).
func (a *MemAllocator) Seed(tenant, source string, v uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if v > a.m[[2]string{tenant, source}] {
		a.m[[2]string{tenant, source}] = v
	}
}

func (a *MemAllocator) Peek(_ context.Context, t, s string) (uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.m[[2]string{t, s}], nil
}

func (a *MemAllocator) Allocate(_ context.Context, t, s string) (uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.m[[2]string{t, s}]++
	return a.m[[2]string{t, s}], nil
}

// MemPublisher records jobs.
type MemPublisher struct {
	mu   sync.Mutex
	Jobs []ReplayJob
	Err  error
}

func (p *MemPublisher) Publish(_ context.Context, j ReplayJob) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Err != nil {
		return p.Err
	}
	p.Jobs = append(p.Jobs, j)
	return nil
}

func (p *MemPublisher) Count() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.Jobs) }

// FixedCounter returns N (and records calls) for tests.
type FixedCounter struct{ N uint64 }

func (c FixedCounter) Count(context.Context, string, string, uint32, time.Time, time.Time, uint64) (uint64, error) {
	return c.N, nil
}
