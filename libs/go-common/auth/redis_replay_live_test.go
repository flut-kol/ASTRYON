package auth

import (
	"context"
	"os"
	"testing"
	"time"
)

// Needs a real Redis: AUTH_IT_REDIS_URL=redis://127.0.0.1:16399 (throwaway container, see tests/aas-live/README.md).
func TestLiveRedisReplayStore(t *testing.T) {
	url := os.Getenv("AUTH_IT_REDIS_URL")
	if url == "" {
		t.Skip("AUTH_IT_REDIS_URL not set")
	}
	a, err := NewRedisReplayStore(url, "")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewRedisReplayStore(url, "") // second replica
	ctx := context.Background()
	key := "dpop:it:" + time.Now().Format(time.RFC3339Nano)
	if ok, err := a.First(ctx, key, 2*time.Second); err != nil || !ok {
		t.Fatalf("a first: %v %v", ok, err)
	}
	if ok, err := b.First(ctx, key, 2*time.Second); err != nil || ok {
		t.Fatalf("replica b must see the replay: %v %v", ok, err)
	}
	time.Sleep(2200 * time.Millisecond)
	if ok, err := b.First(ctx, key, 2*time.Second); err != nil || !ok {
		t.Fatalf("key must expire after ttl: %v %v", ok, err)
	}
}
