package sink

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := ln.Addr().String()
	ln.Close()
	return a
}

// silentAddr accepts connections and never answers (stands in for a broker that drops a failed
// mTLS/ACL peer without a diagnosable connect error).
func silentAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
		}
	}()
	return ln.Addr().String()
}

func quarantineOnce(t *testing.T, c Config, ctx context.Context) (time.Duration, error) {
	t.Helper()
	if c.Brokers == nil {
		c.Brokers = []string{deadAddr(t)}
	}
	c.Topic, c.GroupID, c.BatchSize, c.QuarantineTopic = "t", "g", 1, "q"
	k, err := NewKafka(c)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	st := time.Now()
	err = k.Quarantine(ctx, []Quarantined{{Code: "x", Reason: "r", Msg: Message{Topic: "t", Value: []byte(`{}`)}}})
	return time.Since(st), err
}

func TestQuarantine_UnreachableBrokerBoundedByTimeout(t *testing.T) {
	d, err := quarantineOnce(t, Config{QuarantineTimeout: 1100 * time.Millisecond}, context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if d > 3*time.Second {
		t.Fatalf("blocked %s", d)
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "not acked within 1.1s") {
		t.Fatalf("unclear error: %v", err)
	}
}

func TestQuarantine_BoundedByCallerCtx(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	d, err := quarantineOnce(t, Config{QuarantineTimeout: time.Minute}, ctx)
	if err == nil || d > 3*time.Second {
		t.Fatalf("d=%s err=%v", d, err)
	}
}

func TestQuarantine_TLSTimeoutHint(t *testing.T) {
	_, err := quarantineOnce(t, Config{QuarantineTimeout: 1100 * time.Millisecond, KafkaTLS: true, Brokers: []string{silentAddr(t)}}, context.Background())
	if err == nil || !strings.Contains(err.Error(), "TLS/mTLS") {
		t.Fatalf("missing TLS hint: %v", err)
	}
	_, err = quarantineOnce(t, Config{QuarantineTimeout: 1100 * time.Millisecond, Brokers: []string{silentAddr(t)}}, context.Background())
	if err == nil || strings.Contains(err.Error(), "TLS/mTLS") {
		t.Fatalf("unexpected TLS hint without TLS: %v", err)
	}
}

func TestLoadConfig_QuarantineTimeout(t *testing.T) {
	setEnv(t, "CHSINK_QUARANTINE_TIMEOUT", "3s")
	c, err := LoadConfig()
	if err != nil || c.QuarantineTimeout != 3*time.Second {
		t.Fatalf("%v %v", c.QuarantineTimeout, err)
	}
	setEnv(t, "CHSINK_QUARANTINE_TIMEOUT", "0s")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("zero timeout accepted")
	}
}
