package replay

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestReconcileTxWindowEnv(t *testing.T) {
	get := func(v string) func(string) string {
		return func(k string) string {
			if k == "REPLAY_RECONCILE_TX_WINDOW" {
				return v
			}
			return ""
		}
	}
	for _, c := range []struct {
		in   string
		want time.Duration
	}{{"", DefaultReconcileTxWindow}, {"5m", 5 * time.Minute}, {"90s", 90 * time.Second},
		{"-1m", DefaultReconcileTxWindow}, {"0", DefaultReconcileTxWindow}, {"abc", DefaultReconcileTxWindow}} {
		if got := envDuration(get(c.in), "REPLAY_RECONCILE_TX_WINDOW", DefaultReconcileTxWindow); got != c.want {
			t.Errorf("%q -> %v, want %v", c.in, got, c.want)
		}
	}
}

func TestReconcileCLIRejectsNonPositiveTxWindow(t *testing.T) {
	var out, errb bytes.Buffer
	if code := RunReconcileCLI([]string{"-tx-window=0s"}, func(string) string { return "" }, &out, &errb); code != 2 || !strings.Contains(errb.String(), "tx-window") {
		t.Fatalf("code %d %q", code, errb.String())
	}
}

// A longer window turns a long-held transaction (switched_at far before the event stamp) from aborted into committed.
func TestJudgeActivationHonoursTxWindow(t *testing.T) {
	ev := AuditEvent{Action: ActionActivate, ToVersion: ptr32(2), Subject: "alice"}
	ref := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	by := "alice"
	f := ActiveFact{Found: true, Active: 2, SwitchedBy: &by, SwitchedAt: ref.Add(-10 * time.Minute)}
	upper := ref.Add(time.Hour)
	short := judgeActivation(ev, f, ref, time.Minute, upper)
	long := judgeActivation(ev, f, ref, 15*time.Minute, upper)
	if short == long {
		t.Fatalf("window had no effect: %v vs %v", short, long)
	}
}
