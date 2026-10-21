package agg

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunStatsTextfile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "chagg.prom")
	s := RunStats{Path: p}
	at := time.Unix(1_800_000_000, 0)
	if err := s.Record(at, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Record(at.Add(time.Hour), errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	for _, want := range []string{`runs_total{result="ok"} 1`, `runs_total{result="error"} 1`, "last_run_ok 0", "last_success_timestamp_seconds 1800000000"} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing %q in\n%s", want, b)
		}
	}
	if m, _ := filepath.Glob(filepath.Join(filepath.Dir(p), ".chagg-*")); len(m) != 0 {
		t.Fatalf("temp files left: %v", m)
	}
}

func TestReconcileStatsTextfile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "rec.prom")
	r := ReconcileStats{Duplicates: 2, SilentChecksum: 1}
	if err := r.Write(p, time.Unix(1_800_000_000, 0), 2); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	for _, want := range []string{`findings{kind="duplicate_rows"} 2`, `findings{kind="aggregate_drift"} 0`, `findings{kind="silent_checksum"} 1`, "last_exit_code 2", "last_run_timestamp_seconds 1800000000"} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing %q in\n%s", want, b)
		}
	}
}
