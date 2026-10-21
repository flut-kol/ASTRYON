package agg

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// RunStats is written as a Prometheus textfile-collector file after every recompute, so a run-once
// (cron) deployment is observable without a long-lived /metrics listener.
type RunStats struct {
	Path        string // empty = disabled
	OK, Failed  int64
	LastSuccess time.Time
	LastOK      bool
}

// Record notes one recompute outcome and rewrites the file atomically (temp file + rename).
func (s *RunStats) Record(now time.Time, err error) error {
	if err == nil {
		s.OK++
		s.LastSuccess = now
		s.LastOK = true
	} else {
		s.Failed++
		s.LastOK = false
	}
	if s.Path == "" {
		return nil
	}
	ok := 0
	if s.LastOK {
		ok = 1
	}
	body := fmt.Sprintf("# HELP astryon_ch_aggregator_runs_total Recompute runs by result\n# TYPE astryon_ch_aggregator_runs_total counter\n"+
		"astryon_ch_aggregator_runs_total{result=\"ok\"} %d\nastryon_ch_aggregator_runs_total{result=\"error\"} %d\n"+
		"# HELP astryon_ch_aggregator_last_run_ok 1 if the latest recompute succeeded\n# TYPE astryon_ch_aggregator_last_run_ok gauge\nastryon_ch_aggregator_last_run_ok %d\n"+
		"# HELP astryon_ch_aggregator_last_success_timestamp_seconds Unix time of the latest successful recompute\n# TYPE astryon_ch_aggregator_last_success_timestamp_seconds gauge\nastryon_ch_aggregator_last_success_timestamp_seconds %d\n",
		s.OK, s.Failed, ok, s.LastSuccess.Unix())
	tmp, err2 := os.CreateTemp(filepath.Dir(s.Path), ".chagg-*.prom")
	if err2 != nil {
		return err2
	}
	defer os.Remove(tmp.Name())
	if _, err2 = tmp.WriteString(body); err2 != nil {
		tmp.Close()
		return err2
	}
	if err2 = tmp.Close(); err2 != nil {
		return err2
	}
	if err2 = os.Chmod(tmp.Name(), 0o644); err2 != nil {
		return err2
	}
	return os.Rename(tmp.Name(), s.Path)
}
