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
	return writeAtomic(s.Path, body)
}

func writeAtomic(path, body string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".chagg-*.prom")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.WriteString(body); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Chmod(tmp.Name(), 0o644); err != nil { // #nosec G302 -- metrics file must be readable by the Prometheus textfile collector
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// ReconcileStats is what one reconcile run found, exported as a textfile so drift can page without log scraping.
type ReconcileStats struct {
	Duplicates, AggregateDrift, RegistryPersistent, SilentChecksum int
}

// Write replaces the file atomically. exit is the Run result: 0 clean, 2 drift, 1 a check could not run; counts of
// checks that did not run stay 0, so alert on last_exit_code as well.
func (r *ReconcileStats) Write(path string, now time.Time, exit int) error {
	body := fmt.Sprintf("# HELP astryon_ch_reconcile_findings Findings of the latest reconcile run by kind\n# TYPE astryon_ch_reconcile_findings gauge\n"+
		"astryon_ch_reconcile_findings{kind=\"duplicate_rows\"} %d\nastryon_ch_reconcile_findings{kind=\"aggregate_drift\"} %d\n"+
		"astryon_ch_reconcile_findings{kind=\"registry_mismatch\"} %d\nastryon_ch_reconcile_findings{kind=\"silent_checksum\"} %d\n"+
		"# HELP astryon_ch_reconcile_last_exit_code Exit code of the latest reconcile run (0 clean, 2 drift, 1 check failed)\n# TYPE astryon_ch_reconcile_last_exit_code gauge\nastryon_ch_reconcile_last_exit_code %d\n"+
		"# HELP astryon_ch_reconcile_last_run_timestamp_seconds Unix time of the latest reconcile run\n# TYPE astryon_ch_reconcile_last_run_timestamp_seconds gauge\nastryon_ch_reconcile_last_run_timestamp_seconds %d\n",
		r.Duplicates, r.AggregateDrift, r.RegistryPersistent, r.SilentChecksum, exit, now.Unix())
	return writeAtomic(path, body)
}
