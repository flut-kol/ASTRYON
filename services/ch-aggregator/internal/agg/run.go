package agg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// RunOptions configures one `reconcile` run.
type RunOptions struct {
	Window         Window
	Now            time.Time
	FailOnGaps     bool
	Registry       Registry      // nil: registry check skipped with a notice
	Grace          time.Duration // batches younger than this (or not yet COMPLETED) are transient
	ChecksumWindow Window        // day aligned inside Checksums
	SnapshotPath   string        // write the checksum snapshot here ("" = none, "-" = JSON on Out)
	ComparePath    string        // previous snapshot to diff against ("" = none)
	Limit          int
	Out            io.Writer // human log lines
	SnapOut        io.Writer // receives the JSON snapshot for SnapshotPath "-"
}

// Run executes all reconcile checks and returns the process exit code: 2 for duplicates, aggregate drift, persistent
// registry mismatch or a silent checksum change (gaps and transient mismatches are informational), 1 for failures to
// run a check, else 0.
func (c *Client) Run(ctx context.Context, o RunOptions) int {
	logf := func(f string, a ...any) { fmt.Fprintf(o.Out, f+"\n", a...) }
	if o.Limit <= 0 {
		o.Limit = 100
	}
	bad := 0
	fail := func(what string, err error) int { logf("reconcile %s: %v", what, err); return 1 }

	dups, err := c.Reconcile(ctx, o.Window, o.Limit)
	if err != nil {
		return fail("duplicates", err)
	}
	for _, d := range dups {
		logf("duplicate-rows tenant=%s source=%s batch=%s pv=%d rows=%d unique=%d", d.TenantID, d.SourceID, d.BatchID, d.ProcessingVersion, d.Rows, d.Unique)
	}
	ad, err := c.AggregateDrift(ctx, o.Window, o.Limit)
	if err != nil {
		return fail("aggregates", err)
	}
	for _, d := range ad {
		logf("aggregate-drift tenant=%s source=%s pv=%d hour=%s stored=%d actual=%d", d.TenantID, d.SourceID, d.ProcessingVersion, d.Hour, d.Stored, d.Actual)
	}
	gaps, err := c.Gaps(ctx, o.Window, o.Limit)
	if err != nil {
		return fail("gaps", err)
	}
	for _, g := range gaps {
		logf("gap (informational) tenant=%s source=%s pv=%d empty-hour=%s", g.TenantID, g.SourceID, g.ProcessingVersion, g.Hour)
	}
	bad += len(dups) + len(ad)

	if o.Registry == nil {
		logf("registry-check skipped: REGISTRY_PG_URL not set")
	} else {
		rr, err := c.ReconcileRegistry(ctx, o.Registry, o.Window, o.Now, o.Grace, o.Limit)
		if err != nil {
			return fail("registry", err)
		}
		for _, m := range rr.Mismatches {
			logf("registry-mismatch kind=%s class=%s tenant=%s source=%s batch=%s pv=%d ch_events=%d registry_accepted=%d status=%s",
				m.Kind, m.Class, m.TenantID, m.SourceID, m.BatchID, m.ProcessingVersion, m.CHEvents, m.Accepted, m.Status)
		}
		if rr.Truncated {
			logf("registry-check partial: batch limit %d reached", o.Limit)
		}
		logf("registry-check: %d batch/version pairs compared, %d mismatches (%d persistent)", rr.Checked, len(rr.Mismatches), rr.Persistent())
		bad += rr.Persistent()
	}

	snap, err := c.Checksums(ctx, o.ChecksumWindow, o.Limit*50, o.Now)
	if err != nil {
		return fail("checksums", err)
	}
	if snap.Truncated {
		logf("checksums partial: partition limit reached")
	}
	logf("checksums: %d partitions %s..%s", len(snap.Partitions), snap.From.Format("2006-01-02"), snap.To.Format("2006-01-02"))
	switch o.SnapshotPath {
	case "":
	case "-":
		enc := json.NewEncoder(o.SnapOut)
		enc.SetIndent("", " ")
		_ = enc.Encode(snap)
	default:
		if err := WriteSnapshot(o.SnapshotPath, snap); err != nil {
			return fail("snapshot", err)
		}
		logf("snapshot written: %s", o.SnapshotPath)
	}
	if o.ComparePath != "" {
		prev, err := ReadSnapshot(o.ComparePath)
		if err != nil {
			return fail("compare", err)
		}
		cr := CompareSnapshots(prev, snap)
		for _, ch := range cr.Changes {
			tag := "checksum-change (count/revision changed too, explained)"
			if ch.Silent {
				tag = "checksum-change SILENT (corruption candidate)"
			}
			logf("%s tenant=%s source=%s date=%s pv=%d events=%d->%d checksum=%d->%d", tag, ch.TenantID, ch.SourceID, ch.EventDate, ch.ProcessingVersion, ch.PrevEvents, ch.Events, ch.PrevChecksum, ch.Checksum)
		}
		for _, p := range cr.Missing {
			logf("partition-missing (informational) tenant=%s source=%s date=%s pv=%d events=%d", p.TenantID, p.SourceID, p.EventDate, p.ProcessingVersion, p.Events)
		}
		logf("compare: %d changed (%d silent), %d missing, %d new", len(cr.Changes), cr.SilentCount(), len(cr.Missing), cr.Added)
		bad += cr.SilentCount()
	}

	if bad > 0 || (len(gaps) > 0 && o.FailOnGaps) {
		return 2
	}
	logf("reconcile: no drift (%d informational gaps)", len(gaps))
	return 0
}
