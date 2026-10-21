package export

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestMetricsRender(t *testing.T) {
	m := &metrics{failed: map[string]int64{}, queueErr: map[string]int64{}}
	m.started.Add(3)
	m.exportDone("", 10, 100, 0.5)
	m.exportDone("row_cap_exceeded", 5, 50, 20)
	m.exportDone("row_cap_exceeded", 0, 0, 5000)
	m.queueError("claim")
	m.SetAuditStatsProvider(func() AuditWriterStats {
		return AuditWriterStats{Queued: 7, Written: 5, Dropped: 2, Rejected: 1}
	})
	var b bytes.Buffer
	m.WritePrometheus(&b)
	out := b.String()
	for _, want := range []string{
		"astryon_export_started_total 3", "astryon_export_succeeded_total 1",
		"astryon_export_rows_total 15", "astryon_export_bytes_total 150",
		`astryon_export_failed_total{code="row_cap_exceeded"} 2`,
		`astryon_export_queue_errors_total{op="claim"} 1`,
		"astryon_audit_queued_total 7", "astryon_audit_written_total 5",
		"astryon_audit_dropped_total 2", "astryon_audit_rejected_total 1",
		`astryon_export_duration_seconds_bucket{le="1"} 1`,
		`astryon_export_duration_seconds_bucket{le="60"} 2`,
		`astryon_export_duration_seconds_bucket{le="+Inf"} 3`,
		"astryon_export_duration_seconds_count 3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "tenant") {
		t.Error("tenant in metrics")
	}
}

func TestMetricsRace(t *testing.T) {
	m := &metrics{failed: map[string]int64{}, queueErr: map[string]int64{}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.exportDone("x", 1, 1, 1)
				m.queueError("claim")
				m.WritePrometheus(&bytes.Buffer{})
			}
		}()
	}
	wg.Wait()
}

func TestRunNoPresignCountsFailure(t *testing.T) {
	before := Metrics.started.Load()
	r := &Runner{}
	_, err := r.runNoPresign(context.Background(), Principal{}, "bad", ExportSpec{})
	if err == nil || !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("err=%v", err)
	}
	if Metrics.started.Load() != before+1 {
		t.Error("started not counted")
	}
	var b bytes.Buffer
	Metrics.WritePrometheus(&b)
	if !strings.Contains(b.String(), `astryon_export_failed_total{code="invalid_spec"}`) {
		t.Errorf("no invalid_spec:\n%s", b.String())
	}
}
