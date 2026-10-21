package worker

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// goldenJob is the job JSON the replay-controller publishes (its test TestJobWireFormMatchesGolden pins the same
// file). Reading it across the module boundary proves the real controller wire form is accepted by the real worker.
const goldenJob = "../../../replay-controller/internal/replay/testdata/job.v1.json"

// TestE2EControllerJobToCompleted: controller job bytes -> Worker.Step -> v1 events copied to v2 on the output
// topic, request RUNNING -> COMPLETED, offset committed. (Activation is the controller's step; see its
// TestE2EControllerToWorkerToActivate.)
func TestE2EControllerJobToCompleted(t *testing.T) {
	raw, err := os.ReadFile(goldenJob)
	if err != nil {
		t.Fatal(err)
	}
	var g Job
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	rows := mkRows(g.TenantID, 6)
	day := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	for i := range rows {
		rows[i].EventTime = day.Add(time.Duration(i) * time.Hour)
		rows[i].IngestedAt = day.Add(24 * time.Hour)
	}
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	ch.rows[ch.key(g.TenantID, 1)] = rows
	src := &fakeSource{msgs: [][]byte{raw}}
	sink := &fakeSink{}
	st := &fakeStatus{}
	w := newWorker(src, ch, sink, st)
	w.Now = func() time.Time { return day.Add(48 * time.Hour) }
	if handled, err := w.Step(context.Background()); err != nil || !handled {
		t.Fatalf("%v %v", handled, err)
	}
	if len(sink.recs) != 6 {
		t.Fatalf("published %d", len(sink.recs))
	}
	for _, r := range sink.recs {
		var e struct {
			TenantID          string `json:"tenant_id"`
			ProcessingVersion int    `json:"processing_version"`
		}
		if err := json.Unmarshal(r.Value, &e); err != nil || e.ProcessingVersion != int(g.ProcessingVersion) || e.TenantID != g.TenantID {
			t.Fatalf("%v %+v", err, e)
		}
	}
	want := g.TenantID + "/2/RUNNING," + g.TenantID + "/2/COMPLETED"
	if strings.Join(st.calls, ",") != want || len(src.committed) != 1 {
		t.Fatalf("status %v committed %v", st.calls, src.committed)
	}
}
