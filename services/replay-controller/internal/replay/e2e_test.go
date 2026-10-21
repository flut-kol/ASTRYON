package replay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// replayTable is the in-memory replay_requests the "worker" moves through REQUESTED -> RUNNING -> COMPLETED,
// the same transitions services/replay-worker's PGStatus applies, and which MemActivator reads for cutover.
type replayTable struct {
	mu     sync.Mutex
	status map[string]string // key tenant|source|version
}

func rk(t, s string, v uint32) string { return t + "|" + s + "|" + string(rune('0'+v)) }

func (r *replayTable) set(t, s string, v uint32, st string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status[rk(t, s, v)] = st
}

// tableAlloc is a RequestStore over the table so Reserve records the REQUESTED row.
type tableAlloc struct {
	*MemAllocator
	tbl *replayTable
	act *MemActivator
	n   int
}

func (a *tableAlloc) Reserve(ctx context.Context, tenantID, sourceID string, _, _ time.Time) (ReplayRequest, error) {
	v, _ := a.Allocate(ctx, tenantID, sourceID)
	a.n++
	id := "00000000-0000-0000-0000-00000000e" + string(rune('0'+a.n)) + "00"
	a.tbl.set(tenantID, sourceID, v, "REQUESTED")
	a.act.mu.Lock()
	a.act.Replays[id] = MemReplay{tenantID, sourceID, "REQUESTED", int32(v)} // #nosec G115 -- test value
	a.act.mu.Unlock()
	return ReplayRequest{ID: id, ProcessingVersion: v}, nil
}
func (a *tableAlloc) SetStatus(context.Context, string, string, string) error { return nil }

// TestE2EControllerToWorkerToActivate: POST /v1/replays -> job on the topic (JSON wire form) -> worker flips the
// row RUNNING -> COMPLETED (simulated by the table; the real worker is exercised in services/replay-worker's
// TestE2EControllerJobToCompleted against the same golden job) -> POST activate switches readers.
func TestE2EControllerToWorkerToActivate(t *testing.T) {
	tbl := &replayTable{status: map[string]string{}}
	act := &MemActivator{Replays: map[string]MemReplay{}}
	alloc := &tableAlloc{MemAllocator: NewMemAllocator(), tbl: tbl, act: act}
	alloc.Seed(tenantA, srcX, 1) // v1 exists in ClickHouse
	fp := &fakeProducer{}
	pub := &KafkaJobPublisher{p: fp, topic: TopicReplayJobs}
	audit := &MemAuditSink{}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Subject: "alice", Scopes: []string{ScopeReplayWrite}}}
	h := (&Handler{Auth: auth, Planner: &Planner{Alloc: alloc, Counter: FixedCounter{N: 7}, Publisher: pub}, Activator: act, Audit: audit}).Routes()
	call := func(method, path, b string) (int, map[string]any) {
		r := httptest.NewRequest(method, path, strings.NewReader(b))
		r.Header.Set("Authorization", "Bearer tok")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var m map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &m)
		return w.Code, m
	}
	code, plan := call("POST", "/v1/replays", body("", false))
	if code != http.StatusAccepted || plan["processing_version"] != float64(2) {
		t.Fatalf("%d %v", code, plan)
	}
	if fp.topic != TopicReplayJobs || len(fp.value) == 0 {
		t.Fatal("job not on the jobs topic")
	}
	var job ReplayJob
	if err := json.Unmarshal(fp.value, &job); err != nil || job.TenantID != tenantA || job.ProcessingVersion != 2 {
		t.Fatalf("%+v %v", job, err)
	}
	var replayID string
	for id, r := range act.Replays {
		replayID = id
		if r.Status != "REQUESTED" {
			t.Fatal(r.Status)
		}
	}
	// Not COMPLETED yet: cutover refused, nothing switched.
	if code, m := call("POST", "/v1/replays/"+replayID+"/activate", ""); code != 409 || m["code"] != "replay_not_completed" {
		t.Fatalf("%d %v", code, m)
	}
	// Worker: RUNNING then COMPLETED.
	for _, st := range []string{"RUNNING", "COMPLETED"} {
		act.mu.Lock()
		r := act.Replays[replayID]
		r.Status = st
		act.Replays[replayID] = r
		act.mu.Unlock()
	}
	code, a := call("POST", "/v1/replays/"+replayID+"/activate", "")
	if code != 200 || a["action"] != "activate" || a["active_processing_version"] != float64(2) {
		t.Fatalf("%d %v", code, a)
	}
	if v, _, ok := act.Active(tenantA, srcX); !ok || v != 2 {
		t.Fatalf("active %d %v", v, ok)
	}
}

// TestJobWireFormMatchesGolden pins the JSON the worker consumes (testdata/job.v1.json is also decoded and run by
// services/replay-worker). Changing a field here must change the golden file and the worker together.
func TestJobWireFormMatchesGolden(t *testing.T) {
	golden, err := os.ReadFile("testdata/job.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var job ReplayJob
	if err := json.Unmarshal(golden, &job); err != nil {
		t.Fatal(err)
	}
	fp := &fakeProducer{}
	if err := (&KafkaJobPublisher{p: fp, topic: TopicReplayJobs}).Publish(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	var got, want map[string]any
	_ = json.Unmarshal(fp.value, &got)
	_ = json.Unmarshal(golden, &want)
	gb, _ := json.Marshal(got)
	wb, _ := json.Marshal(want)
	if string(gb) != string(wb) {
		t.Fatalf("wire form drifted:\n got %s\nwant %s", gb, wb)
	}
}
