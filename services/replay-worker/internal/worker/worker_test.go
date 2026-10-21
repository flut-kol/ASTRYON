package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
)

const (
	tenantA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	tenantB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	srcID   = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

var t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// ---- fakes ----

type fakeSource struct {
	msgs      [][]byte
	pos       int
	committed []int64
}

func (f *fakeSource) Poll(ctx context.Context) (Delivery, bool, error) {
	if f.pos >= len(f.msgs) {
		return Delivery{}, false, nil
	}
	d := Delivery{Topic: "replay.jobs.v1", Offset: int64(f.pos), Value: f.msgs[f.pos]}
	f.pos++
	return d, true, nil
}
func (f *fakeSource) Commit(ctx context.Context, d Delivery) error {
	f.committed = append(f.committed, d.Offset)
	return nil
}

type fakeSink struct {
	recs []Record
	err  error
}

func (f *fakeSink) Publish(ctx context.Context, recs []Record) error {
	if f.err != nil {
		return f.err
	}
	f.recs = append(f.recs, recs...)
	return nil
}

// fakeCH holds rows per (tenant, version). It honours keyset order, dedup, ranges and Limit.
type fakeCH struct {
	rows     map[string][]SourceEvent // key tenant|version
	pages    int
	maxLimit int
	queried  []string // tenants seen
}

func (f *fakeCH) key(t string, v uint32) string { return fmt.Sprintf("%s|%d", t, v) }
func (f *fakeCH) BaseVersion(ctx context.Context, tenant, source string, below uint32) (uint32, error) {
	f.queried = append(f.queried, tenant)
	var best uint32
	for v := uint32(1); v < below; v++ {
		if len(f.rows[f.key(tenant, v)]) > 0 {
			best = v
		}
	}
	return best, nil
}
func (f *fakeCH) Page(ctx context.Context, q PageQuery) ([]SourceEvent, error) {
	f.queried = append(f.queried, q.TenantID)
	f.pages++
	if q.Limit > f.maxLimit {
		f.maxLimit = q.Limit
	}
	var all []SourceEvent
	seen := map[string]bool{}
	for _, r := range f.rows[f.key(q.TenantID, q.Version)] {
		if seen[r.EventID] || r.EventTime.Before(q.From) || !r.EventTime.Before(q.To) {
			continue
		}
		seen[r.EventID] = true
		all = append(all, r)
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].EventTime.Equal(all[j].EventTime) {
			return all[i].EventTime.Before(all[j].EventTime)
		}
		return all[i].EventID < all[j].EventID
	})
	var out []SourceEvent
	for _, r := range all {
		if q.After != nil && !(r.EventTime.After(q.After.EventTime) || (r.EventTime.Equal(q.After.EventTime) && r.EventID > q.After.EventID)) {
			continue
		}
		out = append(out, r)
		if len(out) == q.Limit {
			break
		}
	}
	return out, nil
}
func (f *fakeCH) Existing(ctx context.Context, tenant, source string, version uint32, from, to time.Time, ids []string) (map[string]bool, error) {
	have := map[string]bool{}
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	for _, r := range f.rows[f.key(tenant, version)] {
		if want[r.EventID] {
			have[r.EventID] = true
		}
	}
	return have, nil
}

type fakeStatus struct {
	calls []string
	err   map[string]error
}

func (f *fakeStatus) SetStatus(ctx context.Context, tenant, source string, v uint32, s string) error {
	f.calls = append(f.calls, fmt.Sprintf("%s/%d/%s", tenant, v, s))
	return f.err[s]
}

// ---- helpers ----

func mkRows(tenant string, n int) []SourceEvent {
	rows := make([]SourceEvent, n)
	for i := range rows {
		rows[i] = SourceEvent{
			EventID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), TenantID: tenant,
			BatchID: "00000000-0000-0000-0000-000000000000", PipelineID: "00000000-0000-0000-0000-000000000000",
			PipelineVersion: 1, EventType: "sensor.reading", EventTime: t0.Add(time.Duration(i) * time.Minute),
			IngestedAt: t0.Add(time.Hour), EntityID: fmt.Sprintf("dev-%d", i%3),
			Attributes: map[string]string{"b": "2", "a": "1"}, Payload: fmt.Sprintf(`{"v":%d}`, i), RawRef: "s3://b/k#" + fmt.Sprint(i),
		}
	}
	return rows
}

func jobJSON(t *testing.T, mut func(*Job)) []byte {
	j := Job{JobID: "job1", TenantID: tenantA, SourceID: srcID, PipelineVersion: 1, ProcessingVersion: 2,
		Ranges: []Range{{t0, t0.Add(24 * time.Hour)}}, CreatedAt: t0}
	if mut != nil {
		mut(&j)
	}
	b, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newWorker(src *fakeSource, ch *fakeCH, sink *fakeSink, st StatusUpdater) *Worker {
	w := &Worker{Source: src, Reader: ch, Sink: sink, PageSize: 4, Now: func() time.Time { return t0.Add(2 * time.Hour) }, Logf: func(string, ...any) {}}
	if st != nil {
		w.Status = st
	}
	return w
}

// ---- tests ----

func TestCopyForwardEnvelopes(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa|1": mkRows(tenantA, 10)}}
	// Duplicate row in the source must be collapsed (LIMIT 1 BY).
	ch.rows[ch.key(tenantA, 1)] = append(ch.rows[ch.key(tenantA, 1)], mkRows(tenantA, 1)[0])
	src := &fakeSource{msgs: [][]byte{jobJSON(t, nil)}}
	sink := &fakeSink{}
	st := &fakeStatus{}
	w := newWorker(src, ch, sink, st)
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.recs) != 10 {
		t.Fatalf("published %d, want 10 (dedup)", len(sink.recs))
	}
	if ch.pages < 3 {
		t.Fatalf("expected keyset paging over several pages, pages=%d", ch.pages)
	}
	if ch.maxLimit != 4 {
		t.Fatalf("page limit not applied: %d", ch.maxLimit)
	}
	ids := map[string]bool{}
	for _, r := range sink.recs {
		if r.Topic != "processed.events.v1" {
			t.Fatalf("topic %s", r.Topic)
		}
		var e struct {
			ec.Envelope
			PipelineVersion int    `json:"pipeline_version"`
			EventType       string `json:"event_type"`
			RawRef          string `json:"raw_ref"`
		}
		if err := json.Unmarshal(r.Value, &e); err != nil {
			t.Fatal(err)
		}
		if err := e.Envelope.ValidateAt(t0.Add(2 * time.Hour)); err != nil {
			t.Fatalf("envelope invalid: %v", err)
		}
		if e.ProcessingVersion != 2 || e.TenantID != tenantA || e.SourceID != srcID || e.TraceID != "replay:job1" || e.EventType != "sensor.reading" || e.PipelineVersion != 1 {
			t.Fatalf("bad envelope %+v", e)
		}
		if !bytes.Equal(r.Key, e.PartitionKey()) {
			t.Fatalf("key %q", r.Key)
		}
		ids[e.EventID] = true
	}
	if len(ids) != 10 {
		t.Fatalf("distinct ids %d", len(ids))
	}
	if fmt.Sprint(src.committed) != "[0]" {
		t.Fatalf("commit: %v", src.committed)
	}
	if strings.Join(st.calls, ",") != tenantA+"/2/RUNNING,"+tenantA+"/2/COMPLETED" {
		t.Fatalf("status calls: %v", st.calls)
	}
}

func TestSpacedPayloadSurvivesSinkSideValidation(t *testing.T) {
	row := mkRows(tenantA, 1)[0]
	row.Payload = "{ \"v\":  1,\n \"w\": [1, 2] }"
	job := Job{JobID: "j", TenantID: tenantA, SourceID: srcID, ProcessingVersion: 2}
	rec, err := BuildRecord("processed.events.v1", job, row, t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var e ec.Envelope // what the clickhouse-sink does with the wire bytes
	if err := json.Unmarshal(rec.Value, &e); err != nil {
		t.Fatal(err)
	}
	if err := e.ValidateAt(t0.Add(time.Hour)); err != nil {
		t.Fatalf("sink would quarantine: %v", err)
	}
}

func TestRedeliveryProducesIdenticalEnvelopes(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa|1": mkRows(tenantA, 9)}}
	run := func(now time.Time) []Record {
		sink := &fakeSink{}
		w := newWorker(&fakeSource{msgs: [][]byte{jobJSON(t, nil)}}, ch, sink, nil)
		w.Now = func() time.Time { return now } // different wall clock must not change output
		if _, err := w.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		return sink.recs
	}
	a, b := run(t0.Add(2*time.Hour)), run(t0.Add(5*time.Hour))
	if len(a) != 9 || len(a) != len(b) {
		t.Fatalf("len %d %d", len(a), len(b))
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, b[i].Key) || !bytes.Equal(a[i].Value, b[i].Value) {
			t.Fatalf("record %d differs:\n%s\n%s", i, a[i].Value, b[i].Value)
		}
	}
}

func TestResumeSkipsAlreadyPresentEvents(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	ch.rows[ch.key(tenantA, 1)] = mkRows(tenantA, 8)
	done := mkRows(tenantA, 8)[:5] // 5 already visible under the new version
	ch.rows[ch.key(tenantA, 2)] = done
	sink := &fakeSink{}
	w := newWorker(&fakeSource{msgs: [][]byte{jobJSON(t, nil)}}, ch, sink, nil)
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.recs) != 3 {
		t.Fatalf("published %d, want only the 3 missing", len(sink.recs))
	}
}

func TestFailureMarksFailedAndDoesNotCommit(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	ch.rows[ch.key(tenantA, 1)] = mkRows(tenantA, 6)
	src := &fakeSource{msgs: [][]byte{jobJSON(t, nil)}}
	st := &fakeStatus{}
	w := newWorker(src, ch, &fakeSink{err: errors.New("broker down")}, st)
	handled, err := w.Step(context.Background())
	if !handled || err == nil {
		t.Fatalf("want error, got handled=%v err=%v", handled, err)
	}
	if len(src.committed) != 0 {
		t.Fatalf("committed despite failure: %v", src.committed)
	}
	if got := strings.Join(st.calls, ","); got != tenantA+"/2/RUNNING,"+tenantA+"/2/FAILED" {
		t.Fatalf("status: %s", got)
	}
}

func TestStatusUpdateFailureDoesNotCommit(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	ch.rows[ch.key(tenantA, 1)] = mkRows(tenantA, 2)
	src := &fakeSource{msgs: [][]byte{jobJSON(t, nil)}}
	st := &fakeStatus{err: map[string]error{StatusCompleted: errors.New("pg down")}}
	w := newWorker(src, ch, &fakeSink{}, st)
	if _, err := w.Step(context.Background()); err == nil || len(src.committed) != 0 {
		t.Fatalf("err=%v committed=%v", err, src.committed)
	}
}

func TestPermanentFailuresCommitAndMarkFailed(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	bad := mkRows(tenantA, 3)
	bad[1].Payload = "not json"
	ch.rows[ch.key(tenantA, 1)] = bad
	for name, msg := range map[string][]byte{
		"undecodable": []byte("{nope"),
		"bad range":   jobJSON(t, func(j *Job) { j.Ranges = []Range{{t0, t0}} }),
		"invalid row": jobJSON(t, nil),
	} {
		src := &fakeSource{msgs: [][]byte{msg}}
		st := &fakeStatus{}
		sink := &fakeSink{}
		w := newWorker(src, ch, sink, st)
		if _, err := w.Step(context.Background()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(src.committed) != 1 {
			t.Fatalf("%s: poison job must be committed (skipped)", name)
		}
		if name != "undecodable" && !strings.HasSuffix(st.calls[len(st.calls)-1], "/FAILED") {
			t.Fatalf("%s: %v", name, st.calls)
		}
	}
}

func TestNotRunnableRequestIsSkipped(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	ch.rows[ch.key(tenantA, 1)] = mkRows(tenantA, 3)
	src := &fakeSource{msgs: [][]byte{jobJSON(t, nil)}}
	sink := &fakeSink{}
	st := &fakeStatus{err: map[string]error{StatusRunning: ErrNotRunnable}}
	w := newWorker(src, ch, sink, st)
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.recs) != 0 || len(src.committed) != 1 {
		t.Fatalf("cancelled/unknown request must not execute: recs=%d committed=%v", len(sink.recs), src.committed)
	}
}

// Found live: after a kill -9 mid-run the redelivered job met its own fresh RUNNING row, was skipped and committed, and the
// replay stayed RUNNING with a partial version forever. A recent RUNNING row must leave the offset uncommitted.
func TestRunningRecentlyIsNotCommitted(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	ch.rows[ch.key(tenantA, 1)] = mkRows(tenantA, 3)
	src := &fakeSource{msgs: [][]byte{jobJSON(t, nil)}}
	sink := &fakeSink{}
	st := &fakeStatus{err: map[string]error{StatusRunning: ErrRunningRecently}}
	w := newWorker(src, ch, sink, st)
	if _, err := w.Step(context.Background()); !errors.Is(err, ErrRunningRecently) {
		t.Fatalf("want ErrRunningRecently, got %v", err)
	}
	if len(sink.recs) != 0 || len(src.committed) != 0 {
		t.Fatalf("must neither execute nor commit: recs=%d committed=%v", len(sink.recs), src.committed)
	}
}

func TestTenantComesOnlyFromJob(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	ch.rows[ch.key(tenantA, 1)] = mkRows(tenantA, 3)
	ch.rows[ch.key(tenantB, 1)] = mkRows(tenantB, 5)
	// The job claims tenant A; nothing else (message key, headers) can name the tenant.
	src := &fakeSource{msgs: [][]byte{jobJSON(t, nil)}}
	sink := &fakeSink{}
	w := newWorker(src, ch, sink, nil)
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.recs) != 3 {
		t.Fatalf("published %d, want tenant A's 3", len(sink.recs))
	}
	for _, q := range ch.queried {
		if q != tenantA {
			t.Fatalf("queried tenant %s", q)
		}
	}
	for _, r := range sink.recs {
		if !strings.Contains(string(r.Value), `"tenant_id":"`+tenantA+`"`) {
			t.Fatalf("foreign tenant in %s", r.Value)
		}
	}
	// A row of another tenant returned by the store (e.g. a bug / wrong shard) is rejected, not forwarded.
	if _, err := BuildRecord("t", Job{JobID: "j", TenantID: tenantA, SourceID: srcID, ProcessingVersion: 2}, mkRows(tenantB, 1)[0], t0.Add(time.Hour)); !errors.Is(err, ErrPermanent) {
		t.Fatalf("cross-tenant row accepted: %v", err)
	}
}

func TestNoPreviousVersionPublishesNothing(t *testing.T) {
	ch := &fakeCH{rows: map[string][]SourceEvent{}}
	sink := &fakeSink{}
	st := &fakeStatus{}
	w := newWorker(&fakeSource{msgs: [][]byte{jobJSON(t, nil)}}, ch, sink, st)
	if _, err := w.Step(context.Background()); err != nil || len(sink.recs) != 0 || len(st.calls) != 2 {
		t.Fatalf("err=%v recs=%d calls=%v", err, len(sink.recs), st.calls)
	}
}

func TestJobValidation(t *testing.T) {
	ok := Job{JobID: "j", TenantID: strings.ToUpper(tenantA), SourceID: srcID, ProcessingVersion: 3, Ranges: []Range{{t0, t0.Add(time.Hour)}}}
	if err := ok.Validate(); err != nil || ok.TenantID != tenantA {
		t.Fatalf("%v %s", err, ok.TenantID)
	}
	for name, mut := range map[string]func(*Job){
		"tenant":  func(j *Job) { j.TenantID = "x" },
		"source":  func(j *Job) { j.SourceID = "" },
		"version": func(j *Job) { j.ProcessingVersion = 0 },
		"ranges":  func(j *Job) { j.Ranges = nil },
		"span":    func(j *Job) { j.Ranges = []Range{{t0, t0.Add(32 * 24 * time.Hour)}} },
	} {
		j := ok
		j.TenantID = tenantA
		mut(&j)
		if err := j.Validate(); !errors.Is(err, ErrPermanent) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
