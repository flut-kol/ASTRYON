package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
)

var sources = []string{
	"11111111-1111-4111-8111-aaaaaaaaaaa1", "11111111-1111-4111-8111-aaaaaaaaaaa2",
	"11111111-1111-4111-8111-aaaaaaaaaaa3", "11111111-1111-4111-8111-aaaaaaaaaaa4",
}

type gwResp struct {
	Accepted    int `json:"accepted"`
	Quarantined int `json:"quarantined"`
	Failed      int `json:"failed"`
}

// mkEvent builds a valid envelope. Same (id, version, tag) -> identical bytes.
func mkEvent(tenant, id string, i, pv int, tag string, at time.Time) []byte {
	et := at.Add(-time.Duration(i) * time.Second).UTC().Truncate(time.Millisecond)
	e := ec.Envelope{
		EventID: id, TenantID: tenant, SourceID: sources[i%len(sources)], SchemaID: "e2e.event", SchemaVersion: 1,
		EventTime: &et, IngestedAt: at.UTC(), EntityKey: fmt.Sprintf("ent-%d", i%17), TraceID: "e2e-trace",
		PayloadFormat: ec.FormatJSON, ProcessingVersion: pv,
		Payload: json.RawMessage(fmt.Sprintf(`{"i":%d,"tag":%q}`, i, tag)),
	}
	b, _ := json.Marshal(e)
	return b
}

// invalidEvents returns n deliberately broken events (each rejected by the gateway).
func invalidEvents(tenant string, n int, at time.Time) [][]byte {
	var out [][]byte
	for i := 0; i < n; i++ {
		var m map[string]any
		_ = json.Unmarshal(mkEvent(tenant, uuid(), i, 1, "bad", at), &m)
		switch i % 5 {
		case 0:
			m["event_id"] = "not-a-uuid"
		case 1:
			delete(m, "event_time")
		case 2:
			m["schema_version"] = 0
		case 3:
			delete(m, "payload")
		default:
			m["unknown_field_xyz"] = 1
		}
		b, _ := json.Marshal(m)
		out = append(out, b)
	}
	return out
}

func (ts *tenantSvc) post(t testing.TB, evs [][]byte) (int, gwResp) {
	var r gwResp
	raw := make([]json.RawMessage, len(evs))
	for i, e := range evs {
		raw[i] = e
	}
	code := httpJSON(t, "POST", ts.gw+"/v1/events/batch", ts.token, map[string]any{"events": raw}, &r)
	return code, r
}

// postAll posts in chunks of 50 and returns summed counts.
func (ts *tenantSvc) postAll(t testing.TB, evs [][]byte) gwResp {
	var sum gwResp
	for i := 0; i < len(evs); i += 50 {
		j := min(i+50, len(evs))
		code, r := ts.post(t, evs[i:j])
		if code != 200 && code != 207 {
			t.Fatalf("gateway status %d", code)
		}
		sum.Accepted += r.Accepted
		sum.Quarantined += r.Quarantined
		sum.Failed += r.Failed
	}
	return sum
}

type qEvent struct {
	EventID           string  `json:"event_id"`
	SourceID          string  `json:"source_id"`
	ProcessingVersion uint32  `json:"processing_version"`
	EntityID          string  `json:"entity_id"`
	Payload           *string `json:"payload"`
}

// queryAll pages through POST /v1/query/events and returns every item.
func (ts *tenantSvc) queryAll(t testing.TB, filters map[string]any, withPayload bool) []qEvent {
	var all []qEvent
	cursor := ""
	for page := 0; page < 100; page++ {
		now := time.Now().UTC()
		body := map[string]any{
			"time_range": map[string]any{"from": now.Add(-2 * time.Hour).Format(time.RFC3339), "to": now.Add(10 * time.Minute).Format(time.RFC3339)},
			"filters":    filters, "limit": 100, "include_payload": withPayload, "order": "asc",
		}
		if cursor != "" {
			body["cursor"] = cursor
		}
		var resp struct {
			Items      []qEvent `json:"items"`
			NextCursor *string  `json:"next_cursor"`
		}
		if code := httpJSON(t, "POST", ts.qa+"/v1/query/events", ts.token, body, &resp); code != 200 {
			t.Fatalf("query status %d", code)
		}
		all = append(all, resp.Items...)
		if resp.NextCursor == nil {
			return all
		}
		cursor = *resp.NextCursor
	}
	t.Fatal("too many pages")
	return nil
}

func (ts *tenantSvc) chCount(t testing.TB, e *env, extra string) (rows, uniq int) {
	out := e.chQuery(t, fmt.Sprintf("SELECT count(), uniqExact(event_id) FROM events_normalized_local WHERE tenant_id='%s' %s FORMAT TSV", ts.tenant, extra))
	fmt.Sscanf(out, "%d\t%d", &rows, &uniq)
	return
}

func waitRows(t testing.TB, e *env, ts *tenantSvc, rows int) {
	eventually(t, 60*time.Second, fmt.Sprintf("%d rows in ClickHouse for tenant %s", rows, ts.tenant[:8]), func() (bool, string) {
		r, u := ts.chCount(t, e, "")
		return r == rows, fmt.Sprintf("rows=%d uniq=%d", r, u)
	})
}

// toInt reads a JSON number that ClickHouse may render as a string (UInt64).
func toInt(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case string:
		var n int
		fmt.Sscanf(x, "%d", &n)
		return n
	}
	return -1
}

func idSet(evs []qEvent) map[string]uint32 {
	m := map[string]uint32{}
	for _, e := range evs {
		m[e.EventID] = e.ProcessingVersion
	}
	return m
}

// TestE2EPipeline: gateway -> raw -> stream-processor -> processed -> sink -> ClickHouse -> query-api.
func TestE2EPipeline(t *testing.T) {
	e := getStack(t)
	start := time.Now().Add(-5 * time.Second)
	A, B := e.startTenant(t, nil), e.startTenant(t, nil)
	now := time.Now().UTC()

	const nA, nDup, nBad, nB = 200, 20, 10, 50
	var evA, evB [][]byte
	idsA, idsB := map[string]bool{}, map[string]bool{}
	for i := 0; i < nA; i++ {
		id := uuid()
		idsA[id] = true
		evA = append(evA, mkEvent(A.tenant, id, i, 1, "a", now))
	}
	for i := 0; i < nB; i++ {
		id := uuid()
		idsB[id] = true
		evB = append(evB, mkEvent(B.tenant, id, i, 1, "b", now))
	}
	// exact re-posts of the first nDup events of A (identical bytes and event_id)
	post := append(append([][]byte{}, evA...), evA[:nDup]...)
	post = append(post, invalidEvents(A.tenant, nBad, now)...)

	ra := A.postAll(t, post)
	rb := B.postAll(t, evB)
	if ra.Accepted != nA+nDup || ra.Quarantined != nBad || ra.Failed != 0 {
		t.Fatalf("gateway A counts: %+v", ra)
	}
	if rb.Accepted != nB || rb.Quarantined != 0 || rb.Failed != 0 {
		t.Fatalf("gateway B counts: %+v", rb)
	}

	// one record that bypasses the gateway and is invalid -> stream-processor must quarantine it
	badID := uuid()
	var m map[string]any
	_ = json.Unmarshal(mkEvent(A.tenant, badID, 0, 1, "bypass", now), &m)
	m["schema_version"] = 0
	m["ingested_at"] = now.Format(time.RFC3339Nano)
	bad, _ := json.Marshal(m)
	e.produceRaw(t, []byte(A.tenant+"\x1f"+badID), bad)

	waitRows(t, e, A, nA)
	waitRows(t, e, B, nB)

	// duplicates were dropped upstream (not only hidden at query time): rows == unique ids == 200
	if r, u := A.chCount(t, e, ""); r != nA || u != nA {
		t.Fatalf("tenant A clickhouse rows=%d uniq=%d, want %d/%d (duplicates must be dropped by stream-processor)", r, u, nA, nA)
	}
	if d := expvarInt(t, e.procMetrics, "streamproc", "duplicates"); d < nDup {
		t.Fatalf("stream-processor duplicates metric=%d, want >= %d", d, nDup)
	}

	// query-api: counts and ids
	gotA := idSet(A.queryAll(t, map[string]any{}, false))
	if len(gotA) != nA {
		t.Fatalf("tenant A query returned %d events, want %d", len(gotA), nA)
	}
	for id := range idsA {
		if _, ok := gotA[id]; !ok {
			t.Fatalf("event %s missing in tenant A query", id)
		}
	}
	gotB := idSet(B.queryAll(t, map[string]any{}, false))
	if len(gotB) != nB {
		t.Fatalf("tenant B query returned %d events, want %d", len(gotB), nB)
	}

	// tenant isolation
	for id := range gotB {
		if idsA[id] {
			t.Fatalf("tenant B sees tenant A event %s", id)
		}
	}
	for id := range gotA {
		if idsB[id] {
			t.Fatalf("tenant A sees tenant B event %s", id)
		}
	}
	// B asking for A's source ids still only gets B's own rows (same source ids are used by both tenants)
	if n := len(B.queryAll(t, map[string]any{"source_ids": sources[:1]}, false)); n == 0 || n > nB {
		t.Fatalf("tenant B source-filter query returned %d rows (expected only its own, <= %d)", n, nB)
	}
	var out map[string]any
	if code := httpJSON(t, "POST", B.qa+"/v1/query/events", A.token, map[string]any{
		"time_range": map[string]any{"from": now.Add(-time.Hour).Format(time.RFC3339), "to": now.Add(time.Hour).Format(time.RFC3339)}}, &out); code != 401 && code != 403 {
		t.Fatalf("A's token on B's query-api: status %d, want 401/403", code)
	}
	// tenant_id in a body that differs from the token is refused by the gateway (never trusted)
	if code, _ := B.post(t, [][]byte{mkEvent(A.tenant, uuid(), 1, 1, "x", now)}); code != 403 {
		t.Fatalf("cross-tenant post status %d, want 403", code)
	}
	if n, _ := B.chCount(t, e, ""); n != nB {
		t.Fatalf("tenant B rows=%d after cross-tenant attempt, want %d", n, nB)
	}

	// invalid events: 10 quarantined at the gateway + 1 at the stream-processor, none stored
	eventually(t, 30*time.Second, "quarantine records", func() (bool, string) {
		q := e.readTopic(t, "events.quarantine.v1", A.tenant, start)
		gw, sp := 0, 0
		for _, v := range q {
			if strings.Contains(string(v), `"stage":"stream-processor"`) {
				if strings.Contains(string(v), badID) {
					sp++
				}
			} else {
				gw++
			}
		}
		return gw == nBad && sp == 1, fmt.Sprintf("gateway=%d processor=%d", gw, sp)
	})
	if n := e.chQuery(t, fmt.Sprintf("SELECT count() FROM events_normalized_local WHERE event_id='%s' FORMAT TSV", badID)); n != "0" {
		t.Fatalf("quarantined event %s was stored (%s rows)", badID, n)
	}
	if q := expvarInt(t, e.procMetrics, "streamproc", "quarantined"); q < 1 {
		t.Fatalf("stream-processor quarantined metric=%d", q)
	}
	// aggregates agree with row counts (distinct events, no doubling)
	var agg struct {
		Rows []map[string]any `json:"rows"`
	}
	nw := time.Now().UTC()
	httpJSON(t, "POST", A.qa+"/v1/query/aggregate", A.token, map[string]any{
		"time_range": map[string]any{"from": nw.Add(-2 * time.Hour).Format(time.RFC3339), "to": nw.Add(10 * time.Minute).Format(time.RFC3339)},
		"metrics":    []string{"events"}}, &agg)
	t.Logf("aggregate: %v", agg.Rows)
	if len(agg.Rows) != 1 || toInt(agg.Rows[0]["events"]) != nA {
		t.Fatalf("aggregate events: %v, want %d", agg.Rows, nA)
	}
}

// TestE2EReplay: re-publishing the same events with the same processing_version is
// idempotent; with a new processing_version the old rows stay (rollback possible)
// but queries/aggregates see each event once, at the highest version.
func TestE2EReplay(t *testing.T) {
	e := getStack(t)
	C := e.startTenant(t, nil)
	now := time.Now().UTC()
	const n = 100
	ids := make([]string, n)
	for i := range ids {
		ids[i] = uuid()
	}
	build := func(pv int, tag string) [][]byte {
		var evs [][]byte
		for i, id := range ids {
			evs = append(evs, mkEvent(C.tenant, id, i, pv, tag, now))
		}
		return evs
	}
	dupBefore := func() int64 { return expvarInt(t, e.procMetrics, "streamproc", "duplicates") }
	checkQuery := func(wantPV uint32, wantTag string) {
		t.Helper()
		got := C.queryAll(t, map[string]any{}, true)
		if len(got) != n {
			t.Fatalf("query returned %d events, want %d (replay must not double)", len(got), n)
		}
		for _, ev := range got {
			if ev.ProcessingVersion != wantPV {
				t.Fatalf("event %s at processing_version %d, want %d", ev.EventID, ev.ProcessingVersion, wantPV)
			}
			if ev.Payload == nil || !strings.Contains(*ev.Payload, `"tag":"`+wantTag+`"`) {
				t.Fatalf("event %s payload %v does not carry tag %q", ev.EventID, ev.Payload, wantTag)
			}
		}
	}
	aggEvents := func(group string) map[string]any {
		var agg struct {
			Rows []map[string]any `json:"rows"`
		}
		nw := time.Now().UTC()
		body := map[string]any{
			"time_range": map[string]any{"from": nw.Add(-2 * time.Hour).Format(time.RFC3339), "to": nw.Add(10 * time.Minute).Format(time.RFC3339)},
			"metrics":    []string{"events"}}
		if group != "" {
			body["group_by"] = []string{group}
		}
		httpJSON(t, "POST", C.qa+"/v1/query/aggregate", C.token, body, &agg)
		res := map[string]any{}
		for _, it := range agg.Rows {
			k := "total"
			if group != "" {
				k = fmt.Sprint(it[group])
			}
			res[k] = it["events"]
		}
		return res
	}

	// 1. initial load at pv=1
	if r := C.postAll(t, build(1, "v1")); r.Accepted != n {
		t.Fatalf("initial: %+v", r)
	}
	waitRows(t, e, C, n)
	checkQuery(1, "v1")

	// 2. same-version replay (identical events): dropped by stream-processor, nothing new in ClickHouse
	d0 := dupBefore()
	if r := C.postAll(t, build(1, "v1")); r.Accepted != n {
		t.Fatalf("same-version replay: %+v", r)
	}
	eventually(t, 30*time.Second, "processor to drop same-version replay", func() (bool, string) {
		d := dupBefore() - d0
		return d >= n, fmt.Sprintf("duplicates delta=%d", d)
	})
	time.Sleep(2 * time.Second) // give the sink a chance to (wrongly) insert something
	if r, u := C.chCount(t, e, ""); r != n || u != n {
		t.Fatalf("after same-version replay rows=%d uniq=%d, want %d/%d", r, u, n, n)
	}
	checkQuery(1, "v1")

	// 3. replay into a NEW processing_version: rows double physically, query results must not
	if r := C.postAll(t, build(2, "v2")); r.Accepted != n {
		t.Fatalf("pv2 replay: %+v", r)
	}
	waitRows(t, e, C, 2*n)
	checkQuery(2, "v2")
	if got := aggEvents(""); toInt(got["total"]) != n {
		t.Fatalf("aggregate events after replay = %v, want %d (must not double)", got, n)
	}
	// rollback view: the old version is still queryable and untouched
	if old := C.queryAll(t, map[string]any{"processing_version": 1}, true); len(old) != n {
		t.Fatalf("processing_version=1 view has %d events, want %d", len(old), n)
	}
	byVer := aggEvents("processing_version")
	if toInt(byVer["1"]) != n || toInt(byVer["2"]) != n {
		t.Fatalf("per-version aggregate = %v, want %d each", byVer, n)
	}

	// 4. same new version again: idempotent
	d0 = dupBefore()
	if r := C.postAll(t, build(2, "v2")); r.Accepted != n {
		t.Fatalf("pv2 re-replay: %+v", r)
	}
	eventually(t, 30*time.Second, "processor to drop pv2 re-replay", func() (bool, string) {
		d := dupBefore() - d0
		return d >= n, fmt.Sprintf("duplicates delta=%d", d)
	})
	time.Sleep(2 * time.Second)
	if r, _ := C.chCount(t, e, ""); r != 2*n {
		t.Fatalf("rows=%d after pv2 re-replay, want %d", r, 2*n)
	}
	checkQuery(2, "v2")
	if got := aggEvents(""); toInt(got["total"]) != n {
		t.Fatalf("aggregate events after re-replay = %v, want %d", got, n)
	}
}
