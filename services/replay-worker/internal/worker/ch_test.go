package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCHQueriesAreReadonlyAndBound(t *testing.T) {
	var qs []string
	var params []map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("readonly") != "1" || r.Method != http.MethodPost {
			t.Errorf("readonly=%q method=%s", q.Get("readonly"), r.Method)
		}
		p := map[string]string{}
		for k, v := range q {
			if strings.HasPrefix(k, "param_") {
				p[strings.TrimPrefix(k, "param_")] = v[0]
			}
		}
		qs, params = append(qs, q.Get("query")), append(params, p)
		if strings.Contains(q.Get("query"), "max(") {
			w.Write([]byte("1\n"))
			return
		}
		if strings.Contains(q.Get("query"), "ingested_at") {
			w.Write([]byte(`{"tenant_id":"` + tenantA + `","event_id":"00000000-0000-4000-8000-000000000001","batch_id":"b","pipeline_id":"p","pipeline_version":1,"event_type":"x","event_time":"2026-10-01 00:00:00.123","ingested_at":"2026-10-01 01:00:00.000","entity_id":"e","attributes":{"a":"1"},"payload":"{\"v\":1}","raw_ref":"r","quality_flags":[]}` + "\n"))
		}
	}))
	defer srv.Close()
	c := &CH{URL: srv.URL}
	ctx := context.Background()
	evil := "x' OR 1=1 --"
	if _, err := c.BaseVersion(ctx, evil, srcID, 5); err != nil {
		t.Fatal(err)
	}
	rows, err := c.Page(ctx, PageQuery{TenantID: tenantA, SourceID: srcID, Version: 1, From: t0, To: t0.Add(time.Hour), Limit: 10,
		After: &Cursor{EventTime: t0, EventID: "00000000-0000-4000-8000-000000000000"}})
	if err != nil || len(rows) != 1 || rows[0].EventTime.Format("15:04:05.000") != "00:00:00.123" || rows[0].Payload != `{"v":1}` {
		t.Fatalf("%v %+v", err, rows)
	}
	if _, err := c.Existing(ctx, tenantA, srcID, 2, t0, t0, []string{"00000000-0000-4000-8000-000000000001"}); err != nil {
		t.Fatal(err)
	}
	for i, q := range qs {
		if strings.Contains(q, evil) || strings.Contains(q, tenantA) || strings.Contains(q, srcID) {
			t.Fatalf("value concatenated into SQL: %s", q)
		}
		if i == 0 && params[0]["tenant"] != evil {
			t.Fatalf("tenant not bound: %v", params[0])
		}
	}
	if !strings.Contains(qs[1], "LIMIT 1 BY") || !strings.Contains(qs[1], "ORDER BY event_time, event_id") || !strings.Contains(qs[1], "(event_time, event_id) > (") {
		t.Fatalf("page sql: %s", qs[1])
	}
}
