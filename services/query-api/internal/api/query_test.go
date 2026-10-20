package api

import (
	"context"
	"strings"
	"testing"
	"time"
)

const (
	tenantA = "11111111-1111-4111-8111-111111111111"
	tenantB = "22222222-2222-4222-8222-222222222222"
	srcID   = "33333333-3333-4333-8333-333333333333"
)

func rng() TimeRange {
	to := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	return TimeRange{From: to.Add(-24 * time.Hour), To: to}
}

func assertTenant(t *testing.T, q Query, tenant string) {
	t.Helper()
	if !strings.Contains(q.SQL, "tenant_id = {tenant_id:UUID}") {
		t.Fatalf("missing tenant predicate: %s", q.SQL)
	}
	if q.Params["tenant_id"] != tenant {
		t.Fatalf("tenant param = %q, want %q", q.Params["tenant_id"], tenant)
	}
	// tenant predicate must be in the first WHERE position of the (sub)query
	if i := strings.Index(q.SQL, "WHERE "); i < 0 || !strings.HasPrefix(q.SQL[i:], "WHERE tenant_id = {tenant_id:UUID}") {
		t.Fatalf("tenant predicate not first: %s", q.SQL)
	}
}

func TestEventsQueryAlwaysTenantScopedAndParameterized(t *testing.T) {
	evil := "x'; DROP TABLE events_normalized_local; --"
	pv := uint32(3)
	reqs := []EventsRequest{
		{TimeRange: rng()},
		{TimeRange: rng(), Order: "asc", IncludePayload: true},
		{TimeRange: rng(), Filters: EventFilters{SourceIDs: []string{srcID}, EventTypes: []string{evil, "b"}, EntityID: evil,
			ProcessingVersion: &pv, Attributes: map[string]string{evil: evil, "k": "v"}}},
	}
	for _, r := range reqs {
		if err := r.Normalize(); err != nil {
			t.Fatal(err)
		}
		for _, after := range []*Keyset{nil, {Time: rng().From, ID: srcID}} {
			q := BuildEventsQuery(tenantA, r, after)
			assertTenant(t, q, tenantA)
			if strings.Contains(q.SQL, "DROP") || strings.Contains(q.SQL, "x'") {
				t.Fatalf("user data leaked into SQL: %s", q.SQL)
			}
			if !strings.Contains(q.SQL, "LIMIT 1 BY") {
				t.Fatalf("missing dedup: %s", q.SQL)
			}
		}
	}
	r0 := reqs[0]
	_ = r0.Normalize()
	q := BuildEventsQuery(tenantB, r0, nil)
	assertTenant(t, q, tenantB)
	if q.Params["limit"] != "101" {
		t.Fatalf("limit+1 expected, got %s", q.Params["limit"])
	}
}

func TestEventsRequestValidation(t *testing.T) {
	ok := func() EventsRequest { return EventsRequest{TimeRange: rng()} }
	cases := map[string]func(*EventsRequest){
		"no range":   func(r *EventsRequest) { r.TimeRange = TimeRange{} },
		"inverted":   func(r *EventsRequest) { r.TimeRange.To = r.TimeRange.From.Add(-time.Hour) },
		"too wide":   func(r *EventsRequest) { r.TimeRange.From = r.TimeRange.To.Add(-91 * 24 * time.Hour) },
		"limit high": func(r *EventsRequest) { r.Limit = MaxEventsLimit + 1 },
		"limit neg":  func(r *EventsRequest) { r.Limit = -1 },
		"bad order":  func(r *EventsRequest) { r.Order = "sideways" },
		"bad source": func(r *EventsRequest) { r.Filters.SourceIDs = []string{"nope"} },
		"many types": func(r *EventsRequest) { r.Filters.EventTypes = make([]string, MaxInValues+1) },
		"many attrs": func(r *EventsRequest) {
			r.Filters.Attributes = map[string]string{"a": "", "b": "", "c": "", "d": "", "e": "", "f": ""}
		},
	}
	for name, mut := range cases {
		r := ok()
		mut(&r)
		if err := r.Normalize(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	r := ok()
	if err := r.Normalize(); err != nil || r.Limit != DefaultLimit || r.Order != "desc" {
		t.Fatalf("defaults: %v %+v", err, r)
	}
}

func TestAggregateWhitelistAndTenant(t *testing.T) {
	for _, g := range []string{"source_id); DROP TABLE x;--", "payload", "tenant_id", ""} {
		r := AggregateRequest{TimeRange: rng(), GroupBy: []string{g}}
		if r.Normalize() == nil {
			t.Errorf("group_by %q accepted", g)
		}
	}
	for _, m := range []string{"sum(payload)", "count", "events,1"} {
		r := AggregateRequest{TimeRange: rng(), Metrics: []string{m}}
		if r.Normalize() == nil {
			t.Errorf("metric %q accepted", m)
		}
	}
	if (&AggregateRequest{TimeRange: rng(), GroupBy: []string{"day", "day"}}).Normalize() == nil {
		t.Error("duplicate group_by accepted")
	}
	if (&AggregateRequest{TimeRange: rng(), GroupBy: []string{"day", "hour", "source_id", "event_type"}}).Normalize() == nil {
		t.Error("too many group_by accepted")
	}
	if (&AggregateRequest{TimeRange: rng(), Limit: MaxAggLimit + 1}).Normalize() == nil {
		t.Error("limit accepted")
	}
	r := AggregateRequest{TimeRange: rng(), GroupBy: []string{"day", "event_type"}, Metrics: []string{"events", "entities"},
		Filters: EventFilters{EntityID: "e'1"}}
	if err := r.Normalize(); err != nil {
		t.Fatal(err)
	}
	q := BuildAggregateQuery(tenantA, r)
	assertTenant(t, q, tenantA)
	if !strings.Contains(q.SQL, "GROUP BY g_day, g_event_type") || strings.Contains(q.SQL, "e'1") {
		t.Fatalf("unexpected SQL %s", q.SQL)
	}
}

func TestBuildAggregatePanicsOnUnwhitelisted(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	BuildAggregateQuery(tenantA, AggregateRequest{TimeRange: rng(), GroupBy: []string{"1; DROP"}})
}

// captureRunner records every query and returns canned rows.
type captureRunner struct {
	qs   []Query
	rows []Row
}

func (c *captureRunner) Run(_ context.Context, q Query) ([]Row, error) {
	c.qs = append(c.qs, q)
	return c.rows, nil
}
func (c *captureRunner) Ping(context.Context) error { return nil }

func TestSummariesAreTenantScoped(t *testing.T) {
	cr := &captureRunner{}
	a := CHAnalytics{R: cr}
	ctx := context.Background()
	if _, err := a.MetricsSummary(ctx, tenantA, rng(), srcID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.QualitySummary(ctx, tenantB, rng(), ""); err != nil {
		t.Fatal(err)
	}
	if len(cr.qs) != 5 {
		t.Fatalf("expected 5 queries, got %d", len(cr.qs))
	}
	for i, q := range cr.qs {
		want := tenantA
		if i >= 2 {
			want = tenantB
		}
		assertTenant(t, q, want)
	}
}
