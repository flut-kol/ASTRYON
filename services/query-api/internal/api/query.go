package api

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Hard limits. These are intentionally constants, not request-tunable.
const (
	DefaultLimit     = 100
	MaxEventsLimit   = 1000
	MaxAggLimit      = 1000
	MaxRange         = 90 * 24 * time.Hour
	MaxInValues      = 20
	MaxAttributeKeys = 5
	MaxAttrLen       = 256
	MaxGroupBy       = 3
)

const (
	tblEvents     = "events_normalized_distributed"
	tblSourceHr   = "source_metrics_hourly"
	tblQuality    = "processing_quality_daily"
	tblProcErrors = "processing_errors_local"
	chTimeLayout  = "2006-01-02 15:04:05.000"
	chTimeType    = "DateTime64(3,'UTC')"
)

// Query is a parameterized ClickHouse statement. SQL contains only
// whitelisted identifiers and {name:Type} placeholders; every caller-supplied
// value (and the tenant) lives in Params. Params are sent as param_<name>.
type Query struct {
	SQL    string
	Params map[string]string
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func isUUID(s string) bool { return uuidRe.MatchString(s) }

type TimeRange struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

func (t TimeRange) validate() error {
	if t.From.IsZero() || t.To.IsZero() {
		return fmt.Errorf("time_range.from and time_range.to are required (RFC 3339)")
	}
	if !t.To.After(t.From) {
		return fmt.Errorf("time_range.to must be after time_range.from")
	}
	if t.To.Sub(t.From) > MaxRange {
		return fmt.Errorf("time_range must not exceed %d days", int(MaxRange.Hours()/24))
	}
	return nil
}

// EventFilters are the structured, whitelisted event filters.
type EventFilters struct {
	SourceIDs         []string          `json:"source_ids,omitempty"`
	EventTypes        []string          `json:"event_types,omitempty"`
	EntityID          string            `json:"entity_id,omitempty"`
	ProcessingVersion *uint32           `json:"processing_version,omitempty"`
	Attributes        map[string]string `json:"attributes,omitempty"`
}

func (f EventFilters) validate() error {
	if len(f.SourceIDs) > MaxInValues || len(f.EventTypes) > MaxInValues {
		return fmt.Errorf("at most %d source_ids / event_types", MaxInValues)
	}
	for _, s := range f.SourceIDs {
		if !isUUID(s) {
			return fmt.Errorf("source_ids must be UUIDs")
		}
	}
	for _, s := range f.EventTypes {
		if s == "" || len(s) > MaxAttrLen {
			return fmt.Errorf("event_types entries must be 1..%d bytes", MaxAttrLen)
		}
	}
	if len(f.EntityID) > MaxAttrLen {
		return fmt.Errorf("entity_id too long")
	}
	if len(f.Attributes) > MaxAttributeKeys {
		return fmt.Errorf("at most %d attribute filters", MaxAttributeKeys)
	}
	for k, v := range f.Attributes {
		if k == "" || len(k) > MaxAttrLen || len(v) > MaxAttrLen {
			return fmt.Errorf("attribute filter key/value too long")
		}
	}
	return nil
}

type EventsRequest struct {
	TimeRange      TimeRange    `json:"time_range"`
	Filters        EventFilters `json:"filters"`
	Order          string       `json:"order,omitempty"` // "desc" (default) | "asc"
	IncludePayload bool         `json:"include_payload,omitempty"`
	Limit          int          `json:"limit,omitempty"`
	Cursor         string       `json:"cursor,omitempty"`
}

// queryHash binds a cursor to the query shape (everything except paging).
func (r EventsRequest) queryHash() string {
	return hashOf(struct {
		T TimeRange
		F EventFilters
		O string
		P bool
	}{r.TimeRange, r.Filters, r.Order, r.IncludePayload})
}

// Normalize validates and applies defaults.
func (r *EventsRequest) Normalize() error {
	if err := r.TimeRange.validate(); err != nil {
		return err
	}
	if err := r.Filters.validate(); err != nil {
		return err
	}
	switch r.Order {
	case "":
		r.Order = "desc"
	case "asc", "desc":
	default:
		return fmt.Errorf("order must be asc or desc")
	}
	if r.Limit == 0 {
		r.Limit = DefaultLimit
	}
	if r.Limit < 0 || r.Limit > MaxEventsLimit {
		return fmt.Errorf("limit must be between 1 and %d", MaxEventsLimit)
	}
	r.TimeRange.From = r.TimeRange.From.UTC()
	r.TimeRange.To = r.TimeRange.To.UTC()
	return nil
}

// Keyset is the position after which the next page starts.
type Keyset struct {
	Time time.Time
	ID   string
}

type qb struct {
	where  []string
	params map[string]string
}

// newQB always starts with the tenant predicate; there is no way to build a
// query without it.
func newQB(tenant string) *qb {
	return &qb{
		where:  []string{"tenant_id = {tenant_id:UUID}"},
		params: map[string]string{"tenant_id": tenant},
	}
}

func (b *qb) add(cond string) { b.where = append(b.where, cond) }

func (b *qb) timeRange(col string, tr TimeRange) {
	b.add(col + " >= {t_from:" + chTimeType + "} AND " + col + " < {t_to:" + chTimeType + "}")
	b.params["t_from"] = tr.From.UTC().Format(chTimeLayout)
	b.params["t_to"] = tr.To.UTC().Format(chTimeLayout)
}

func (b *qb) in(col, prefix, typ string, vals []string) {
	if len(vals) == 0 {
		return
	}
	ph := make([]string, len(vals))
	for i, v := range vals {
		n := fmt.Sprintf("%s%d", prefix, i)
		ph[i] = "{" + n + ":" + typ + "}"
		b.params[n] = v
	}
	b.add(col + " IN (" + strings.Join(ph, ",") + ")")
}

func (b *qb) eventFilters(f EventFilters) {
	b.in("source_id", "sid", "UUID", f.SourceIDs)
	b.in("event_type", "et", "String", f.EventTypes)
	if f.EntityID != "" {
		b.add("entity_id = {entity_id:String}")
		b.params["entity_id"] = f.EntityID
	}
	if f.ProcessingVersion != nil {
		b.add("processing_version = {pv:UInt32}")
		b.params["pv"] = fmt.Sprint(*f.ProcessingVersion)
	}
	// deterministic order for stable SQL
	keys := make([]string, 0, len(f.Attributes))
	for k := range f.Attributes {
		keys = append(keys, k)
	}
	sortStrings(keys)
	for i, k := range keys {
		kn, vn := fmt.Sprintf("ak%d", i), fmt.Sprintf("av%d", i)
		b.add("attributes[{" + kn + ":String}] = {" + vn + ":String}")
		b.params[kn], b.params[vn] = k, f.Attributes[k]
	}
}

func (b *qb) whereSQL() string { return strings.Join(b.where, " AND ") }

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// BuildEventsQuery builds the tenant-scoped event search. It fetches Limit+1
// rows so the caller can detect a next page. Reads are deduplicated by
// (tenant, source, event_id) keeping the highest processing_version (see
// db/clickhouse/migrations/0001: readers never rely on merge-time dedup).
func BuildEventsQuery(tenant string, r EventsRequest, after *Keyset) Query {
	b := newQB(tenant)
	b.timeRange("event_time", r.TimeRange)
	b.eventFilters(r.Filters)
	dir, cmp := "DESC", "<"
	if r.Order == "asc" {
		dir, cmp = "ASC", ">"
	}
	if after != nil {
		b.add("(event_time, event_id) " + cmp + " ({c_time:" + chTimeType + "}, {c_id:UUID})")
		b.params["c_time"] = after.Time.UTC().Format(chTimeLayout)
		b.params["c_id"] = after.ID
	}
	cols := "event_id, source_id, batch_id, processing_version, event_type, event_time, ingested_at, entity_id, attributes, quality_flags"
	if r.IncludePayload {
		cols += ", payload"
	}
	b.params["limit"] = fmt.Sprint(r.Limit + 1)
	return Query{
		SQL: "SELECT " + cols + " FROM " + tblEvents + " WHERE " + b.whereSQL() +
			" ORDER BY event_time " + dir + ", event_id " + dir + ", processing_version DESC" +
			" LIMIT 1 BY tenant_id, source_id, event_id LIMIT {limit:UInt32}",
		Params: b.params,
	}
}

// Whitelists. Keys are the API names; values are fixed SQL fragments.
var groupByExpr = map[string]string{
	"source_id":          "source_id",
	"event_type":         "event_type",
	"processing_version": "processing_version",
	"day":                "toDate(event_time, 'UTC')",
	"hour":               "toStartOfHour(event_time, 'UTC')",
}

var metricExpr = map[string]string{
	"events":           "uniqExact(event_id)",
	"entities":         "uniqExactIf(entity_id, entity_id != '')",
	"first_event_time": "min(event_time)",
	"last_event_time":  "max(event_time)",
}

type AggregateRequest struct {
	TimeRange TimeRange    `json:"time_range"`
	Filters   EventFilters `json:"filters"`
	GroupBy   []string     `json:"group_by,omitempty"`
	Metrics   []string     `json:"metrics,omitempty"`
	Limit     int          `json:"limit,omitempty"`
}

func (r *AggregateRequest) Normalize() error {
	if err := r.TimeRange.validate(); err != nil {
		return err
	}
	if err := r.Filters.validate(); err != nil {
		return err
	}
	if len(r.GroupBy) > MaxGroupBy {
		return fmt.Errorf("at most %d group_by fields", MaxGroupBy)
	}
	seen := map[string]bool{}
	for _, g := range r.GroupBy {
		if _, ok := groupByExpr[g]; !ok || seen[g] {
			return fmt.Errorf("unsupported or duplicate group_by %q", g)
		}
		seen[g] = true
	}
	if len(r.Metrics) == 0 {
		r.Metrics = []string{"events"}
	}
	seen = map[string]bool{}
	for _, m := range r.Metrics {
		if _, ok := metricExpr[m]; !ok || seen[m] {
			return fmt.Errorf("unsupported or duplicate metric %q", m)
		}
		seen[m] = true
	}
	if r.Limit == 0 {
		r.Limit = DefaultLimit
	}
	if r.Limit < 0 || r.Limit > MaxAggLimit {
		return fmt.Errorf("limit must be between 1 and %d", MaxAggLimit)
	}
	r.TimeRange.From = r.TimeRange.From.UTC()
	r.TimeRange.To = r.TimeRange.To.UTC()
	return nil
}

// BuildAggregateQuery expects a Normalize()d request (names are whitelisted
// again defensively: unknown names panic rather than reach SQL).
func BuildAggregateQuery(tenant string, r AggregateRequest) Query {
	b := newQB(tenant)
	b.timeRange("event_time", r.TimeRange)
	b.eventFilters(r.Filters)
	var sel, grp []string
	for _, g := range r.GroupBy {
		e, ok := groupByExpr[g]
		if !ok {
			panic("query-api: unwhitelisted group_by " + g)
		}
		sel = append(sel, e+" AS g_"+g)
		grp = append(grp, "g_"+g)
	}
	for _, m := range r.Metrics {
		e, ok := metricExpr[m]
		if !ok {
			panic("query-api: unwhitelisted metric " + m)
		}
		sel = append(sel, e+" AS m_"+m)
	}
	b.params["limit"] = fmt.Sprint(r.Limit + 1)
	sql := "SELECT " + strings.Join(sel, ", ") + " FROM " + tblEvents + " WHERE " + b.whereSQL()
	if len(grp) > 0 {
		sql += " GROUP BY " + strings.Join(grp, ", ") + " ORDER BY " + strings.Join(grp, ", ")
	}
	return Query{SQL: sql + " LIMIT {limit:UInt32}", Params: b.params}
}
