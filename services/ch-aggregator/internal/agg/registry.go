package agg

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// RegBatch is one row of the primary registry (PostgreSQL ingestion_batches): aggregates only, never per event.
type RegBatch struct {
	TenantID, SourceID, BatchID string
	Status                      string // OPEN SEALED PROCESSING COMPLETED FAILED CANCELLED
	Accepted, Rejected          int64
	CreatedAt, UpdatedAt        time.Time
}

// Registry is the read-only view of the primary registry needed by the reconciliation.
type Registry interface {
	// BatchesCreated returns up to limit batches created in [from, to).
	BatchesCreated(ctx context.Context, from, to time.Time, limit int) ([]RegBatch, error)
	// BatchesByID returns the registry rows of the given batch ids (unknown ids are simply absent).
	BatchesByID(ctx context.Context, ids []string) ([]RegBatch, error)
}

// CHBatchCount is the de-duplicated event count of one (tenant, source, batch, processing_version) in ClickHouse.
type CHBatchCount struct {
	TenantID          string `json:"tenant_id"`
	SourceID          string `json:"source_id"`
	BatchID           string `json:"batch_id"`
	ProcessingVersion uint32 `json:"processing_version"`
	Events            uint64 `json:"events,string"`
	FirstIngestedMs   int64  `json:"first_ingested_ms,string"`
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// queryLimits bound every reconcile query on the server side.
func (c *Client) queryLimits() map[string]string {
	t := c.QueryTimeout
	if t <= 0 {
		t = 120 * time.Second
	}
	return map[string]string{"max_execution_time": fmt.Sprint(int(t.Seconds())), "max_memory_usage": "4000000000"}
}

// RegistryCountsSQL counts de-duplicated events per batch for every batch that received data in the window
// (ingested_at) plus the explicitly listed registry batch ids (so registry batches with no/late data show up as 0).
// The count covers the whole batch, not only the window. ids must be UUIDs.
func (c *Client) RegistryCountsSQL(w Window, ids []string, limit int) (string, error) {
	f, t := w.lit()
	cond := fmt.Sprintf(`batch_id IN (SELECT batch_id FROM %s WHERE ingested_at >= toDateTime64('%s', 3, 'UTC') AND ingested_at < toDateTime64('%s', 3, 'UTC'))`, c.Events, f, t)
	if len(ids) > 0 {
		lits := make([]string, len(ids))
		for i, id := range ids {
			if !uuidRe.MatchString(id) {
				return "", fmt.Errorf("invalid batch id %q", id)
			}
			lits[i] = "'" + id + "'"
		}
		cond += " OR batch_id IN (" + strings.Join(lits, ",") + ")"
	}
	return fmt.Sprintf(`SELECT toString(tenant_id) AS tenant_id, toString(source_id) AS source_id, toString(batch_id) AS batch_id, processing_version,
 count() AS events, toUnixTimestamp64Milli(min(ingested_at)) AS first_ingested_ms
FROM (SELECT tenant_id, source_id, batch_id, processing_version, event_id, ingested_at FROM %s WHERE %s
 LIMIT 1 BY tenant_id, source_id, event_id, processing_version)
GROUP BY tenant_id, source_id, batch_id, processing_version
ORDER BY tenant_id, source_id, batch_id, processing_version LIMIT %d FORMAT JSONEachRow`, c.Events, cond, limit), nil
}

// RegistryMismatch is one disagreement between the ClickHouse count and the registry accepted counter.
type RegistryMismatch struct {
	Kind              string `json:"kind"`  // missing (CH < accepted: data-loss candidate) | extra (CH > accepted: duplicates/foreign) | unregistered (batch not in registry)
	Class             string `json:"class"` // transient | persistent
	TenantID          string `json:"tenant_id"`
	SourceID          string `json:"source_id"`
	BatchID           string `json:"batch_id"`
	ProcessingVersion uint32 `json:"processing_version"`
	CHEvents          uint64 `json:"ch_events"`
	Accepted          int64  `json:"registry_accepted"`
	Status            string `json:"registry_status,omitempty"`
}

// RegistryResult is the outcome of the registry reconciliation.
type RegistryResult struct {
	Mismatches []RegistryMismatch
	Checked    int  // (batch, processing_version) pairs compared
	Truncated  bool // a bound (batch limit) was hit; the check is partial
}

// Persistent counts mismatches that are not explained by an open or young batch.
func (r RegistryResult) Persistent() int {
	n := 0
	for _, m := range r.Mismatches {
		if m.Class == "persistent" {
			n++
		}
	}
	return n
}

type batchKey struct{ t, s, b string }

// ReconcileRegistry compares ClickHouse de-duplicated counts per (tenant, source, batch, processing_version) with
// ingestion_batches.events_accepted. Open/sealed/processing batches and batches updated within grace are transient.
func (c *Client) ReconcileRegistry(ctx context.Context, reg Registry, w Window, now time.Time, grace time.Duration, limit int) (RegistryResult, error) {
	var res RegistryResult
	batches, err := reg.BatchesCreated(ctx, w.From, w.To, limit)
	if err != nil {
		return res, fmt.Errorf("registry: %w", err)
	}
	regMap := map[batchKey]RegBatch{}
	ids := make([]string, 0, len(batches))
	for _, b := range batches {
		regMap[batchKey{b.TenantID, b.SourceID, b.BatchID}] = b
		ids = append(ids, b.BatchID)
	}
	res.Truncated = len(batches) >= limit
	sql, err := c.RegistryCountsSQL(w, ids, limit)
	if err != nil {
		return res, err
	}
	counts, err := queryWith[CHBatchCount](ctx, c, sql, c.queryLimits())
	if err != nil {
		return res, fmt.Errorf("clickhouse batch counts: %w", err)
	}
	if len(counts) >= limit {
		res.Truncated = true
	}
	// Resolve ClickHouse batches that are not in the created-in-window set (older batches that received data now).
	var unknown []string
	seen := map[string]bool{}
	for _, n := range counts {
		if _, ok := regMap[batchKey{n.TenantID, n.SourceID, n.BatchID}]; !ok && !seen[n.BatchID] {
			seen[n.BatchID] = true
			unknown = append(unknown, n.BatchID)
		}
	}
	if len(unknown) > 0 {
		more, err := reg.BatchesByID(ctx, unknown)
		if err != nil {
			return res, fmt.Errorf("registry: %w", err)
		}
		for _, b := range more {
			regMap[batchKey{b.TenantID, b.SourceID, b.BatchID}] = b
		}
	}
	transient := func(b RegBatch) bool {
		switch b.Status {
		case "OPEN", "SEALED", "PROCESSING":
			return true
		}
		return now.Sub(b.UpdatedAt) < grace
	}
	have := map[batchKey]bool{}
	for _, n := range counts {
		k := batchKey{n.TenantID, n.SourceID, n.BatchID}
		have[k] = true
		res.Checked++
		b, ok := regMap[k]
		m := RegistryMismatch{TenantID: n.TenantID, SourceID: n.SourceID, BatchID: n.BatchID, ProcessingVersion: n.ProcessingVersion, CHEvents: n.Events}
		switch {
		case !ok:
			m.Kind, m.Class = "unregistered", "persistent"
			if now.Sub(time.UnixMilli(n.FirstIngestedMs)) < grace { // registry write may lag the sink
				m.Class = "transient"
			}
		case int64(n.Events) == b.Accepted:
			continue
		default:
			m.Accepted, m.Status = b.Accepted, b.Status
			m.Kind = "extra"
			if int64(n.Events) < b.Accepted {
				m.Kind = "missing"
			}
			m.Class = "persistent"
			if transient(b) {
				m.Class = "transient"
			}
		}
		res.Mismatches = append(res.Mismatches, m)
	}
	// Registry batches with accepted events but no ClickHouse rows at all.
	for _, b := range batches {
		k := batchKey{b.TenantID, b.SourceID, b.BatchID}
		if have[k] || b.Accepted <= 0 {
			continue
		}
		res.Checked++
		m := RegistryMismatch{Kind: "missing", Class: "persistent", TenantID: b.TenantID, SourceID: b.SourceID, BatchID: b.BatchID, Accepted: b.Accepted, Status: b.Status}
		if transient(b) {
			m.Class = "transient"
		}
		res.Mismatches = append(res.Mismatches, m)
	}
	sort.SliceStable(res.Mismatches, func(i, j int) bool {
		a, b := res.Mismatches[i], res.Mismatches[j]
		if a.Class != b.Class {
			return a.Class > b.Class // persistent first
		}
		return a.BatchID < b.BatchID
	})
	return res, nil
}
