package agg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

// Partition is the content fingerprint of one (tenant, source, event_date, processing_version) partition: the
// de-duplicated row count, an order-independent checksum and the newest ingestion time (the "revision": any legitimate
// write moves it or the count).
type Partition struct {
	TenantID          string `json:"tenant_id"`
	SourceID          string `json:"source_id"`
	EventDate         string `json:"event_date"`
	ProcessingVersion uint32 `json:"processing_version"`
	Events            uint64 `json:"events,string"`
	Checksum          uint64 `json:"checksum,string"`
	MaxIngestedMs     int64  `json:"max_ingested_ms,string"`
}

func (p Partition) key() string {
	return fmt.Sprintf("%s/%s/%s/%d", p.TenantID, p.SourceID, p.EventDate, p.ProcessingVersion)
}

// Snapshot is the comparable output of one checksum run (JSON).
type Snapshot struct {
	Version    int         `json:"version"`
	TakenAt    time.Time   `json:"taken_at"`
	From       time.Time   `json:"window_from"`
	To         time.Time   `json:"window_to"`
	Truncated  bool        `json:"truncated,omitempty"`
	Partitions []Partition `json:"partitions"`
}

// DayWindow widens a window outward to whole UTC days so partitions are always complete and comparable across runs.
func DayWindow(w Window) Window {
	f, t := w.From.UTC().Truncate(24*time.Hour), w.To.UTC()
	if d := t.Truncate(24 * time.Hour); d.Before(t) {
		t = d.Add(24 * time.Hour)
	}
	return Window{f, t}
}

// ChecksumSQL fingerprints de-duplicated events per partition. cityHash64 sums are order independent (and independent
// of which duplicate LIMIT 1 BY keeps, as duplicates are identical).
func (c *Client) ChecksumSQL(w Window, limit int) string {
	return fmt.Sprintf(`SELECT toString(tenant_id) AS tenant_id, toString(source_id) AS source_id, toString(toDate(event_time, 'UTC')) AS event_date, processing_version,
 count() AS events, sum(cityHash64(event_id, toString(event_time), toString(batch_id), event_type, entity_id, payload)) AS checksum,
 toUnixTimestamp64Milli(max(ingested_at)) AS max_ingested_ms
FROM (%s) GROUP BY tenant_id, source_id, event_date, processing_version
ORDER BY tenant_id, source_id, event_date, processing_version LIMIT %d FORMAT JSONEachRow`, c.dedupEvents(w), limit)
}

// Checksums returns the partition fingerprints of the (day aligned) window.
func (c *Client) Checksums(ctx context.Context, w Window, limit int, now time.Time) (Snapshot, error) {
	w = DayWindow(w)
	ps, err := queryWith[Partition](ctx, c, c.ChecksumSQL(w, limit), c.queryLimits())
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Version: 1, TakenAt: now.UTC(), From: w.From, To: w.To, Truncated: len(ps) >= limit, Partitions: ps}, nil
}

// WriteSnapshot stores a snapshot as JSON; "-" is not handled here (callers write to stdout themselves).
func WriteSnapshot(path string, s Snapshot) error {
	b, err := json.MarshalIndent(s, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

// ReadSnapshot loads a snapshot written by WriteSnapshot.
func ReadSnapshot(path string) (Snapshot, error) {
	var s Snapshot
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, err
	}
	if s.Version != 1 {
		return s, fmt.Errorf("unsupported snapshot version %d", s.Version)
	}
	return s, nil
}

// ChecksumChange is a partition whose fingerprint differs between two snapshots.
type ChecksumChange struct {
	Partition
	PrevEvents   uint64 `json:"prev_events"`
	PrevChecksum uint64 `json:"prev_checksum"`
	// Silent is true when the checksum changed but neither the count nor the newest ingestion time did: nothing
	// legitimate wrote to the partition, so this is a corruption candidate (fails the run).
	Silent bool `json:"silent"`
}

// CompareResult lists differences between a previous and the current snapshot over their common day range.
type CompareResult struct {
	Changes []ChecksumChange // checksum changed (Silent ones fail the run)
	Missing []Partition      // in previous, absent now (informational: TTL/partition drop or data loss)
	Added   int              // new partitions (informational)
}

// SilentCount is the number of silent-corruption candidates.
func (r CompareResult) SilentCount() int {
	n := 0
	for _, c := range r.Changes {
		if c.Silent {
			n++
		}
	}
	return n
}

// CompareSnapshots diffs prev against cur, considering only partitions whose date lies in both windows.
func CompareSnapshots(prev, cur Snapshot) CompareResult {
	lo, hi := prev.From, prev.To
	if cur.From.After(lo) {
		lo = cur.From
	}
	if cur.To.Before(hi) {
		hi = cur.To
	}
	inRange := func(date string) bool {
		d, err := time.Parse("2006-01-02", date)
		return err == nil && !d.Before(lo) && d.Before(hi)
	}
	curM := map[string]Partition{}
	for _, p := range cur.Partitions {
		curM[p.key()] = p
	}
	var r CompareResult
	prevKeys := map[string]bool{}
	for _, p := range prev.Partitions {
		if !inRange(p.EventDate) {
			continue
		}
		prevKeys[p.key()] = true
		n, ok := curM[p.key()]
		switch {
		case !ok:
			r.Missing = append(r.Missing, p)
		case n.Checksum != p.Checksum:
			r.Changes = append(r.Changes, ChecksumChange{Partition: n, PrevEvents: p.Events, PrevChecksum: p.Checksum,
				Silent: n.Events == p.Events && n.MaxIngestedMs == p.MaxIngestedMs})
		}
	}
	for _, p := range cur.Partitions {
		if inRange(p.EventDate) && !prevKeys[p.key()] {
			r.Added++
		}
	}
	sort.SliceStable(r.Changes, func(i, j int) bool { return r.Changes[i].Silent && !r.Changes[j].Silent })
	return r
}
