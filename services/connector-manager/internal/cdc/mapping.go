package cdc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	ec "github.com/astryon/event-contracts"
)

// MapConfig comes from the connector row only. Tenant and Source are trusted; nothing in a payload can override them.
type MapConfig struct {
	ConnectorID       string
	Tenant            string
	Source            string
	SchemaID          string
	SchemaVersion     int
	ProcessingVersion int
	PrimaryKey        []string // PK columns; empty = use the Kafka record key (Debezium puts the PK there)
}

// Validate rejects a configuration that would make every event invalid, so that misconfiguration fails loudly
// instead of quarantining the whole stream.
func (m MapConfig) Validate(now time.Time) error {
	if m.ConnectorID == "" {
		return errors.New("cdc: connector id is required")
	}
	probe := ec.Envelope{EventID: EventID("probe", Change{}, ""), TenantID: m.Tenant, SourceID: m.Source, SchemaID: m.SchemaID,
		SchemaVersion: m.SchemaVersion, IngestedAt: now.UTC(), TraceID: "cdc-probe", PayloadFormat: ec.FormatJSON,
		Payload: json.RawMessage(`{}`), ProcessingVersion: m.ProcessingVersion, EventTimeMissing: true}
	if err := probe.ValidateAt(now); err != nil {
		return fmt.Errorf("cdc: invalid connector mapping config: %w", err)
	}
	return nil
}

// Position renders the source position used for event identity. Snapshot rows have no log position, so the
// snapshot timestamp (constant for one snapshot run) stands in.
func (s Source) Position() string {
	p := func(v *int64) string {
		if v == nil {
			return ""
		}
		return fmt.Sprint(*v)
	}
	if s.LSN == nil && s.Pos == nil && s.GTID == "" {
		return fmt.Sprintf("snap:%d", s.TsMs)
	}
	return fmt.Sprintf("lsn=%s;tx=%s;file=%s;pos=%s;row=%s;gtid=%s", p(s.LSN), p(s.TxID), s.File, p(s.Pos), p(s.Row), s.GTID)
}

// Map converts one change into a marshalled envelope. key is the Kafka record key (JSON) used when PrimaryKey is unset.
// Deletes become tombstone-style events: payload {"op":"d","deleted":true,"table":..,"key":{..}} with no row image.
func Map(c Change, key []byte, m MapConfig, now time.Time) ([]byte, error) {
	pk, err := primaryKey(c, key, m.PrimaryKey)
	if err != nil {
		return nil, err
	}
	entity := entityKey(pk)
	body := map[string]any{"op": string(c.Op), "table": c.Source.Table, "key": pk}
	if c.Source.Schema != "" {
		body["schema"] = c.Source.Schema
	}
	if c.Op == OpDelete {
		body["deleted"] = true
	} else {
		body["after"] = c.After
	}
	payload, _ := json.Marshal(body)
	env := ec.Envelope{
		EventID: EventID(m.ConnectorID, c, entity), TenantID: m.Tenant, SourceID: m.Source,
		SchemaID: m.SchemaID, SchemaVersion: m.SchemaVersion, IngestedAt: now.UTC(), EntityKey: entity,
		TraceID: "cdc-" + m.ConnectorID, PayloadFormat: ec.FormatJSON, Payload: payload, ProcessingVersion: m.ProcessingVersion,
	}
	ts := c.TsMs
	if ts == 0 {
		ts = c.Source.TsMs
	}
	if ts > 0 {
		t := time.UnixMilli(ts).UTC()
		env.EventTime = &t
	} else {
		env.EventTimeMissing = true
	}
	if err := env.ValidateAt(now); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return json.Marshal(env)
}

// EventID is a v5-shaped UUID of connector + source position + table + key + op, so a redelivered change dedups.
func EventID(connector string, c Change, entity string) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("cdc:%s:%s:%s:%s:%s", connector, c.Source.Position(), c.Source.Table, entity, c.Op)))
	b := h[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	x := hex.EncodeToString(b)
	return x[0:8] + "-" + x[8:12] + "-" + x[12:16] + "-" + x[16:20] + "-" + x[20:]
}

func primaryKey(c Change, key []byte, cols []string) (map[string]json.RawMessage, error) {
	img := c.After
	if c.Op == OpDelete {
		img = c.Before
	}
	pk := map[string]json.RawMessage{}
	if len(cols) > 0 {
		for _, col := range cols {
			v, ok := img[col]
			if !ok || string(v) == "null" {
				return nil, fmt.Errorf("%w: primary key column %q missing", ErrMalformed, col)
			}
			pk[col] = v
		}
		return pk, nil
	}
	var k map[string]json.RawMessage
	if json.Unmarshal(key, &k) != nil || k == nil {
		return nil, fmt.Errorf("%w: no primary_key configured and record key is not a JSON object", ErrMalformed)
	}
	if p, hasP := k["payload"]; hasP && k["schema"] != nil && len(k) == 2 { // schema-wrapped key
		var inner map[string]json.RawMessage
		if json.Unmarshal(p, &inner) != nil || inner == nil {
			return nil, fmt.Errorf("%w: unreadable record key", ErrMalformed)
		}
		k = inner
	}
	if len(k) == 0 {
		return nil, fmt.Errorf("%w: empty primary key", ErrMalformed)
	}
	return k, nil
}

// entityKey joins PK values in sorted column order, e.g. "42" or "a|b".
func entityKey(pk map[string]json.RawMessage) string {
	names := make([]string, 0, len(pk))
	for n := range pk {
		names = append(names, n)
	}
	for i := 1; i < len(names); i++ { // tiny insertion sort, no extra import
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	parts := make([]string, 0, len(names))
	for _, n := range names {
		var s string
		if json.Unmarshal(pk[n], &s) == nil {
			parts = append(parts, s)
		} else {
			parts = append(parts, strings.TrimSpace(string(pk[n])))
		}
	}
	return strings.Join(parts, "|")
}
