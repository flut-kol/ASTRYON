package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	ec "github.com/astryon/event-contracts"
)

var ErrBadResponse = errors.New("runner: response is not a JSON array of records")

// Mapping turns a pulled page into event envelopes. IDField gives record identity (the same record keeps its event_id
// across pages and retries); without it identity is the SHA-256 of the record, so an unchanged record is never duplicated.
type Mapping struct {
	SchemaID          string
	SchemaVersion     int
	RecordsPath       string // top-level key holding the array; empty = the body itself is the array
	IDField           string
	EventTimeField    string // RFC 3339 text or Unix seconds/milliseconds; missing -> event_time_missing=true
	EntityField       string
	ProcessingVersion int
}

// MapPage returns marshalled envelopes and the number of non-object records that were skipped.
func MapPage(body []byte, m Mapping, tenant, source, connectorID string, now time.Time) (events [][]byte, skipped int, err error) {
	var records []json.RawMessage
	if m.RecordsPath == "" {
		if json.Unmarshal(body, &records) != nil {
			return nil, 0, ErrBadResponse
		}
	} else {
		var obj map[string]json.RawMessage
		if json.Unmarshal(body, &obj) != nil || json.Unmarshal(obj[m.RecordsPath], &records) != nil {
			return nil, 0, ErrBadResponse
		}
	}
	for _, raw := range records {
		var doc map[string]any
		if json.Unmarshal(raw, &doc) != nil || doc == nil {
			skipped++
			continue
		}
		env := ec.Envelope{
			EventID: eventID(connectorID, m.IDField, doc, raw), TenantID: tenant, SourceID: source,
			SchemaID: m.SchemaID, SchemaVersion: m.SchemaVersion, IngestedAt: now.UTC(), TraceID: "pull-" + connectorID,
			PayloadFormat: ec.FormatJSON, Payload: raw, ProcessingVersion: m.ProcessingVersion,
		}
		if t, ok := parseTime(doc[m.EventTimeField]); ok {
			u := t.UTC()
			env.EventTime = &u
		} else {
			env.EventTimeMissing = true
		}
		env.EntityKey = scalar(doc[m.EntityField])
		if err := env.ValidateAt(now); err != nil {
			skipped++ // e.g. event time too far in the future: never publish a malformed envelope
			continue
		}
		b, _ := json.Marshal(env)
		events = append(events, b)
	}
	return events, skipped, nil
}

func scalar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return ""
}

// eventID is a stable UUID (v5-shaped) of connector + record identity.
func eventID(connector, idField string, doc map[string]any, raw []byte) string {
	ident := ""
	if idField != "" {
		ident = scalar(doc[idField])
	}
	if ident == "" {
		sum := sha256.Sum256(raw)
		ident = "sha256:" + hex.EncodeToString(sum[:])
	} else {
		ident = "id:" + ident
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("pull:%s:%s", connector, ident)))
	b := h[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	x := hex.EncodeToString(b)
	return x[0:8] + "-" + x[8:12] + "-" + x[12:16] + "-" + x[16:20] + "-" + x[20:]
}

func parseTime(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return t, true
		}
	case float64:
		if x > 0 && x < 1e11 {
			return time.Unix(int64(x), 0), true
		}
		if x >= 1e11 && x < 1e14 {
			return time.UnixMilli(int64(x)), true
		}
	}
	return time.Time{}, false
}
