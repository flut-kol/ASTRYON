package cdc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Op is a Debezium operation code.
type Op string

const (
	OpCreate   Op = "c"
	OpUpdate   Op = "u"
	OpDelete   Op = "d"
	OpSnapshot Op = "r"
)

// ErrMalformed marks a change event that cannot be mapped; it is quarantined, never retried.
var ErrMalformed = errors.New("cdc: malformed change event")

// Source is the Debezium source block. Only position-like fields are used; identity of tenant/source never comes from it.
type Source struct {
	Connector string `json:"connector"`
	DB        string `json:"db"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	TsMs      int64  `json:"ts_ms"`
	Snapshot  any    `json:"snapshot"` // "true"/"false"/"last" or bool
	LSN       *int64 `json:"lsn"`      // PostgreSQL
	TxID      *int64 `json:"txId"`
	File      string `json:"file"` // MySQL binlog
	Pos       *int64 `json:"pos"`
	Row       *int64 `json:"row"`
	GTID      string `json:"gtid"`
}

// Change is a parsed Debezium JSON change event (envelope, optionally wrapped in {"schema","payload"}).
type Change struct {
	Op     Op
	Before map[string]json.RawMessage
	After  map[string]json.RawMessage
	Source Source
	TsMs   int64
}

type wireChange struct {
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Source *Source         `json:"source"`
	Op     string          `json:"op"`
	TsMs   int64           `json:"ts_ms"`
}

// ParseChange decodes a Debezium value. Unknown ops, a missing source or missing row images are ErrMalformed.
func ParseChange(value []byte) (Change, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(value, &top); err != nil || top == nil {
		return Change{}, fmt.Errorf("%w: not a JSON object", ErrMalformed)
	}
	if p, ok := top["payload"]; ok && top["op"] == nil {
		value = p
	}
	var w wireChange
	if err := json.Unmarshal(value, &w); err != nil {
		return Change{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if w.Source == nil {
		return Change{}, fmt.Errorf("%w: missing source", ErrMalformed)
	}
	c := Change{Op: Op(w.Op), Source: *w.Source, TsMs: w.TsMs}
	var err error
	if c.Before, err = image(w.Before); err != nil {
		return Change{}, err
	}
	if c.After, err = image(w.After); err != nil {
		return Change{}, err
	}
	switch c.Op {
	case OpCreate, OpUpdate, OpSnapshot:
		if c.After == nil {
			return Change{}, fmt.Errorf("%w: op %q without after image", ErrMalformed, w.Op)
		}
	case OpDelete:
		if c.Before == nil {
			return Change{}, fmt.Errorf("%w: delete without before image", ErrMalformed)
		}
	default:
		return Change{}, fmt.Errorf("%w: unsupported op %q", ErrMalformed, w.Op)
	}
	if c.Source.Table == "" {
		return Change{}, fmt.Errorf("%w: missing source.table", ErrMalformed)
	}
	return c, nil
}

func image(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: row image: %v", ErrMalformed, err)
	}
	return m, nil
}
