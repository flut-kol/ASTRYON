// Package sink consumes processed events and inserts them into ClickHouse idempotently.
package sink

import "context"

// Message is one consumed Kafka record.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Source is a pull-based message source (Kafka in production). Poll must return
// (nil, nil) when the context deadline expires with nothing to deliver; it returns
// an error only for fatal problems. Not polling is the backpressure mechanism.
type Source interface {
	Poll(ctx context.Context) ([]Message, error)
	// Commit marks all given messages (and everything before them per partition) as processed.
	Commit(ctx context.Context, msgs []Message) error
	Close()
}

// Chunk is one ClickHouse insert: rows of a single topic/partition offset range.
type Chunk struct {
	Token       string // deterministic insert_deduplication_token
	Topic       string
	Partition   int32
	FirstOffset int64
	LastOffset  int64
	Rows        [][]byte // one JSON object per row (JSONEachRow)
}

// Sink inserts a chunk. Implementations must be safe to call again with an identical chunk.
type Sink interface {
	Insert(ctx context.Context, c Chunk) error
}

// Quarantined describes a message that could not be mapped to a row.
type Quarantined struct {
	Msg    Message
	Code   string
	Reason string
}

// Quarantiner publishes rejected messages (events.quarantine.v1).
type Quarantiner interface {
	Quarantine(ctx context.Context, q []Quarantined) error
}
