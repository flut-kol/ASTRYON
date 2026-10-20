package sink

import (
	"expvar"
	"sync/atomic"
)

// Metrics are plain atomic counters, published through expvar.
type Metrics struct {
	Consumed, Inserted, Quarantined, Batches, InsertRetries, InsertFailures, Commits, CommitFailures atomic.Int64
	Buffered                                                                                         atomic.Int64
}

// Publish registers the counters under expvar name "clickhouse_sink".
func (m *Metrics) Publish() {
	expvar.Publish("clickhouse_sink", expvar.Func(func() any {
		return map[string]int64{
			"consumed": m.Consumed.Load(), "inserted_rows": m.Inserted.Load(), "quarantined": m.Quarantined.Load(),
			"batches": m.Batches.Load(), "insert_retries": m.InsertRetries.Load(), "insert_failures": m.InsertFailures.Load(),
			"commits": m.Commits.Load(), "commit_failures": m.CommitFailures.Load(), "buffered": m.Buffered.Load(),
		}
	}))
}
