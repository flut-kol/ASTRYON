package sink

import (
	"expvar"
	"fmt"
	"io"
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

// WritePrometheus renders the counters in Prometheus text exposition format.
func (m *Metrics) WritePrometheus(w io.Writer) {
	for _, c := range []struct {
		name, help, typ string
		v               int64
	}{
		{"consumed_total", "Kafka messages consumed", "counter", m.Consumed.Load()},
		{"inserted_rows_total", "Rows inserted into ClickHouse", "counter", m.Inserted.Load()},
		{"quarantined_total", "Messages routed to quarantine", "counter", m.Quarantined.Load()},
		{"batches_total", "Insert batches built", "counter", m.Batches.Load()},
		{"insert_retries_total", "ClickHouse insert retries", "counter", m.InsertRetries.Load()},
		{"insert_failures_total", "ClickHouse insert failures", "counter", m.InsertFailures.Load()},
		{"commits_total", "Kafka offset commits", "counter", m.Commits.Load()},
		{"commit_failures_total", "Kafka offset commit failures", "counter", m.CommitFailures.Load()},
		{"buffered", "Messages buffered, not yet committed", "gauge", m.Buffered.Load()},
	} {
		fmt.Fprintf(w, "# HELP astryon_clickhouse_sink_%s %s\n# TYPE astryon_clickhouse_sink_%s %s\nastryon_clickhouse_sink_%s %d\n",
			c.name, c.help, c.name, c.typ, c.name, c.v)
	}
}
