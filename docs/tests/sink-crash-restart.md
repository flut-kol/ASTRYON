# clickhouse-sink crash/restart test

Run 2026-10-20, throwaway compose project `chagg-crash` (kafka 19592, clickhouse 18524), torn down afterwards.
Setup: 30 000 distinct events produced to `processed.events.v1` (6 partitions), sink built from `services/clickhouse-sink`,
single-node ClickHouse 24.8 with migrations applied, table `events_normalized_local`.

| Run | Procedure | Result |
|---|---|---|
| 1 | batch 700 / 300 ms, 3 × `kill -9` after ~2 s, then run to completion | count == uniqExact(event_id) == 30 000 after every kill and at the end; group lag 0 on all 6 partitions |
| 2 | new group, `TRUNCATE`, batch 150 / 100 ms, 25 × `kill -9` after random 0.2–0.8 s, then run to completion | count == uniqExact == 30 000 after every kill and at the end |

Conclusion: no loss and no duplicate rows were observed across restarts.

Limits (do not over-read this result):
- The sink drains 30 000 events in a few seconds, so most of the 25 kills landed after the data was already consumed.
  The window "insert succeeded, offset not yet committed" was not shown to be hit; token replay after such a kill
  is covered only by `TestIntegrationDedupToken*` (same token twice), not by a proven mid-flight crash.
- A restart re-cuts chunks from the committed offset, so boundaries may differ from the lost in-flight chunk and the
  dedup token then does not match. Not reproduced by the kill test, but proven deterministically with fault injection
  (`internal/sink/crash_test.go`): a crash after insert and before commit followed by redelivery with the same
  boundaries stores no duplicates; with different boundaries (6 rows, then 10 overlapping) 16 rows are stored for
  10 unique events. Nothing is lost. Exact readers (`LIMIT 1 BY`) and the aggregates are unaffected, and
  `ch-aggregator reconcile` reports the drift. Fix needs durable chunk boundaries (`sink_checkpoints.insert_token`, 0001).
- Single broker, single node, plaintext, no TLS/SASL.

## Durable chunk boundaries (opt-in)

Set `CHSINK_INTENT_TABLE=sink_chunk_intents_local` (migration `0004`) to record each chunk's offset range before it is
inserted. After a crash between insert and offset commit, the restarted sink looks up the latest recorded range of a
partition whose buffered offsets do not continue from its last commit and re-cuts the first chunk to exactly that range:
the token matches and ClickHouse drops the block. If the redelivery stops short of the recorded range the flush is held
for up to `CHSINK_INTENT_WAIT` (default 30 s) and the range is then abandoned (rows may be stored twice; readers dedup).
Costs one small ClickHouse insert per chunk and one lookup per partition after a restart/rebalance.

Tests: `crash_test.go` (6 rows delivered, crash before commit, redelivery as 10 rows: 10 rows stored, was 16; hold and
abandon path), `TestIntegrationIntentStore` (live ClickHouse 24.8, 64-bit offsets round-trip). End-to-end on real Kafka and
ClickHouse: 6 000 events consumed with intents enabled, 6 000 rows, intent rows recorded per chunk. Not proven: a real
`kill -9` inside the insert→commit window with the feature on (the window is too narrow to hit reliably; covered by
the fault-injection test instead).

### Throughput cost of durable boundaries (2026-10-20, dev host, single Kafka + single ClickHouse)

Backlog of 200 000 events (~460 B each) drained by one sink, batch 5000 / 2 s, group offsets from earliest, tables
truncated between runs, two repetitions per mode:

| Mode | Run 1 | Run 2 |
|---|---|---|
| intents off | 29 844 ev/s (6.7 s) | 30 387 ev/s (6.6 s) |
| intents on (121 intent rows) | 32 023 ev/s (6.2 s) | 29 830 ev/s (6.7 s) |

No measurable difference at this batch size (one extra tiny insert per ~1.6 k-row chunk). The small-batch worst case
(batch 200 / 300 ms) was not measured. Numbers are from a laptop-class Docker host and a drain, not a steady-state
latency test; do not compare with the DPP-010 gateway benchmark.
