# stream-processor (DPP-026 / DPP-027)

`raw.events.v1` -> validate -> normalize -> bounded dedup -> `processed.events.v1`;
contract violations -> `events.quarantine.v1`; unrecoverable -> `events.dlq.v1`.

## Deviation from plan: Go instead of Flink
DPP-026 names a Flink job with S3 checkpoints. Flink cannot be built in this
environment (no mvn/gradle), so the same semantics are implemented as a Go
franz-go consumer/processor. Core logic (`internal/processor/normalize.go`,
`dedup.go`) is pure and Kafka-free, so it can be ported to a Flink operator later.
Consequence: there are no Flink checkpoints/S3 state; see "State and recovery".

## Semantics
- Validate with `event-contracts` `Envelope.ValidateAt`. `tenant_id` is taken only from the
  envelope (bound to the validated token claims by the gateway); nothing is read from headers.
- Normalize: canonical ids, `event_time` defaulted to `ingested_at` when explicitly missing
  (flag `event_time_defaulted_to_ingested_at`), `quality_flags`, `pipeline_version`
  (`STREAMPROC_PIPELINE_VERSION`), `processing_version` kept, `raw_ref=kafka:<topic>/<partition>/<offset>`
  pointing to the original. Output is deterministic and is the format `clickhouse-sink` reads.
- Dedup key = `eventcontracts.DedupKey` (tenant, source, event_id, processing_version), so a replay
  into a new processing_version does not collide. State is a FIFO window bounded by TTL
  (`STREAMPROC_DEDUP_TTL`) AND max entries (`STREAMPROC_DEDUP_MAX_ENTRIES`); hits do not refresh.
  State size cannot exceed max entries (~225 B/entry measured in the MVP gate run, RSS roughly double — see docs/tests/performance-report-v0.md). Beyond the window a repeat is accepted again.
- Output key = `tenant_id 0x1F entity_key` (event_id when no entity key).
- Routing: malformed JSON / contract failure -> quarantine (reason codes only, original carried);
  panic, dedup-key failure, oversize record -> DLQ. No per-event status in PostgreSQL.

## Delivery and recovery
- At-least-once: batch is produced synchronously (idempotent producer, acks=all, retry with
  backoff), then offsets are committed. Dedup keys are added to the window only after the produce
  succeeds, so a failed produce retried never loses events.
- Crash between produce and commit re-processes the batch and may emit duplicates to
  `processed.events.v1`; the ClickHouse sink (deterministic insert token + dedup) is the final
  idempotent backstop.
- State restore: on start (`STREAMPROC_WARM_START=true`) the window is rebuilt by replaying
  `processed.events.v1` from now-TTL. If that fails, the loss is accepted and bounded by the dedup
  window (duplicates inside it reach the sink backstop). Multiple instances rely on raw-topic keying
  routing a duplicate to the same partition; cross-partition duplicates are also left to the sink.

## Metrics
expvar at `STREAMPROC_METRICS_ADDR` `/debug/vars`, key `streamproc`: counters (in/out/duplicates/
quarantined/dlq/retries) and state gauges `dedup_state_entries`, `dedup_state_bytes_approx`,
`dedup_evicted_ttl`, `dedup_evicted_cap`.

## Tests
`go vet ./... && go test ./...`. State growth: `TestStateGrowthBounded`, `TestHeapFlatAtCap`,
`go test -bench . ./internal/processor`. Real Kafka (dev stack, `make dev-up`):
`STREAMPROC_IT_KAFKA=127.0.0.1:19092 go test ./internal/processor -run TestRealKafka -v`.

Not done: TLS/SASL client options (`NewKafka` accepts extra `kgo.Opt`; mirror the gateway wiring).
