# Failure model

ASTRYON confirms ingestion only after Kafka acknowledges the record. The system
is at-least-once across Kafka, processors and analytical sinks; clients retain
their idempotency key until they receive a confirmed result.

Kafka producer retries can duplicate delivery. Consumers and sinks must use the
documented event identity (`tenant_id`, `event_id`, `pipeline_version`) and
sink-specific durable chunk/token mechanism. ClickHouse readers use the
documented dedup query path where overlap is possible; this is not a claim that
every physical row is exactly once.

Failure boundaries:

- before Kafka ack: client retries with the same idempotency key;
- after Kafka ack: retrying a lost HTTP response is safe only through identity
  deduplication;
- after sink insert but before offset commit: the sink may replay; durable chunk
  intent is required where overlapping chunks would otherwise be ambiguous;
- PostgreSQL stores only control-plane/job metadata and bounded deviations, never
  a per-event delivery status ledger.

HA, cross-site RPO/RTO and exactly-once business outcomes require dedicated
failure/restore evidence. They are not established by the single-node dev stack.
