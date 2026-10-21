# ASTRYON processing (DPP-026/027, first tranche)

Apache Flink 1.20.3 / Java 17 job for the first executable processing slice:

1. consume `raw.events.v1` while preserving Kafka topic/partition/offset lineage;
2. validate the shared envelope rules and normalize inline JSON;
3. publish every supported valid envelope to `validated.events.v1`;
4. bounded processing-time dedup on `(tenant_id, source_id, event_id, processing_version)`;
5. publish unique normalized events to `processed.events.v1`;
6. publish validation/normalization failures as canonical `QuarantineRecordV1` values, with a bounded original copy, to `events.quarantine.v1`.

The job uses incremental RocksDB state, exactly-once Flink checkpoints in S3 and at-least-once Kafka output. The output choice follows ADR-011: downstream consumers must remain idempotent.

The actual dedup identity is `(tenant_id, source_id, event_id, processing_version)`, so a replay into a new processing version is never suppressed by an older version still inside the TTL window.

`tenant_id` is never accepted from an HTTP header or request by this service. This internal job trusts the claim-derived `tenant_id` already embedded by the gateway because Kafka ACLs must restrict `raw.events.v1` writers to the authenticated gateway and explicitly approved producers. A producer ACL is therefore part of the security boundary.

Quarantine records use a deterministic UUIDv8 derived from `("flink-processing", topic, partition, offset)`, carry Kafka provenance as a coordinate pair, and use the Kafka key `tenant_id + 0x1f + deviation_id`. Original bytes are canonical padded base64 and limited to 64 KiB. Diagnostics are bounded fixed messages and never copy payload data. If the envelope has no validated tenant or Kafka provenance is invalid, processing fails instead of publishing a misleading tenant record or checkpointing a silent drop.

## Build and test

```bash
mvn test
mvn package -DskipTests
```

The shaded job JAR is `target/processing-0.1.0-SNAPSHOT.jar`. Flink runtime libraries remain `provided`; the Kafka connector and Jackson are included. The Flink distribution must have an S3 filesystem plugin installed and configured for the S3-compatible endpoint.

## Required configuration

| Variable | Example | Meaning |
|---|---|---|
| `PROCESSING_KAFKA_BROKERS` | `kafka-1:9092,kafka-2:9092` | Kafka bootstrap servers |
| `PROCESSING_CHECKPOINT_URI` | `s3://astryon-checkpoints/processing-v1` | Durable checkpoint prefix; non-S3 locations are refused |
| `PROCESSING_PIPELINE_ID` | UUID | Pipeline identity written to normalized events |
| `PROCESSING_KAFKA_GROUP_ID` | `astryon-processing-v1` | Consumer group; default shown |
| `PROCESSING_PIPELINE_VERSION` | `1` | Positive normalization version; default 1 |
| `PROCESSING_CHECKPOINT_INTERVAL` | `30s` | Positive `ms`, `s`, `m`, or `h`; default 30s |
| `PROCESSING_DEDUP_WINDOW` | `24h` | Fixed first-seen dedup window; default 24h |

Kafka TLS/SASL and S3 credentials belong in the Flink cluster configuration/secret injection, never in the repository.

## Metrics

Custom Flink counters: `records_validated_total`, `records_processed_total` (after dedup) and
`records_quarantined_total{reason}` (`reason` from a fixed `ERR_*` allow-list, otherwise `OTHER`). With the dev image they are
scraped via the Prometheus reporter; see `infra/docker/extras/metrics/README.md`.

## State-growth measurement

The `bounded-dedup` operator exports `dedup_unique_total`, `dedup_dropped_total`, and `dedup_expirations_total`. For a real DPP-027 measurement, feed successive unique-key plateaus (for example 100k, 1m, 10m), wait for a completed checkpoint at each plateau, and record the checkpoint `state_size` from Flink's `/jobs/:jobid/checkpoints` response together with the three counters. Then wait one full dedup window and confirm both expirations and checkpoint-size contraction. State TTL is a cleanup fallback; explicit processing-time timers define the fixed window and duplicates do not extend it.

No state-size number is claimed yet: this checkout has no Flink cluster, so build and unit verification do not constitute runtime state-growth evidence.

## Deliberate first-tranche limits

- Inline `payload_format=json` only. Avro, Protobuf and S3 object resolution are quarantined with `ERR_NORMALIZATION_UNSUPPORTED`; accepting them without a schema registry/object integrity adapter would be misleading.
- An optional inline checksum is verified against the compact canonical JSON payload bytes and preserved unchanged. A mismatch is quarantined; the processor never rewrites a failed integrity claim.
- Processing-time dedup is deliberate for the first bounded implementation. Event-time late-data policy and watermarks remain a follow-up.
- This does not claim an end-to-end checkpoint/restore, TaskManager kill, Kafka failover, or state-growth benchmark. Those require the dev Flink/S3 deployment and are acceptance work, not unit evidence.
