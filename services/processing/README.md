# ASTRYON processing (DPP-026/027, first tranche)

Apache Flink 1.20.3 / Java 17 job for the first executable processing slice:

1. consume `raw.events.v1` while preserving Kafka topic/partition/offset lineage;
2. validate the shared envelope rules and normalize inline JSON;
3. publish every supported valid envelope to `validated.events.v1`;
4. bounded processing-time dedup on `(tenant_id, source_id, event_id, processing_version)`;
5. publish unique normalized events to `processed.events.v1`;
6. publish validation/normalization failures as canonical `QuarantineRecordV1` values, with a bounded original copy, to `events.quarantine.v1`.

Inputs that cannot provide a validated UUID `tenant_id` are published to
`events.dlq.v1` instead. The DLQ record deliberately has no tenant field (a
tenant must never be invented), carries the Kafka topic/partition/offset,
deterministic `dead_letter_id`, and fixed diagnostic. It deliberately does not
copy opaque input bytes, because an invalid envelope cannot establish a safe
tenant boundary. Tenant-bearing validation failures continue to use the canonical
`QuarantineRecordV1`; invalid Kafka provenance fails the operator so a record
cannot be acknowledged without a stable retry identity.

The job uses incremental RocksDB state, exactly-once Flink checkpoints in S3 and at-least-once Kafka output. The output choice follows ADR-011: downstream consumers must remain idempotent.

The actual dedup identity is `(tenant_id, source_id, event_id, processing_version)`, so a replay into a new processing version is never suppressed by an older version still inside the TTL window.

`tenant_id` is never accepted from an HTTP header or request by this service. This internal job trusts the claim-derived `tenant_id` already embedded by the gateway because Kafka ACLs must restrict `raw.events.v1` writers to the authenticated gateway and explicitly approved producers. A producer ACL is therefore part of the security boundary.

Quarantine records use a deterministic UUIDv8 derived from `("flink-processing", topic, partition, offset)`, carry Kafka provenance as a coordinate pair, and use the Kafka key `tenant_id + 0x1f + deviation_id`. Original bytes are canonical padded base64 and limited to 64 KiB. Diagnostics are bounded fixed messages and never copy payload data. Tenantless input uses the same provenance identity in the DLQ; invalid Kafka provenance still fails instead of publishing an unstable record or checkpointing a silent drop.

## Build and test

```bash
mvn test
mvn package -DskipTests
# without a local Maven:
docker run --rm -v "$PWD":/w -w /w maven:3.9-eclipse-temurin-17 mvn -B test
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

## Dedup design (DPP-027)

Key: `tenant_id 0x1f source_id 0x1f event_id 0x1f processing_version`, built in `EnvelopeProcessor` from the validated envelope (the same identity as `libs/event-contracts` event identity plus the processing version). The job does `keyBy(dedupKey)` into `BoundedDeduplicateFunction`:

- per key one `Long` of state (`dedup-expires-at`); the first record is emitted, duplicates inside the window are dropped;
- the window is fixed from first sight: a duplicate does not extend it, so a duplicate storm cannot keep state alive;
- expiry is an explicit processing-time timer that clears the state; Flink state TTL (`OnCreateAndWrite`, `NeverReturnExpired`) is configured with the same duration as a cleanup fallback;
- window length is `PROCESSING_DEDUP_WINDOW` (default `24h`, zero or negative refused), so state is bounded by `unique keys per window`, not by history.

Limits: dedup is per processing-time window; a duplicate arriving after the window is emitted again (downstream consumers stay idempotent, ADR-011). Late data and event-time semantics are not handled.

## Metrics

Custom Flink counters: `records_validated_total`, `records_processed_total` (after dedup) and
`records_quarantined_total{reason}` (`reason` from a fixed `ERR_*` allow-list, otherwise `OTHER`). Dedup operator (`bounded-dedup`):
`dedup_unique_total`, `dedup_dropped_total` (duplicate count), `dedup_expirations_total`, and gauge `dedup_active_keys` (keys currently tracked by the subtask; approximate: it starts from 0 after a restore from a checkpoint and is clamped at 0, so use checkpoint `state_size` as the authoritative figure). No tenant, source or event identifiers are used as labels. With the dev image they are scraped via the Prometheus reporter; see `infra/docker/extras/metrics/README.md`.

## State-growth measurement

Goal: show that state follows unique keys per window and shrinks after the window, not total history.

1. Start the job with a short window to make the experiment fast, for example `PROCESSING_DEDUP_WINDOW=10m` and `PROCESSING_CHECKPOINT_INTERVAL=10s` (dev mini-cluster: `docker compose -f infra/docker/docker-compose.extras.yml --profile processing up -d --build processing`; rebuild is required for a jar that contains `dedup_active_keys`).
2. Feed successive plateaus of unique keys, for example 100k, 1M, 10M events (`tests/load` `loadgen` with `dup=0`, distinct event ids). At each plateau wait for a completed checkpoint, then record: checkpoint `state_size` from `GET /jobs/<jobid>/checkpoints` (Flink UI port 18481 in dev), the metrics `dedup_unique_total`, `dedup_dropped_total`, `dedup_expirations_total`, `dedup_active_keys`, and the RocksDB size on the TaskManager volume.
3. Duplicate storm: resend the last plateau unchanged. Expect `dedup_dropped_total` to rise by the plateau size, `dedup_unique_total` and `state_size` to stay flat, and the expiry time not to move.
4. Wait one full window with no input. Expect `dedup_expirations_total` to approach `dedup_unique_total`, `dedup_active_keys` to fall to about 0 and the checkpoint `state_size` to contract (RocksDB reclaims space after compaction, so allow a few checkpoints).
5. Derive bytes per key (`state_size` delta / plateau delta) and extrapolate: `bytes_per_key * unique_keys_per_window` is the steady-state bound for the configured window.

Unit tests (`BoundedDeduplicateFunctionTest`, Flink keyed-operator test harness) verify the semantics only: duplicate dropped, window not extended by duplicates, timer clears state, new processing version is not a duplicate, snapshot/restore keeps the window, non-positive window refused. No state-size number is claimed: no plateau run has been made, so there is no runtime state-growth evidence yet.

## Deliberate first-tranche limits

- Inline `payload_format=json` only. Avro, Protobuf and S3 object resolution are quarantined with `ERR_NORMALIZATION_UNSUPPORTED`; accepting them without a schema registry/object integrity adapter would be misleading.
- An optional inline checksum is verified against the compact canonical JSON payload bytes and preserved unchanged. A mismatch is quarantined; the processor never rewrites a failed integrity claim.
- Processing-time dedup is deliberate for the first bounded implementation. Event-time late-data policy and watermarks remain a follow-up.
- Live dev verification on 2026-10-22 covered the Flink/S3 checkpoint path and savepoint restore: a running job was savepointed, canceled, restarted from that savepoint, and a duplicate key was dropped after restore (`dedup_dropped_total` increased while `processed.events.v1` did not). This still does not claim TaskManager kill, Kafka failover, HA, or a state-growth benchmark; those remain acceptance work.
