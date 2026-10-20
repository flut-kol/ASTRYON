# File ingestion

Foundation for Sprint 07 (`DPP-031` through `DPP-035`). The package is a
library-only implementation at present; it does not yet expose an HTTP server
or run distributed workers.

## Implemented contracts

- tenant-scoped multipart upload planning with bounded size/part count,
  allow-listed content types, SHA-256 validation and short-lived part URLs;
- bounded-memory CSV and NDJSON parsing with per-record errors suitable for
  quarantine;
- restart-safe NDJSON positions (`Offset`, `NextOffset`, record number) and a
  checkpoint that can only advance over the next completed record;
- deterministic NDJSON deduplication keys derived from the immutable source
  fingerprint and byte offset;
- explicit batch states and transition validation;
- lease acquire/heartbeat rules;
- bulk-result lineage for Parquet S3 or ClickHouse paths (not per-row Kafka).

For NDJSON, persist `Checkpoint.NextOffset` and `NextRecordNumber` only after
the normalized or quarantine result has been durably accepted. On restart,
verify `Checkpoint.SourceFingerprint` against the object checksum, range-read
from `NextOffset`, and call `NDJSONFrom`. A crash after the sink write but
before checkpoint persistence can replay one record; sinks must therefore use
`Record.DedupKey` as their idempotency key.

## Remaining work

- durable upload completion, checksum verification, malware/content scanning
  and object lifecycle;
- Parquet streaming and resumable CSV/Parquet checkpoints;
- persistent job/state/checkpoint repository and atomic sink-plus-checkpoint
  integration;
- durable fenced leases/heartbeats and worker executable;
- actual Parquet normalization, ClickHouse bulk ingest and durable lineage.
