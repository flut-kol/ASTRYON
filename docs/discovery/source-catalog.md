# Source-type catalog (DPP-001)

Status: draft from repository evidence only. Every "supported" statement below points at code or a doc in this repo.
No real data source has been interviewed: columns about volumes, SLA and owners that the repo cannot answer are
`TODO-owner`. Plan: `DEVELOPMENT_PLAN.md` §9 Sprint 01, §5 (envelope), §2 (SLO).

## 1. Ingestion paths that exist

| Path | Entry point | Evidence |
|---|---|---|
| Push, single event | `POST /v1/events` (JSON envelope) | `services/gateway`, `docs/onboarding/source-ingestion.md` |
| Push, micro-batch | `POST /v1/events/batch` (envelope array) | same |
| File upload (multipart to S3, then job) | `POST /v1/files/uploads`, `.../{id}/complete` | `services/file-ingestion/README.md`, `docs/api/file-ingestion.openapi.yaml` |
| REST pull (cursor, incremental) | connector type `REST_PULL` | `services/connector-manager/README.md` (restpull, pullstore, runner) |
| CDC (Debezium JSON from Kafka) | `ASTRYON_CDC_TOPIC` consumer in connector-manager | same, `internal/cdc`; Debezium 2.7.3 + a separate PG in the dev extras compose |

## 2. Catalog

| # | Source type | Format | Event / object size (limit in code) | Volume and SLA | CDC / ETL / OCR | Security | Status in repo |
|---|---|---|---|---|---|---|---|
| S1 | Machine producer, small JSON events | JSON envelope `docs/events/schemas/envelope.v1.json`; `payload_format` enum in plan §5.1 is json/protobuf/avro/parquet-reference, only JSON is implemented in the gateway | single request body <= 2 MiB (`ASTRYON_GATEWAY_MAX_BODY_BYTES`); batch <= 8 MiB and <= 100 events (`ASTRYON_GATEWAY_MAX_BATCH_BYTES`, `_MAX_BATCH_EVENTS`); benchmarked at ~1 KiB | target 100M/day early, 120k ev/s peak, p95 ACK <= 250 ms (plan §2, targets). Measured: 45.3k ev/s accepted, p95 ACK 420 ms on one dev broker (`docs/benchmarks/dpp-010-2026-10-20.md`). Per-source volumes: TODO-owner | none (ETL = normalization in `services/processing`, Flink) | AAS `client_credentials` + `private_key_jwt`, scope `astryon:events:write`, tenant from token claim only, optional DPoP (`AAS_REQUIRE_DPOP`), `Idempotency-Key` | gateway done; Flink job in progress; real AAS token verified (`docs/tests/acceptance.md`) |
| S2 | Wide JSON events | same envelope | no separate limit; falls under the 2 MiB / 8 MiB caps above; no wide-event benchmark exists | TODO-owner | none | as S1 | generator for wide JSON (DPP-002) not verified in this review: TODO-owner |
| S3 | NDJSON file / bulk import | `application/x-ndjson`, resumable byte-offset checkpoints | file <= 5 GiB (`FILE_MAX_BYTES`), <= 1000 parts, record <= 1 MiB (`FILE_MAX_RECORD_BYTES`); tenant quota 20 open uploads, 100 GiB declared bytes/day (`FILE_QUOTA_*`) | plan SLO: "TB/h import, completion SLA depends on data" has no number; 1+ TB backfill test PERF-07 not run. TODO-owner | ETL: worker batches (500) go **straight to ClickHouse**, not via Kafka (decision 2026-10-21, `file-ingestion/README.md`) | AAS scope `astryon:uploads:write`; SHA-256 verified on complete, tampered object deleted; credential/PII classifier with policy `quarantine|block|allow` (`FILE_CREDENTIALS_POLICY`); malware scan: **not implemented** | file-api + worker proven e2e against MinIO/PG16/CH24.8 |
| S4 | CSV file | `text/csv`, header row, <= 1024 columns, BOM handled | same limits as S3 | as S3 | ETL as S3; malformed rows -> `processing_errors` `ERR_CSV_MALFORMED` | as S3 | done in worker |
| S5 | Parquet / Avro / Protobuf files | not accepted: `FILE_CONTENT_TYPES` default is `application/x-ndjson,text/csv`; parser set is CSV and NDJSON | n/a | n/a | ETL via Spark/Python (plan ADR-004) not started | n/a | **not supported at commit cfef8b9**; uncommitted Parquet files (`internal/parse/parquet.go`, `internal/worker/parquet.go`) were present in the working tree when this was written, re-check before sign-off; demand: TODO-owner |
| S6 | REST API polled by the platform | JSON array, optionally under `records_path`; cursor via `X-Next-Cursor` | response <= `max_response_bytes` (connector config); rate `requests_per_second` | interval `interval_seconds`; backoff 30 s base, 1 h cap. Per-API volume: TODO-owner | ETL: `MapPage` to envelopes; stable `event_id` from connector + `id_field` or record SHA-256 | credential from `secret_ref` via env/file resolver (Vault resolver not done); SSRF controls on `endpoint`: **not evidenced**, review before enabling user-defined connectors (plan §11 security list) | runner and Kafka emitter done; no UI-driven connection test evidence |
| S7 | PostgreSQL / MySQL via CDC | Debezium JSON change events (ops c/u/d/r) on Kafka | one Kafka record per row change; no size benchmark | RPO/lag targets: TODO-owner (Sprint 08 gate: restart/failover keeps position) | CDC: Debezium consumer implemented; deletes become tombstone-style events; snapshot + tail ordering and schema evolution beyond `cdc.Compatible`: not done | tenant/source only from connector mapping, never payload; DB credentials live in Kafka Connect config (provisioning not in repo) | consumer core + live Kafka test; Debezium provisioning and a real PG/MySQL source not verified. MySQL only by design, nothing MySQL-specific tested in the repo |
| S8 | Documents / scans requiring OCR | none | n/a | n/a | OCR: **no code, no config, no worker anywhere in the repo**; plan lists AI/OCR workers for v1.1 (DPP-074) | n/a | not started; see `post-rollout.md` |
| S9 | Browser / operator uploads via Admin UI | multipart through admin-bff `/api/files` | as S3 | n/a | none | OIDC code + PKCE against AAS, server-side session cookie, CSRF check (`services/admin-bff`) | UI done; no user has signed in live (`readiness-review.md`) |

## 3. Cross-cutting facts the catalog depends on

- Retention defaults (pending DPP-003): ClickHouse events 400 d, aggregates 800 d, errors 90 d (`db/clickhouse/migrations/0006`).
  Kafka topic retention hypotheses are in plan §5.2 (raw 7 d, quarantine 14 d).
- Delivery semantics are at-least-once with idempotent sinks (ADR-011); readers dedup with `LIMIT 1 BY` or `uniqExact`.
- Duplicates, late events, invalid schemas and 80%-tenant skew are in the loadgen (`tests/load`: dup, invalid, tenant-skew).

## 4. Open items for the catalog (owner needed)

- TODO-owner: list of real sources per tenant with expected events/day, peak ev/s, avg and p99 event size, freshness SLA.
- TODO-owner: which sources need Parquet/Avro/Protobuf, OCR, MySQL CDC or lakehouse. Until answered these stay out of v1.
- TODO-owner: legal classification of data per source (PII, secrets), required retention per source type.
