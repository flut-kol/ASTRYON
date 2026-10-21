# Acceptance matrix

| Area | Evidence required | Current boundary |
|---|---|---|
| Ingestion | authenticated submit, idempotent retry, malformed quarantine | fake-AAS tests plus live AAS run (`tests/aas-live`, dev-ingest token through gateway, 2026-10-20); production AAS pending |
| Kafka/sink | confirmed ack, restart/replay, dedup read path | single-node kill/restart, multi-shard and 2-replica dedup dev evidence (`docs/tests/sink-crash-restart.md`); production HA pending |
| Query | tenant isolation, bounded filters/cursors, backend failure mapping | ClickHouse dev evidence |
| File ingestion | checksum, bounded parser, job state transitions | service tests; object-store policy pending |
| Retry | max-attempt bound, SKIP LOCKED claim, runtime health/metrics | dev PostgreSQL smoke; file-ingestion bridge (`FILE_RETRY_MODE=controller`) is opt-in, live run pending |
| Replay / export | one version per (tenant, source) replay, no loss on worker restart, bounded export streams | fake-based tests only; live checks listed in `docs/tests/live-verification-checklist.md`, not run |
| Security | SAST/SCA/secrets/SBOM and auth negative tests | CI definition present; protected checks pending |
| Production | backup restore, HA/failure domains, SLO/load report | not accepted until dedicated evidence exists |

Every release gate records command output or a linked report, date, environment,
owner and known exclusions. Passing unit tests never substitutes for a real
cluster failure, restore or AAS contract test.
