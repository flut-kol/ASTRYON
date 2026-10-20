# Acceptance matrix

| Area | Evidence required | Current boundary |
|---|---|---|
| Ingestion | authenticated submit, idempotent retry, malformed quarantine | dev/fake-AAS tests; real AAS pending |
| Kafka/sink | confirmed ack, restart/replay, dedup read path | single-node dev evidence; HA pending |
| Query | tenant isolation, bounded filters/cursors, backend failure mapping | ClickHouse dev evidence |
| File ingestion | checksum, bounded parser, job state transitions | service tests; object-store policy pending |
| Retry | max-attempt bound, SKIP LOCKED claim, runtime health/metrics | dev PostgreSQL smoke; job-worker integration pending |
| Security | SAST/SCA/secrets/SBOM and auth negative tests | CI definition present; protected checks pending |
| Production | backup restore, HA/failure domains, SLO/load report | not accepted until dedicated evidence exists |

Every release gate records command output or a linked report, date, environment,
owner and known exclusions. Passing unit tests never substitutes for a real
cluster failure, restore or AAS contract test.
