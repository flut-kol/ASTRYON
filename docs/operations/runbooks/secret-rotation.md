# Runbook: secret and certificate rotation (DPP-064, partial) and compromise containment

Status: written from code behaviour; not rehearsed. Secrets live only in the secret manager/mounted files, never in
the repository, tickets or logs.

## What rotates without restart (from code)

| Secret | Mechanism | Evidence |
|---|---|---|
| Kafka mTLS client cert/key and CA (`ASTRYON_KAFKA_TLS_CERT_FILE/_KEY_FILE/_CA_FILE`, `CHSINK_KAFKA_TLS_*`) in gateway, clickhouse-sink, replay-controller, replay-worker | polled every `*_TLS_RELOAD_INTERVAL` (default 1m); new broker connections use the new files; a broken file keeps the last good one and logs | live-verified incl. rotation (`kafka_mtls_it_test.go` per service) |
| Connector pull credentials (`ASTRYON_SECRET_<NAME>_FILE`) | file re-read on every pull | `connector-manager/internal/secrets/env.go` (unit-level) |

## What needs a restart or an owner

- Kafka SASL password (`*_SASL_PASSWORD`), ClickHouse/PostgreSQL passwords (`CH_PASSWORD`, `*_PG_URL`),
  `AAS_INTROSPECTION_KEY_FILE`, dev tokens: env/file read at start; rolling restart per service after the new
  credential is accepted by the server side (add new first, switch clients, remove old).
- AAS clients, keys and JWKS belong to the AAS owner. ASTRYON verifies via discovery/JWKS with a cache; do not copy auth code or keys.

## Procedure (planned rotation)

1. Create the new credential beside the old one (Kafka user/ACL, DB role password, cert signed by the same CA).
2. For hot-reloadable items: replace the mounted file; check logs for a reload (and no repeated "keeping last good" warnings).
3. For restart items: rolling restart one replica at a time; watch `AstryonGatewayAckErrors`, `AstryonSinkInsertFailures`, `AstryonKafkaConsumerLagGrowing`.
4. Revoke the old credential only after all replicas use the new one and error rates are flat.
5. Record the rotation in the audit trail (who, which secret name, when), without values.

## Compromise (SEV1, see `../incident-management.md`)

1. Revoke at the source first (AAS client/token revocation by the AAS owner; Kafka ACL/user; DB role).
2. Rotate as above but do not wait for the maintenance window; accept the restart.
3. Assess blast radius by tenant: use `auth.denied` and replay/export audit events and gateway logs by `client_id`/`jti`; never export payloads into the ticket.
4. Presigned export URLs expire (default 15 min, max 1 h); invalidate by rotating the S3 signing credential if needed.
5. Notify the component owners and the AAS owner; write the review within 5 working days.

## Not verified

Rotation under production load, SASL/DB password rotation without downtime, S3 credential rotation for export-worker and file-ingestion.
