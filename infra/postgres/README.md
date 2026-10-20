# PostgreSQL HA baseline (DPP-017)

These are deployment-time templates for a three-member Patroni cluster,
PgBouncer transaction pooling and pgBackRest WAL/base-backup storage. Render
them only through the approved secret manager: the environment placeholders are
not defaults and must never be replaced with credentials in Git.

Before production use, provide distinct failure domains, a mutually authenticated
DCS, TLS identities, a writable S3-compatible backup repository and an
application role without SUPERUSER/BYPASSRLS. The local Docker PostgreSQL
container is not this HA design.

## Restore test

1. Isolate a clean PostgreSQL host/namespace and restore the same pgBackRest
   stanza to a new data directory.
2. Start it without production clients, replay WAL to the selected recovery
   target, then run schema and tenant/RLS smoke checks.
3. Record backup timestamp, recovered LSN, elapsed restore time, data checksums
   and any gaps. Destroy the test environment after sign-off.

Do not claim RPO/RTO or HA correctness until a three-node failover and this
restore procedure have been independently exercised.
