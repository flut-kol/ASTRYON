# Gateway

The ingestion gateway can persist tenant-scoped security events through `libs/audit`.
Set `ASTRYON_AUDIT_PG_URL` to enable the PostgreSQL writer; leave it unset to disable
audit persistence. The database role must not be a superuser and must not have
`BYPASSRLS`.

Audit writer settings:

| Variable | Default | Constraint |
|---|---:|---|
| `ASTRYON_AUDIT_QUEUE_SIZE` | `1024` | positive integer |
| `ASTRYON_AUDIT_BATCH_SIZE` | `100` | positive integer |
| `ASTRYON_AUDIT_FLUSH_INTERVAL` | `500ms` | positive Go duration |
| `ASTRYON_AUDIT_WRITE_TIMEOUT` | `5s` | positive Go duration |
| `ASTRYON_AUDIT_RETRIES` | `2` | non-negative integer |
| `ASTRYON_AUDIT_DRAIN_TIMEOUT` | `10s` | positive Go duration |
| `ASTRYON_AUDIT_PG_MAX_CONNS` | `4` | `1..32` |

The gateway drains the writer during graceful shutdown. `/metrics` exposes queued,
written, dropped, and rejected audit totals. It also exposes attempts that cannot be
persisted because no validated principal exists.

`auth.denied` is recorded for scope denials and tenant-mismatch attempts. Event tenant,
actor, and ACR come only from the validated principal; the request body tenant is never
copied into audit data. A missing or invalid token has no validated tenant, while the
audit schema requires one. Such attempts are therefore not assigned a synthetic tenant
and are counted by `astryon_gateway_audit_*_unrecordable_total`.
