# Secrets management and internal mTLS (DPP-014)

Libraries: `libs/go-common/pkg/secrets`, `libs/go-common/pkg/tlsconf`. Dev Vault: `infra/vault/`.

## Secret references

Config never holds secret values, only references resolved at startup (and on rotation):

| Ref | Source | Notes |
|---|---|---|
| `env:NAME` | environment variable | dev / CI |
| `file:/path` | file, re-read on every `Get` | k8s Secret mounts, Vault Agent sinks; trailing newline trimmed; warns if world-readable |
| `vault:mount/path#key` | Vault KV v2 (`GET /v1/mount/data/path`) | `VAULT_ADDR` + `VAULT_TOKEN`, or k8s auth (`VAULT_K8S_ROLE`, SA JWT); 30s cache |

`secrets.Secret` redacts in `String`, `%v/%#v`, JSON and `slog`; use `.Reveal()` only at the point of use. Errors carry the reference, never values or Vault response bodies.

### Mapping to backends

| Backend | Config (reference) | Vault path |
|---|---|---|
| PostgreSQL | `PG_PASSWORD_REF=vault:secret/astryon/postgres#password` (DSN built in code) | `secret/astryon/postgres` |
| Kafka SASL/SCRAM | `KAFKA_SASL_PASSWORD_REF=vault:secret/astryon/kafka/<svc>#password` | one user per service |
| ClickHouse | `CH_PASSWORD_REF=vault:secret/astryon/clickhouse/<svc>#password` | one user per service |

Dev default: `env:` / `file:` refs; production: `vault:` (or `file:` fed by Vault Agent / External Secrets). tenant_id is unrelated to secrets and still comes only from token claims.

## Rotation

- `file:` and `vault:` are re-resolved: callers should re-`Resolve` on auth failure and on a timer (<= cache TTL), and rebuild the pool/producer credentials.
- Passwords: dual-credential rotation (create new user/password, roll deployments, drop old). Kafka SCRAM supports multiple credentials per user in transition.
- Certificates rotate without restart via `tlsconf` (stat-based hot reload, throttled; a corrupt/half-written file keeps the previous cert).

## Where mTLS applies

- service <-> service (gateway -> query-api, internal HTTP/gRPC): `tlsconf.ServerConfig` (RequireAndVerifyClientCert) / `ClientConfig`. Public edge uses normal TLS plus AAS tokens; mTLS is internal only and does not replace token auth.
- Kafka: TLS listeners with client certs (in addition to SASL/SCRAM per existing dev scripts).
- ClickHouse: secure native/HTTP ports with client cert verification.
- PostgreSQL: `sslmode=verify-full` with client cert (`hostssl ... cert` in pg_hba).

Minimum TLS 1.2, `Options.MinTLS13` for services that can require 1.3; Go negotiates 1.3 when available.

## Certificate issuance plan

1. Kubernetes: cert-manager `ClusterIssuer` of type Vault, backed by the Vault PKI role in `infra/vault/pki-role.hcl`; one `Certificate` per service (short TTL, 24h, renew at 2/3), mounted as `tls.crt`, `tls.key`, `ca.crt` (consumed by `tlsconf.Files`).
2. Non-k8s / dev: Vault Agent template or `vault write pki_int/issue/astryon-internal`.
3. Roots: offline root CA -> Vault intermediate (`pki_int`); trust bundle distributed as `ca.crt`.
4. Identity: SAN = `<service>.<namespace>.svc`; services may additionally authorize by client-cert CN/SAN.
