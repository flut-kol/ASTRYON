# Dev Vault (no real secrets)

Dev mode only: in-memory, root token `dev-root-token`, never use in shared environments.

```yaml
# compose snippet (merge into infra/docker/docker-compose.dev.yml if desired)
services:
  vault:
    image: hashicorp/vault:1.17
    cap_add: [IPC_LOCK]
    environment:
      VAULT_DEV_ROOT_TOKEN_ID: dev-root-token
      VAULT_DEV_LISTEN_ADDRESS: 0.0.0.0:8200
    ports: ["8200:8200"]
```

```sh
export VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=dev-root-token
vault policy write astryon-service infra/vault/policy.hcl
vault kv put secret/astryon/postgres password=change-me-dev
vault secrets enable -path=pki_int pki      # then configure intermediate CA
vault write pki_int/roles/astryon-internal @infra/vault/pki-role.hcl   # or apply the params by hand
```

Use from services: `PG_PASSWORD_REF=vault:secret/astryon/postgres#password`. See `docs/architecture/secrets-and-mtls.md`.
