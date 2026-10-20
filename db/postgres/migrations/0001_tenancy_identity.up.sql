-- ROLLBACK PLAN: forward-only. To revert in a non-prod env: DROP TABLE api_keys, role_bindings, roles, principals, tenants CASCADE;
--   then DROP FUNCTION astryon_enable_rls(regclass), astryon_set_updated_at(), astryon_tenant_id(); DROP TYPE principal_kind.
--   In prod: ship a new forward migration instead; data restore from PITR backup taken before deploy.
-- Group: tenancy + identity references. Authentication is external (AAS); no passwords stored.

CREATE FUNCTION astryon_set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := now(); RETURN NEW; END $$;

-- Tenant from session setting (set per transaction from VALIDATED token claims only: SET LOCAL astryon.tenant_id).
CREATE FUNCTION astryon_tenant_id() RETURNS uuid LANGUAGE sql STABLE AS $$
  SELECT NULLIF(current_setting('astryon.tenant_id', true), '')::uuid $$;

-- Enables + forces RLS on a table that has tenant_id, with a tenant isolation policy.
-- 'astryon.platform_admin'='on' is for trusted platform workers/migrations/seeds only (never set from request data).
CREATE FUNCTION astryon_enable_rls(tbl regclass, col text DEFAULT 'tenant_id') RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
  EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
  EXECUTE format($p$CREATE POLICY tenant_isolation ON %s USING (
      %I = NULLIF(current_setting('astryon.tenant_id', true), '')::uuid
      OR current_setting('astryon.platform_admin', true) = 'on')
    WITH CHECK (
      %I = NULLIF(current_setting('astryon.tenant_id', true), '')::uuid
      OR current_setting('astryon.platform_admin', true) = 'on')$p$, tbl, col, col);
END $$;

CREATE TYPE principal_kind AS ENUM ('user', 'service');

CREATE TABLE tenants (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  slug        text NOT NULL CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  name        text NOT NULL,
  aas_tenant_ref text,                      -- external AAS tenant/org identifier
  attributes  jsonb NOT NULL DEFAULT '{}',
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz
);
CREATE UNIQUE INDEX tenants_slug_uq ON tenants (slug) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX tenants_aas_ref_uq ON tenants (aas_tenant_ref) WHERE aas_tenant_ref IS NOT NULL AND deleted_at IS NULL;
CREATE TRIGGER tenants_updated BEFORE UPDATE ON tenants FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

CREATE TABLE principals (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  uuid NOT NULL REFERENCES tenants(id),
  aas_sub    text NOT NULL,                 -- external AAS subject; no credentials here
  kind       principal_kind NOT NULL DEFAULT 'user',
  display_name text,
  is_active  boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX principals_tenant_sub_uq ON principals (tenant_id, aas_sub) WHERE deleted_at IS NULL;
CREATE TRIGGER principals_updated BEFORE UPDATE ON principals FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

CREATE TABLE roles (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  name        text NOT NULL,
  description text,
  permissions text[] NOT NULL DEFAULT '{}',
  is_system   boolean NOT NULL DEFAULT false,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX roles_tenant_name_uq ON roles (tenant_id, name) WHERE deleted_at IS NULL;
CREATE TRIGGER roles_updated BEFORE UPDATE ON roles FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

CREATE TABLE role_bindings (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    uuid NOT NULL REFERENCES tenants(id),
  principal_id uuid NOT NULL,
  role_id      uuid NOT NULL,
  scope_type   text NOT NULL DEFAULT 'tenant' CHECK (scope_type IN ('tenant','source','pipeline')),
  scope_id     uuid,                        -- NULL for tenant scope
  granted_by   uuid,
  created_at   timestamptz NOT NULL DEFAULT now(),
  expires_at   timestamptz,
  deleted_at   timestamptz,
  FOREIGN KEY (tenant_id, principal_id) REFERENCES principals (tenant_id, id),
  FOREIGN KEY (tenant_id, role_id) REFERENCES roles (tenant_id, id),
  CHECK ((scope_type = 'tenant') = (scope_id IS NULL))
);
CREATE UNIQUE INDEX role_bindings_uq ON role_bindings (tenant_id, principal_id, role_id, scope_type, COALESCE(scope_id, '00000000-0000-0000-0000-000000000000')) WHERE deleted_at IS NULL;
CREATE INDEX role_bindings_principal_ix ON role_bindings (tenant_id, principal_id);
CREATE INDEX role_bindings_role_ix ON role_bindings (tenant_id, role_id);

CREATE TABLE api_keys (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    uuid NOT NULL REFERENCES tenants(id),
  principal_id uuid NOT NULL,
  name         text NOT NULL,
  key_prefix   text NOT NULL,               -- first chars, for display/lookup only
  key_hash     bytea NOT NULL,              -- hash only (e.g. SHA-256/HMAC); plaintext is never stored
  hash_algo    text NOT NULL DEFAULT 'sha256',
  scopes       text[] NOT NULL DEFAULT '{}',
  expires_at   timestamptz,
  last_used_at timestamptz,
  revoked_at   timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now(),
  deleted_at   timestamptz,
  FOREIGN KEY (tenant_id, principal_id) REFERENCES principals (tenant_id, id)
);
CREATE UNIQUE INDEX api_keys_hash_uq ON api_keys (key_hash);
CREATE INDEX api_keys_prefix_ix ON api_keys (key_prefix);
CREATE INDEX api_keys_principal_ix ON api_keys (tenant_id, principal_id);

-- RLS. tenants is keyed on its own id.
SELECT astryon_enable_rls('tenants', 'id');
SELECT astryon_enable_rls('principals');
SELECT astryon_enable_rls('roles');
SELECT astryon_enable_rls('role_bindings');
SELECT astryon_enable_rls('api_keys');
