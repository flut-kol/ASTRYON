-- DEV/TEST seed only. No secrets. Idempotent. Never run in production.
BEGIN;
SELECT set_config('astryon.platform_admin', 'on', true);
INSERT INTO tenants (id, slug, name) VALUES ('00000000-0000-0000-0000-0000000000a1', 'dev', 'Dev Tenant')
  ON CONFLICT DO NOTHING;
INSERT INTO roles (tenant_id, name, description, permissions, is_system)
SELECT '00000000-0000-0000-0000-0000000000a1', r.name, r.descr, r.perms, true
FROM (VALUES
  ('tenant_admin', 'Full tenant administration', ARRAY['*']),
  ('operator', 'Manage sources, pipelines, jobs', ARRAY['sources:*','pipelines:*','jobs:*','replay:request']),
  ('viewer', 'Read-only access', ARRAY['*:read'])
) AS r(name, descr, perms)
WHERE NOT EXISTS (SELECT 1 FROM roles x WHERE x.tenant_id='00000000-0000-0000-0000-0000000000a1' AND x.name=r.name AND x.deleted_at IS NULL);
INSERT INTO tenant_quotas (tenant_id, resource, limit_value, period)
SELECT '00000000-0000-0000-0000-0000000000a1', 'events_per_day', 1000000, 'day'
WHERE NOT EXISTS (SELECT 1 FROM tenant_quotas WHERE tenant_id='00000000-0000-0000-0000-0000000000a1' AND resource='events_per_day' AND period='day' AND deleted_at IS NULL);
COMMIT;
