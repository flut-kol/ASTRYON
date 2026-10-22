# SDK generation contract

Generated clients use the versioned OpenAPI documents in `docs/api/` as the
only source of truth. Generation must run in CI from a pinned generator image
and commit neither access tokens nor environment-specific URLs.

Before publishing a client, CI must verify: OpenAPI validation, a breaking-change
diff against the previous released spec, generated-client compilation and an
authenticated tenant-isolation contract test. Clients expose `trace_id` and
structured error `code`; they must not parse server error message text.

## Validating the specs

CI job `OpenAPI lint` (`.github/workflows/ci.yml`) runs Redocly with `docs/api/redocly.yaml` on every spec. Locally:

```sh
for f in docs/api/*.openapi.yaml; do
  docker run --rm -v "$PWD/docs/api:/spec:ro" redocly/cli:1.25.5 lint --config /spec/redocly.yaml "/spec/$(basename "$f")"
done
```

All five specs pass with no errors or warnings (2026-10-21). Errors are documented via the shared `default` response, so
the `operation-4xx-response` rule is off (see `docs/api/redocly.yaml`). The API specs use OpenAPI 3.x; file-ingestion stays on 3.0.3 for generator compatibility while the remaining specs are 3.1. CI job `OpenAPI breaking changes` (pull requests) runs `oasdiff breaking --fail-on ERR` against the base branch's copy of each
spec (checked locally against HEAD, 2026-10-21; the CI job itself has not run). Go clients, one module per spec: `libs/sdk-query`, `libs/sdk-quarantine`, `libs/sdk-connector-manager`, `libs/sdk-export-worker`, `libs/sdk-file-ingestion` (`go generate`, oapi-codegen v2.4.1; its contract test checks no tenant is sent and `code` (and `trace_id` where the spec has it) are exposed; it does not test server-side tenant isolation). CI job `Generated SDK is up to date` regenerates (`GOWORK=off go generate`, go 1.27) and fails on any diff (not yet run in CI; local regeneration matched the working tree, 2026-10-22). Live tenant-isolation test for the query-api client: `tests/aas-live/tenant_isolation_test.go` (real AAS tokens of two tenants, ClickHouse stubbed; run 2026-10-21). Stored-data isolation: `services/query-api` `TestIntegrationTenantIsolationRealClickHouse` (needs `QUERY_CH_IT_URL`; two tenants with identical source and event ids, events/aggregate/cursors; run 2026-10-21 on ClickHouse 24.8 single node). Still missing: the same for the other four clients. Note the diff is against the base branch, not a tagged release.
