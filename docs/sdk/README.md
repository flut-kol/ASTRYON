# SDK generation contract

Generated clients use the versioned OpenAPI documents in `docs/api/` as the
only source of truth. Generation must run in CI from a pinned generator image
and commit neither access tokens nor environment-specific URLs.

Before publishing a client, CI must verify: OpenAPI validation, a breaking-change
diff against the previous released spec, generated-client compilation and an
authenticated tenant-isolation contract test. Clients expose `trace_id` and
structured error `code`; they must not parse server error message text.
