# API error catalog

All public errors are JSON objects with `code`, `message`, optional `details`
and `trace_id`. Clients may branch only on `code` and HTTP status; `message` and
`details` are diagnostic and can evolve. Always retain `trace_id` for support.

## Ingestion gateway

| HTTP | Code | Client action |
|---:|---|---|
| 401 | `UNAUTHENTICATED` | Obtain a valid AAS access token; never retry the same invalid token. |
| 403 | `FORBIDDEN` | Request the required `astryon:events:write` scope. |
| 403 | `TENANT_MISMATCH` | Correct the payload; tenant identity comes from the token and cannot be overridden. |
| 400 | `IDEMPOTENCY_KEY_INVALID` | Use a printable key of at most 128 bytes. |
| 400 | `BODY_INVALID` / `BATCH_EMPTY` | Correct the JSON request. |
| 413 | `BODY_TOO_LARGE` / `BATCH_TOO_LARGE` | Split the request and retry each bounded part. |
| 422 | `ENVELOPE_INVALID` | Do not blind-retry; inspect the returned per-item validation codes. The event was quarantined. |
| 503 | `PUBLISH_FAILED` | Retry with the same idempotency key after `Retry-After`; acceptance was not confirmed. |

An accepted batch can return `207` with a `results` entry per input event. An
item status of `quarantined` is terminal until the input is repaired.

## File upload API

| HTTP | Code | Client action |
|---:|---|---|
| 401 | `UNAUTHENTICATED` | Obtain a valid AAS access token. |
| 403 | `FORBIDDEN` | Request the `astryon:uploads:write` scope. |
| 400 | `BODY_INVALID` | Correct the request (unknown fields such as `tenant_id` are rejected). |
| 404 | `NOT_FOUND` | The file object does not exist for this tenant. |
| 409 | `ALREADY_REGISTERED` | The same filename and SHA-256 were registered before; continue with that upload or change the filename. |
| 422 | `SOURCE_UNKNOWN` | Use a `source_id` that exists for this tenant. |
| 422 | `CHECKSUM_MISMATCH` | The object was deleted; verify the local file and start a new upload. |
| 422 | `UPLOAD_INCOMPLETE` | Re-upload missing parts and call complete again with the right ETags. |
| 502 / 503 | `STORAGE_UNAVAILABLE` / `UNAVAILABLE` | Retry after a short delay. |
| 422 | `OPS_INVALID` | The spreadsheet-prep ops are invalid (unknown column, bad regex, a naming collision, or a malformed op); fix and resubmit. |
| 422 | `UNSUPPORTED_CONTENT_TYPE` | Only `text/csv` uploads can be previewed or edited (`/preview`, `/versions`). |
| 409 | `FILE_NOT_COMMITTED` | Complete the upload first; preview/edit only works on committed files. |
| 413 | `EDIT_TOO_LARGE` | The edited output exceeds the configured size limit (`FILE_EDIT_MAX_BYTES`); split the file or reduce the ops. |
| 409 | `VERSION_CONFLICT` | A concurrent save claimed the same version number; retry the save. |

### File job outcomes (`GET /v1/files/uploads/{id}` -> `job.error`)

Set by the source-type profile and credentials policy (`FILE_CREDENTIALS_POLICY`); the job is `FAILED` (block) or `QUARANTINED` (nothing was written to the sink).

| Code | Meaning | Client action |
|---|---|---|
| `CREDENTIALS_DETECTED` | Credentials were found (field names in `job.labels`, never values). | Remove the secrets, rotate them, and upload again. |
| `PII_DETECTED` | The profile forbids PII in this file. | Remove or pseudonymize the listed fields, or use a `personal_data` source type with a `purpose` tag. |
| `GEO_DETECTED` | The profile forbids geo data. | Remove the listed fields. |
| `GEO_COORDINATES_REQUIRED` | A `geo` upload had no valid coordinate columns or geometries. | Add valid latitude/longitude columns, WKT or GeoJSON. |
| `SCHEMA_VIOLATION` | A record broke the profile's rules (fail-file profiles). | Fix malformed or incomplete records. |
| `ROW_LIMIT_EXCEEDED` | More records than the profile allows. | Split the file. |

## Query API

| HTTP | Code | Client action |
|---:|---|---|
| 401 | `unauthenticated` | Refresh/replace the AAS token. |
| 403 | `forbidden` | Obtain `astryon:events:read` or correct the authenticated tenant. |
| 400 | `invalid_request` | Correct request JSON, dates, cursor or pagination parameters. |
| 400 | `invalid_group_by` / `invalid_metric` | Use only documented query dimensions and aggregate names. |
| 413 | `body_too_large` | Reduce request size. |
| 502 | `backend_error` | Retry only if the operation is safe; include `trace_id` when escalating. |

The API deliberately does not expose ClickHouse connection details, SQL or AAS
verification reasons in responses. Operators correlate `trace_id` with
restricted server logs; clients must not parse error message text.
