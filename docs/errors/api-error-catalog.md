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
