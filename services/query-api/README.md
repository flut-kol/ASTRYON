# query-api (DPP-029)

Read API over ClickHouse. Contract: `docs/api/query-api.openapi.yaml`.

| Endpoint | Purpose |
|---|---|
| `POST /v1/query/events` | tenant-filtered events, keyset cursor, `limit` ≤ 1000, range ≤ 31 days |
| `POST /v1/query/aggregate` | whitelisted `metrics` (count, bytes, distinct_entities, distinct_batches) and `group_by` (source_id, event_type, hour, day) |
| `GET /v1/metrics/summary`, `GET /v1/quality/summary` | latest revision (`argMax(.., revision)`) of the hourly/daily aggregates written by `ch-aggregator` |

Rules: tenant comes only from the verified token; `processing_version` is optional only when `QUERY_PG_URL` is set (see below), else required; all values are bound ClickHouse
parameters; queries run `readonly=1` with time/memory/row limits; duplicates are removed with `LIMIT 1 BY`.

## Active processing version (replay cutover)
With `QUERY_PG_URL` set (PostgreSQL role WITHOUT BYPASSRLS; table `source_active_versions`, migration 0010, written by `replay-controller`
`POST /v1/replays/{id}/activate`) `processing_version` may be omitted on all four read endpoints:
- explicit `processing_version` always wins (inspect a replay before cutover); the resolver is not consulted;
- omitted + `source_id` -> that source's active version; the lookup is one read-only transaction with `set_config('astryon.tenant_id', <verified token tenant>, true)`, so RLS applies;
- omitted without `source_id` -> 400 `invalid_request` (a tenant-wide read would mix versions of different sources);
- source without a row (or a source of another tenant) -> 400 `no_active_version`. No default of 1: a missing cutover must be explicit;
- results (also "no version") are cached per tenant+source for `QUERY_VERSION_CACHE_SECONDS` (default 5, bounded to 10000 entries), so a cutover is visible within that time on every replica; PG errors are not cached (502 `backend_error`).
`QUERY_PG_URL` unset: `processing_version` stays required exactly as before.

## Run
Env: `LISTEN_ADDR` (:8082), `CH_URL`, `CH_DATABASE`, `CH_USER`, `CH_PASSWORD`.
Auth (token needs scope `astryon:query:read` and a tenant claim): `AAS_BASE_URL`, `AAS_ISSUER`, `AAS_AUDIENCE`, `AAS_ENV`,
`AAS_ALLOWED_AZP`. Optional: `QUERY_PG_URL`, `QUERY_VERSION_CACHE_SECONDS`. Dev only: `QUERY_DEV_TOKEN` + `QUERY_DEV_TENANT`. With neither, every request is rejected.

Tenant protection (DPP-050; in-memory per instance unless `QUERY_REDIS_URL` is set, keyed only by the verified token's tenant, idle tenants evicted;
over limit -> HTTP 429 `rate_limited` with `Retry-After`, or `too_many_concurrent_queries`):

| Env | Default | Meaning |
|---|---|---|
| `QUERY_RATE_PER_SEC` / `QUERY_BURST` | 10 / 20 | per-tenant token bucket |
| `QUERY_MAX_CONCURRENT` | 8 | in-flight queries per tenant |
| `QUERY_IDLE_TTL_SECONDS` / `QUERY_MAX_TENANTS` | 600 / 10000 | eviction of idle tenant state / hard bound |
| `QUERY_MAX_EXECUTION_SECONDS` | 30 | ClickHouse `max_execution_time` |
| `QUERY_MAX_RESULT_ROWS` | 1001 | ClickHouse `max_result_rows` |
| `QUERY_MAX_MEMORY_BYTES` | 2000000000 | ClickHouse `max_memory_usage` |

Invalid values (non-numeric, <= 0) abort startup. Without `QUERY_REDIS_URL` limits are per instance: N replicas allow N times the rate.

Shared limits across replicas (optional, Redis):

| Env | Default | Meaning |
|---|---|---|
| `QUERY_REDIS_URL` | unset | `redis://` or `rediss://[user:password@]host:port[/db]` (password only via env). Unset = per-instance limits, behaviour unchanged |
| `QUERY_REDIS_CA_FILE` | unset | `rediss://` only: PEM bundle of a private CA; unset = system roots. Certificate and host name are always verified (there is no insecure switch) |
| `QUERY_REDIS_TLS_SERVER_NAME` | host of the dialled node | `rediss://` only: name to verify against the node certificate (set when nodes are dialled by IP or an alias) |
| `QUERY_REDIS_NODES` | unset | Redis Cluster: extra seed nodes `host:port,host:port` (the URL host is the first seed). Cluster mode needs no flag: `-MOVED`/`-ASK` are followed automatically |
| `QUERY_REDIS_TOPOLOGY_REFRESH_SECONDS` | 60 | Cluster: periodic `CLUSTER SLOTS` refresh of the slot -> master map (0 = periodic refresh off; error-triggered refresh stays on). Sentinel mode caps it at 5 |
| `QUERY_REDIS_SENTINELS` + `QUERY_REDIS_MASTER_NAME` | unset | Sentinel: `host:port,...` of the sentinels and the monitored master name (both required, not combinable with `QUERY_REDIS_NODES`); `QUERY_REDIS_URL` still supplies scheme, credentials and db |
| `QUERY_REDIS_FAILURE` | `local` | Redis unreachable: `local` = fall back to the in-memory limiter, `closed` = reject with 503 `limiter_unavailable` (+ `Retry-After: 1`) |
| `QUERY_REDIS_LEASE_SECONDS` | 60 | concurrency-slot lease; set above the longest query (`QUERY_MAX_EXECUTION_SECONDS`) |

Semantics: one Lua script (EVALSHA, EVAL fallback on NOSCRIPT) runs atomically per request on keys `q:rate:{<tenant_id>}`
(token bucket, TTL) and `q:conc:{<tenant_id>}` (sorted set of lease expiries); the tenant is only the verified token's tenant.
The `{tenant}` hash tag puts both keys of a tenant in one Cluster slot. These key names differ from earlier builds
(`q:rate:<tenant_id>`): old keys are simply orphaned and expire by TTL, so a rolling upgrade resets each tenant's bucket
once and, while old and new replicas coexist, the two generations count separately (ephemeral, no migration needed).
Cluster: the client keeps a slot -> node cache refreshed on `-MOVED`, retries `-ASK` once on the target after `ASKING`
(not cached), follows at most 3 redirects per command, and tries the other seed nodes if a node is unreachable. Single-node
behaviour is unchanged.
It uses the Redis server clock, so replica clock skew does not matter, and a token is consumed only if the request is
admitted. A slot is released on completion. While a request is in flight its lease is renewed in the background every
lease/3 (a second small Lua script, server clock) until release or request-context cancel, so a query longer than the
lease keeps counting against the cap; renewal never resurrects a released or already expired slot. A crashed or
partitioned replica stops renewing, so its slots expire after at most one lease instead of leaking. Renewal failures are
retried quietly; if Redis is unreachable for more than a lease the slot lapses.
Failure policy trade-off: `local` keeps serving but during an outage each replica enforces the full rate itself (N times
the limit, i.e. today's behaviour); `closed` keeps the global limit exact but the API returns 503 while Redis is down.
After a failure Redis is skipped for 2 s (no per-request timeout storm; this is per Limiter, not per node, so a failed master
also pauses Redis for tenants on healthy masters for that window). Fallbacks/rejections are counted
(`Limiter.SharedStats()`) and logged at most once per minute. Not supported: ACL-restricted users without EVAL, replica reads.

Topology (`redis_topology.go`): the slot -> master map comes from `CLUSTER SLOTS` (only each range's master is used; replicas are
never routed to, the limiter must read what it writes; empty/null/`?` hosts mean the answering node's host), `-MOVED` entries
override it until the next refresh. Refresh triggers: a dial failure, an I/O error/timeout, `-READONLY`, `-CLUSTERDOWN`,
a `-MOVED` storm (8 since the last refresh), and every `QUERY_REDIS_TOPOLOGY_REFRESH_SECONDS` (lazy, no goroutine; at most one
refresh in flight; attempts >= 1 s apart; at most 8 nodes asked per attempt; standalone Redis answers "cluster support disabled" and
refreshing stops). A command is re-sent only when it provably did not run (dial failure, MOVED/ASK, READONLY/CLUSTERDOWN/TRYAGAIN
rejections, at most 3 redirects and 2 retries); after a mid-command I/O error it is not re-sent (the script may have run) and the
caller's failure policy applies. Sentinel mode: `SENTINEL get-master-addr-by-name` is asked of each sentinel in turn (plaintext
unless `rediss://`, no sentinel AUTH), the result is one range covering all slots; it re-resolves on connection loss, `-READONLY`
and every <= 5 s. Sentinel passwords/ACLs and sentinel quorum cross-checks are not implemented (first answering sentinel wins).

Behaviour during failover (measured live, Redis 7 6-node cluster `cluster-node-timeout 2000`, one Limiter, 18 tenants x 40 req/s
attempted vs rate 20/s burst 20, `docker stop` of a master that owns ~5-7 of the tenants):
Redis path for the victim's slots unavailable 2.5 s (4-8 failed 50 ms probes) in one run and 0.7-0.8 s (14-16 probes, first failure only
after the 1.5-4 s container shutdown) in another; the replica was promoted and the client followed after the next refresh.
`closed`: 2664 of ~23k requests (all tenants, ~3.7 s worth: outage + the 2 s Limiter cooldown) answered 503, 0 fallbacks.
`local`: 3303 requests served by the local fallback (~4.6 s worth), 0 503s. Admission per tenant stayed within
rate*elapsed + burst in closed mode (max 628 vs 20*32.6+20 = 672) and exceeded it only by the fallback bucket in local mode
(max 791 vs 20*34.8+20 = 716; test bound = rate*T + 3 bursts + rate*(downtime+4 s) = 852). Stated tolerance: closed = rate*T + burst
(+1 burst if the async replica lacks the last writes); local = that + burst + rate * (outage + 2 s cooldown) per instance, and N instances
multiply the local part by N (same as any Redis outage). A holder killed with the master (lease 10 s): after the failover the slot was
still present (replicated) and expired on the promoted master within one lease; it never leaks. Resharding (`redis-cli --cluster reshard`
of 2000 slots under load, both directions, 15-40 s): 14-22 MOVED and 1-2 ASK followed, 0 503s, max admitted per tenant 397 vs
rate*T+burst = 419 (closed) and 889 vs 933 in the second round (local; 1127 requests fell back, cause not analysed - likely redirects/TRYAGAIN exhausting their bounds) - no tenant
beyond rate*T + 2 bursts. The old master rejoined as a connected replica and received no limiter commands (EVALSHA count unchanged).
Sentinel (1 master + 1 replica + 3 sentinels, `SENTINEL failover` under load): client switched to the promoted master 2.2 s after the
command; closed 809 x 503 (one cooldown), local 809 fallbacks, within rate*T + burst. Without the 5 s cap the switch took 11 s (the
demoted master only answers -READONLY once Sentinel reconfigures it, ~10 s); until then the old master keeps serving, so Sentinel mode
can split a tenant's state across two masters for that window (tolerance: one extra burst after the switch).
Out of scope: Sentinel with AUTH on the sentinels, multi-sentinel majority checks, reading from replicas, `cluster-announce-*` setups
where the announced address is unreachable from query-api (use `QUERY_REDIS_NODES` with reachable aliases or fix the announce).

    docker build -f services/query-api/Dockerfile -t astryon/query-api .   # context = repo root

Verified: unit tests, fake-AAS e2e test, live ClickHouse 24.8, image smoke test (401 without credentials); shared limits
with a fake RESP server (unit) and a real Redis 7 (`QUERY_REDIS_IT_URL=redis://127.0.0.1:16379 go test ./internal/query`,
two Limiter instances: combined rate ~= configured rate, cap holds across instances, killed holder's slot expires,
tenants isolated). Also live-verified (this change): `rediss://` against Redis 7 with a throwaway private CA
(`QUERY_REDIS_IT_TLS_URL` + `QUERY_REDIS_IT_TLS_CA`) and a 3-master Redis 7 Cluster (`QUERY_REDIS_IT_CLUSTER=host:port,...`;
`MOVED` followed for ~2/3 of tenants seeded from a single node), each with the same four scenarios plus a long query
keeping its slot for 2.5 leases while a never-renewed holder's slot expires. Docker Desktop cannot route to cluster
node IPs from the host, so the cluster test binary is cross-compiled (`CGO_ENABLED=0 GOOS=linux go test -c`) and run in
a container on the same user-defined network as the nodes (failover tests: `QUERY_REDIS_IT_FAILOVER`/`QUERY_REDIS_IT_CTL`/`QUERY_REDIS_IT_SENTINELS`, a host-side loop executes the `docker stop|start` requests the test writes to a shared dir). Renewal, `ASK`/`ASKING`, redirect bounds, seed fallback and
TLS verification failures (unknown CA, wrong name, plaintext/TLS mismatch) are verified against the fake RESP server only
(a real `ASK` needs a live slot migration; not exercised).
Active version: unit tests with fakes (resolver cache/TTL, explicit wins, `no_active_version`, cross-tenant, all four endpoints) and live PostgreSQL 16 (`QUERY_PG_IT_URL`, non-superuser NOBYPASSRLS role, migrations 0001-0010): cross-tenant source never resolves, RLS hides other tenants' rows.
Live-verified also: cluster master failover (both failure modes), resharding with ASK/MOVED under load, old-master rejoin, Sentinel failover, expiry of a killed holder's slot (`redis_failover_live_test.go`); fake-only: AUTH, periodic-refresh timing, `-CLUSTERDOWN`, hostname/null-host `CLUSTER SLOTS` quirks, Sentinel reply edge cases. Not verified: real AAS, multi-shard ClickHouse, AUTH against a real server, TLS cluster failover.
