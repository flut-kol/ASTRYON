# Performance report v0 (first measurement of the real chain)

Measured 2026-10-20 with `tests/e2e` `TestPerfReport` (`ASTRYON_E2E=1 ASTRYON_E2E_PERF=1`), driver `tests/load` loadgen.
Raw JSON of the run: not committed (reproducible with the command at the bottom). **These are single-laptop numbers, not the
plan's targets (10k..120k ev/s on a cluster); they must not be read as capacity claims.**

## Setup
- Host: Apple M2 Pro, 10 cores, 16 GiB RAM, macOS (Darwin 25.6.0 arm64), Go go1.27.3. Docker Desktop VM: 10 CPUs / 7.65 GiB.
- Single-node Kafka 3.9.0 (KRaft, RF=1; raw.events.v1 has 6 partitions) and ClickHouse 24.8 in Docker; PostgreSQL up but idle.
- gateway, stream-processor, clickhouse-sink, query-api and loadgen run natively on the same host, one instance each, sharing all 10 cores.
- Tuning used: stream-processor `BATCH_INTERVAL=100ms` (default 500ms), sink `BATCH_INTERVAL=200ms` (default 2s, batch 5000), dedup defaults
  (1 h / 2M entries), gateway max batch 1000 events. Load: 1 KiB events, 100 events/request, 32 concurrent workers, 30 s per step,
  2 % exact duplicates, 1 % invalid, one tenant, 4 sources.
- **Not a clean machine**: another agent's benchmark stack (separate Kafka/ClickHouse containers plus its own gateway/sink processes) was
  running on the same laptop during the run; load average was 4.0 at start and 12.7 at end. Numbers are therefore pessimistic and noisy.
- Each step starts a fresh tenant, gateway and query-api; the stream-processor and sink are shared and keep running between steps
  (the dedup window therefore accumulates across steps: see below). One run per step; no repetitions, no confidence intervals.

## Throughput (loadgen -> ClickHouse)
| Target ev/s | Sent ev/s | Accepted ev/s | Ack latency p50 / p95 / p99 (ms) | Unique accepted | Rows in CH | End-to-end ev/s (incl. drain) |
|---|---|---|---|---|---|---|
| 2,000 | 2,000 | 1,979 | 69 / 247 / 384 | 58,155 | 58,155 | 1,898 |
| 5,000 | 4,989 | 4,938 | 157 / 432 / 549 | 145,498 | 145,498 | 4,782 |
| 10,000 | 9,432 | 9,338 | 155 / 254 / 1,160 | 276,293 | 276,293 | 9,028 |
| 20,000 | 12,730 | 12,601 | 226 / 383 / 523 | 372,123 | 372,123 | 12,167 |
| unlimited | 10,176 | 10,072 | 265 / 681 / 822 | 297,980 | 297,980 | 9,738 |

No request failed in any step. In every step ClickHouse ended with exactly the unique accepted events (duplicates dropped, invalid excluded).
Achieved ceiling on this host: about 12-13k ev/s accepted (20k target not reached; the unlimited run was lower, i.e. noise / contention at
saturation). The gateway process was the first to saturate (about 90 % of a core, peak sampled 89 %), then Kafka (about 100 % of a core) and ClickHouse (up to 113 %).
Whether that is a hard limit of the gateway design or of the shared laptop was not determined.

## End-to-end latency and lag
Probe = one extra event posted through the same gateway every 500 ms during the load, timed from send until it is visible in ClickHouse (polled every 50 ms,
so +/- 50 ms plus poll query time). 33-42 probes per step, 0 timeouts. Percentiles over so few samples are indicative only (p99 ~ max).

| Target ev/s | Probe p50 / p95 / p99 (ms) | Freshness (now - newest ingested_at) p50 / p95 (ms) | Max consumer-group lag: stream-processor / sink (records) |
|---|---|---|---|
| 2,000 | 232 / 301 / 335 | 130 / 587 | 64 / 676 |
| 5,000 | 312 / 413 / 440 | 196 / 382 | 329 / 1,809 |
| 10,000 | 426 / 504 / 1,237 | 235 / 428 | 354 / 4,094 |
| 20,000 | 247 / 428 / 556 | 267 / 565 | 543 / 7,528 |
| unlimited | 286 / 599 / 638 | 360 / 606 | 695 / 7,586 |

Lag was sampled once per second; the backlog never grew without bound in any step. After the load ended ClickHouse reached the final row count
within the first 0.3 s poll, i.e. no meaningful drain tail at these rates. The lower probe latency at 20k than at 10k is within noise (other tenants' load on the host).
The 100 ms / 200 ms batch intervals dominate the floor (~230 ms at 2k ev/s); with the defaults (500 ms / 2 s) latency would be seconds. The plan's
end-to-end SLO was not evaluated against these numbers.

## Dedup state (stream-processor, FIFO window, entries are never refreshed)
| After step | Entries | Approx bytes (own estimate) | Bytes/entry | Duplicates dropped in step | Processor peak RSS (MB, sampled) |
|---|---|---|---|---|---|
| 2k | 58,197 | 13.1 MB | 225 | 1,227 | 47 |
| 5k | 203,734 | 45.8 MB | 225 | 3,066 | 117 |
| 10k | 480,060 | 108.0 MB | 225 | 5,673 | 263 |
| 20k | 852,223 | 191.8 MB | 225 | 7,813 | 415 |
| unlimited | 1,150,240 | 258.8 MB | 225 | 6,079 | 598 |

State grows linearly with unique events inside the TTL: about 225 B/entry by the processor's own estimate (its README says ~150 B), and the observed RSS
(about 520 B per entry, includes buffers and GC headroom) is roughly twice that again. At the default cap of 2M entries this is on the order of 0.5-1 GB RSS per instance. At 12k ev/s
the 2M-entry cap is reached in about 3 minutes, so the effective dedup window at that rate is minutes, not the configured 1 h: the cap, not the TTL, is the binding limit. Other components'
peak sampled RSS: gateway 38 MB, sink 57 MB. Peak CPU (percent of one core, `ps` samples): gateway 89, stream-processor 132, sink 33.

## query-api latency (against ~1.1M rows total in the table, last tenant ~300k rows, sequential single client)
| Query | n | p50 / p95 / p99 (ms) |
|---|---|---|
| events, 1 h range, limit 100 | 100 | 89 / 116 / 155 |
| events, source filter, limit 100 | 100 | 32 / 36 / 44 |
| aggregate count by source | 50 | 24 / 29 / 29 |
| aggregate hourly series (events, entities) | 50 | 29 / 42 / 43 |

These ran right after the load while ClickHouse was still merging parts, with no concurrent query clients (PERF-08 not covered).

## Caveats / not measured
- Single run per point, shared noisy host, loadgen on the same machine consuming CPU; no warm-up discarded.
- One tenant, uniform sources; no tenant skew, no late events, no wide JSON or files; payload is 1 KiB.
- Not measured: ClickHouse parts/merge pressure, disk size, Kafka broker-side metrics beyond a mid-run `docker stats` snapshot (Kafka 51-101 % CPU, ~1 GiB; ClickHouse 35-113 % CPU, 0.6-1.1 GiB),
  restarts/failover, 24 h soak, backfill, TLS/SASL overhead, multi-instance scale-out.
- End-to-end latency is probe based (a handful of samples per step); there is no per-event insert timestamp in the table.
- Targets from the plan (10k to 120k ev/s sustained, PERF-01..08) are NOT claimed: only ~12.7k ev/s accepted peak was observed, on one laptop.

## Reproduce
```bash
# stack as in mvp-gate.md, then
cd tests/e2e && ASTRYON_E2E=1 ASTRYON_E2E_PERF=1 ASTRYON_E2E_PERF_RATES=2000,5000,10000,20000,0 ASTRYON_E2E_PERF_DURATION=30s \
  ASTRYON_E2E_PERF_CONC=32 ASTRYON_E2E_CONTAINERS=astryon-mvp-kafka-1,astryon-mvp-clickhouse-1 ASTRYON_E2E_PERF_OUT=/tmp/perf.json \
  ASTRYON_E2E_KAFKA=127.0.0.1:29092 ASTRYON_E2E_CH_URL=http://127.0.0.1:28124 ASTRYON_E2E_PG_DSN=... go test -v -timeout 30m -run Perf ./...
```
