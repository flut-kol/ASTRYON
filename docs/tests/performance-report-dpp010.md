# DPP-010 performance report: loadgen -> gateway -> Kafka -> clickhouse-sink -> ClickHouse

Run 2026-10-20 on a developer laptop (macOS, 10 logical CPUs, 16 GiB RAM; Docker VM 10 CPUs / 7.65 GiB).
Plan reference: DEVELOPMENT_PLAN.md section 2 (120k events/s peak for a small message, p95 ACK <= 250 ms in LAN,
p95 end-to-end <= 5 s) and section 9 Sprint 02.

## Result summary (honest)
- The 120k events/s target was **NOT met**. The best sustained end-to-end rate (real Kafka `acks=all`, real
  ClickHouse) was about **46k events/s (43 MB/s)**, and even then ack p95 was 1.7 s (target 250 ms).
- Only the **10k** point met the rate; its p95 (274 ms with 64 workers) is slightly above 250 ms.
- 25k was reached at 94% (23.6k/s), 50k at 82% (41k/s), 120k at 39% (46k/s).
- The whole stack (loadgen, gateway, sink, single-node Kafka, single-node ClickHouse) shares one laptop, so
  the host was CPU-saturated (idle 1-5%, ~45-50% sys time) from 25k upward. **These are laptop-bound
  numbers, not production capacity**; they show where the first limits are, not what a 3-node cluster does.

## Setup
- Stack: `infra/docker/docker-compose.dev.yml`, Kafka 3.9.0 single broker (RF=1, plain listener 19092),
  ClickHouse 24.8 single node, project `astryon-bench`. Topic `raw.events.v1` with 6 partitions (24 in the last experiment).
- Gateway: dev auth, `ASTRYON_GATEWAY_MAX_BATCH_EVENTS=1000` (default 100). The hard-coded producer is
  franz-go with `acks=all`, linger 0, `ProduceSync` per event (see `services/gateway/internal/gateway/kafka_publisher.go`,
  `server.go` handleBatch: events of a request are published **sequentially**).
- Sink: `CHSINK_TOPIC=raw.events.v1` (no normalizer exists yet), defaults batch 5000 / 2 s.
- loadgen (`tests/load`): 1 KiB events (`-event-size 1024`; measured ~0.94 KiB per event on the wire, i.e. 1 MB/s ~= 1.07k ev/s), batch 100, single tenant,
  no dup/invalid events, 30 s per rate, concurrency raised per rate (see table).
- Metrics: loadgen JSON report; sink `inserted_rows` via expvar; `docker stats` + `ps`/`top` sampled in a loop. `ps %cpu` on macOS is a decayed
  average, so host process CPU numbers are indicative only (100% = one core).

## Results (30 s each, batch 100, 1 KiB events)
| Target ev/s | Workers | Achieved ev/s | % of target | MB/s | ack p50 / p95 / p99 (ms) | Errors | Sink backlog at end of load | Sink drain after load |
|---|---|---|---|---|---|---|---|---|
| 10k | 32 | 8,858 | 89% | 8.3 | 198 / 524 / 765 | 0 | 3.2k | 1 s |
| 10k | 64 | **9,968** | 100% | 9.3 | 152 / 274 / 328 | 0 | 4.9k | 1 s |
| 25k | 128 | 23,624 | 94% | 22.1 | 321 / 599 / 1233 | 0 | 93k | 5 s |
| 50k | 256 | 40,979 | 82% | 38.3 | 516 / 946 / 1085 | 0 | 721k | 25 s |
| 120k | 512 | 46,259 | 39% | 43.2 | 996 / 1692 / 2091 | 0 | 1.0M | 35 s |

All requests returned 200, all events accepted, 0 failed, and `inserted_rows` equalled `accepted` after each run (no loss
in these runs; final ClickHouse count 5,295,000 across all runs of the session including the experiments below).
The sink catches up within seconds, so end-to-end lag was bounded by drain time (<= 35 s after a 30 s run at 46k/s, i.e. the
sink briefly lags by up to ~1M events; the 5 s e2e SLO was **not** held at 50k+).

### Resource usage (peaks seen while loaded)
| Rate | Kafka container CPU / mem | ClickHouse container CPU / mem | gateway (host, ps %CPU / RSS) | sink | loadgen | host idle (min) |
|---|---|---|---|---|---|---|
| 10k | ~240-265% / ~0.95 GiB | 55-65% / 1.1 GiB | 30-90% / 30 MB (partial samples) | 30% / 50 MB | n/a | 1.6-4.8% |
| 25k | ~255% / 0.97 GiB | 204% / 1.3 GiB | 112% / 78 MB | 44% / 86 MB | 52% / 324 MB | 3.3% |
| 50k | ~150% / 1.0 GiB (24-partition rerun file) | 172% / 1.5 GiB | 166% / 139 MB | 36% / 86 MB | 81% / 641 MB | 2.1% |
| 120k target | ~210% / 1.0 GiB | 229% / 1.7 GiB | 167% / 266 MB | 42% / 81 MB | 124% / 1.07 GiB | 0.9% |

(10k data was sampled with an earlier version of the script that did not label host processes.)

## Experiments to locate the bottleneck
1. **Gateway with fake publisher** (`ASTRYON_DEV_FAKE_PUBLISHER=1`, same loadgen, batch 100, 256 workers, 20 s): 104.8k ev/s (98 MB/s),
   ack p50 1.6 ms. So loadgen + gateway parsing/validation on this laptop can approach ~105k/s without Kafka; the
   ~46k ceiling is not caused by HTTP/validation alone. (Subsequent fake-publisher runs at 512 workers and batch 500 degraded to 20k and 66k ev/s
   with multi-second p95; cause not investigated, possibly memory growth of the fake publisher or CPU contention with the still-running
   sink; treat as inconclusive.)
2. **24 partitions instead of 6** (`kafka-topics.sh --alter`, 50k target, 256 workers): 45.1k ev/s (vs 41.0k), p95 962 ms (vs 946 ms): only a ~10%
   change, within noise for a single broker. Sink drain was 278 s for a 1.0M backlog in that run (vs 25 s): most likely the consumer-group
   rebalance/new-partition assignment after the partition change, not confirmed. Do not draw conclusions about sink throughput from that run.

## Bottleneck analysis
1. **Gateway publish path (primary software bottleneck).** Each event in a batch is a separate `ProduceSync` with linger 0 and
   `acks=all`, executed sequentially inside the request. At 100 events per request the ack latency is ~100 x the per-record broker round trip
   (about 1.5-2 ms on an idle host), so one worker tops out around 500 ev/s and throughput is
   bounded by `workers x batch / latency` (Little's law). Reaching target rates required 64 workers for 10k and 256-512 for 50k+, which
   inflates connections, memory (loadgen 1 GiB at 512) and tail latency. Recommendation: publish a request's events concurrently or as one
   `Produce` of many records (`ProduceSync(ctx, recs...)` or async with a per-request wait group), with a small linger (1-5 ms) and
   larger producer batches. This is the highest-leverage change and lives in `services/gateway`.
2. **Host CPU saturation (everything shares 10 cores).** Above 25k the host had 1-3% idle with ~50% system time (network syscalls,
   Docker VM networking). Kafka ran 2-2.6 cores, ClickHouse 1.7-2.3 cores, gateway 1.1-1.7, loadgen up to 1.2 (JSON build + stats), sink 0.4.
   The plateau of ~46k ev/s at 512 workers (vs the 105k no-Kafka ceiling) is consistent with Kafka + ClickHouse competing for the same
   CPUs. Cannot separate which component is the true limit without separate hosts.
3. **Kafka single broker, acks=all.** With RF=1 the ack cost is a local fsync-less append; the Kafka container reached 2-2.7 cores. RF=3/min ISR=2
   will add replication latency per ack, so the sequential publish path (1) will be hit much harder in the real topology.
4. **Sink / ClickHouse.** Sink consumed at the same rate as ingest (backlog drained in seconds after load, 0 insert failures) with 5000-row/2 s batches,
   ClickHouse used up to 2.3 cores and 1.7 GiB. At 46k ev/s the sink fell behind during the run
   (up to ~1M events, 35 s drain), so e2e latency was far above 5 s at 50k+. The sink is single-instance; its per-poll
   batch size (5000) and 2 s interval were not tuned in this run (no sink-side experiment was done).
5. **loadgen.** Not the limiter up to 50k (80-125% of a core), but it keeps all latencies/IDs in memory (RSS ~1.07 GiB at 120k target, 30 s)
   and with `-rate` set above what the workers can deliver the achieved rate is worker-bound (latency is not corrected for omitted sends). For true 120k from one machine, run several loadgen processes on a separate host.

## Recommendations
- Fix the gateway publish path first (concurrent/batched produce per request, linger 1-5 ms, `batch.max.bytes` >= 256 KiB); re-run this report.
- Partitions: the dev default (6) is fine for one broker; keep the plan value (`plan_partitions: 96` for raw/processed) for production
  so that >= 120 MB/s ingress (120k x ~1 KiB) spreads over the 3+ brokers and 12+ sink consumers; with 120k ev/s x ~1 KiB, plan about 40 MB/s per broker
  on 3 brokers before replication (x3 write amplification at RF=3). Partitions did not matter on one broker in this test (+10%, noise).
- Resources (starting point to be confirmed by a re-run on separate hosts): gateway 4-8 vCPU total across >= 3 replicas, Kafka brokers 8 vCPU / NVMe each, ClickHouse 8+ vCPU per node
  with batched inserts of 50k-100k rows; run >= 3 sink consumers with `CHSINK_BATCH_SIZE` 20k-50k.
- Use loadgen with batch 100 and workers >= `rate x per-batch-latency / 100`; the gateway `ASTRYON_GATEWAY_MAX_BATCH_EVENTS` must be raised if batches > 100 are wanted.
- Add gateway/sink metrics endpoints (Prometheus) so lag and publish latency can be measured without log scraping; this run read sink lag from the expvar counter only.

## Caveats
- Laptop, shared CPU between load generator, services and Docker VM; not representative of production hardware, network or RF=3.
- Single tenant, no dup/invalid events, 30 s runs (no steady-state soak, no Kafka retention/merge pressure). Event size 1 KiB average, one schema.
- `ps %cpu` is a decayed average on macOS; docker stats CPU is per container in percent of one core.
- Sink reads `raw.events.v1` directly because no normalizer exists yet; real pipeline will add stages and latency.
- Ack latency p95 is client-side at the loadgen on loopback and includes loadgen scheduling delay when the host is saturated.
- `kafka-consumer-groups` lag was not sampled; backlog is derived as accepted minus sink `inserted_rows`.

## Reproduce
```bash
export BENCH_DIR=/tmp/astryon-bench      # optional; default ${TMPDIR:-/tmp}/astryon-bench
scripts/dev/bench.sh setup               # compose project astryon-bench: kafka+clickhouse, topics, CH schema, builds, gateway, sink
scripts/dev/bench.sh run 10000  30 100 64
scripts/dev/bench.sh run 25000  30 100 128
scripts/dev/bench.sh run 50000  30 100 256
scripts/dev/bench.sh run 120000 30 100 512
scripts/dev/bench.sh teardown            # kills gateway/sink, removes containers and volumes
```
Fake-publisher experiment: restart the gateway with `ASTRYON_DEV_FAKE_PUBLISHER=1` and run `$BENCH_DIR/loadgen` directly
(`LOADGEN_TOKEN=bench LOADGEN_TENANT_ID=0a1b2c3d-1111-4222-8333-444455556666 loadgen -url http://127.0.0.1:18080/v1/events/batch -rate 120000 -duration 20s -batch 100 -concurrency 256`).
Partition experiment: `BENCH_PARTITIONS=24 scripts/dev/bench.sh setup` (alters `raw.events.v1` before the services start).
No changes were made to `tests/load` or any service code; ports are the dev defaults (Kafka 19092, ClickHouse 18124, gateway 18080, sink metrics 18081).

## Follow-up: batched publish in the gateway (publish-path fix) - re-run 2026-10-20
Change: `handleBatch` no longer does one `ProduceSync` round trip per event. Items are validated first, then all records (events and quarantine records)
go through `BatchPublisher.PublishBatch` (`KafkaPublisher`: franz-go async `Produce` with per-record promises, wait for all). Durability contract is unchanged:
acks=all, idempotent producer, an item is `accepted`/`quarantined` only after its own broker ack, a failed or cancelled record maps to that item's `failed`/`PUBLISH_FAILED`.
New env: `ASTRYON_KAFKA_LINGER` (default 2ms; `0s` restores latency-first behaviour) and `ASTRYON_KAFKA_BATCH_MAX_BYTES` (default 1048576).

Setup identical to the baseline (fresh `scripts/dev/bench.sh setup`, 6 partitions, 30 s per rate, 1 KiB events, batch 100, `ASTRYON_GATEWAY_MAX_BATCH_EVENTS=1000`, same worker counts),
plus `ASTRYON_GATEWAY_METRICS_ADDR=127.0.0.1:18082` via `BENCH_GW_ENV`. Port 18080 was free (checked with lsof). One run per point (no repeats).

### Before / after
| Target ev/s | Workers | Before: ev/s (MB/s) | Before: ack p50 / p95 / p99 ms | After: ev/s (MB/s) | After: ack p50 / p95 / p99 ms | Errors | Sink backlog at end of load | Sink drain | inserted == accepted |
|---|---|---|---|---|---|---|---|---|---|
| 10k | 64 | 9,968 (9.3) | 152 / 274 / 328 | **10,001 (9.35)** | 5.8 / 8.9 / 14.5 | 0 | 3.3k | 2 s | yes (300,100) |
| 25k | 128 | 23,624 (22.1) | 321 / 599 / 1233 | **24,999 (23.4)** | 6.3 / 14.9 / 36.4 | 0 | 5.1k | 1 s | yes (750,000) |
| 50k | 256 | 40,979 (38.3) | 516 / 946 / 1085 | 49,540 (46.3) = 99% | 10.7 / 405 / 721 | 0 | 933k | 31 s | yes (1,486,900) |
| 120k | 512 | 46,259 (43.2) | 996 / 1692 / 2091 | **58,014 (54.2) = 48%** | 670 / 2118 / 3094 | 0 | 1.46M | 56 s | yes (1,776,900) |
| 50k, linger=0s | 256 | n/a | n/a | 38,984 (36.4) | 326 / 1250 / 1908 | 0 | 924k | 30 s | yes (1,180,800) |

All requests returned 200; 0 failed, 0 quarantined; for every run the sink's `inserted_rows` delta equalled `accepted` (no loss).

Results in short:
- The 120k events/s target was **still NOT met**: best was 58k ev/s (48%), i.e. +25% over the 46k baseline, with ack p95 2.1 s.
- The 250 ms p95 ack target is now met at 10k and 25k (9 ms and 15 ms, ~30x better than before) and at 50k throughput is essentially reached (99%) but p95 is 405 ms (not met).
- The 5 s end-to-end SLO is held at 10k and 25k (backlog drains in 1-2 s) but not at 50k+ (up to 1.46M events behind, 31-56 s drain).
- Linger isolation (50k, single runs): ASTRYON_KAFKA_LINGER=2ms gave 49.5k ev/s / p95 405 ms; linger=0s gave 39.0k ev/s / p95 1250 ms. So a small linger helped at this load (fewer, larger produce requests => less broker/CPU overhead on a saturated host),
  although with single 30 s runs on a noisy shared laptop the size of the gap (~27%) should be treated as indicative; the direction matches the CPU-saturation analysis. Linger=0 with batched publish (39k) is roughly equal to the old sequential baseline (41k) at 50k, so most of the benefit at high rate comes from linger plus batching together; at low rate (10k-25k) the pipelined produce is what removes the 100x round trips.

### Resource peaks (docker stats / ps samples, indicative)
| Rate | Kafka CPU | ClickHouse CPU | gateway (ps %CPU / RSS MB) | sink | loadgen (ps %CPU / RSS MB) | host idle (min sample) |
|---|---|---|---|---|---|---|
| 25k | 149% | 176% | 107% / 44 | 41% / 81 | 41% / 299 | not recorded |
| 50k (2ms linger) | 76% | 134% | 178% / 41 | 35% / 92 | 85% / 617 | 1.2% (user 57 / sys 42) |
| 120k target | 125% | 209% | 202% / 484 | 36% / 76 | 175% / 1225 | 1.7% (user 49 / sys 49) |

### Live metrics check
Both endpoints work live. During the 25k run (about 15 s in), gateway `/metrics` showed `astryon_gateway_events_accepted_total` 650,100 and `astryon_gateway_ack_duration_seconds` count 6,501 (cumulative since gateway start, so it includes the preceding 10k run), mean 7.7 ms,
histogram p95 falling in the 5-10 ms bucket (95% of requests under 10 ms; 5,897 of 6,501 <= 10 ms, 6,412 <= 25 ms). Loadgen's overall 25k p95 was 14.9 ms and 10k p95 8.9 ms: consistent, the loadgen figure being slightly higher because it
includes client scheduling and loopback. At the 50k linger=0 run mid-run scrape (6,407 requests so far), the gateway histogram put p95 in the 0.5-1 s bucket (6,155 <= 1 s; 5,325 <= 0.5 s), vs loadgen final p95 1.25 s: same ballpark (scrape was mid-run, before the peak backlog tail).
Sink `/metrics` mid-run: `consumed_total` = `inserted_rows_total` = 641,800, 127 batches, 127 commits, 0 retries, 0 insert failures, 0 commit failures.

### Bottleneck re-analysis (honest)
- The sequential publish path is no longer the limiter. At 10k-25k each request now acks in ~6 ms (versus 150-320 ms before), so far fewer workers would suffice (Little's law: 25k ev/s x 6 ms / 100 = ~1.5 in-flight requests).
  The worker counts used here are the baseline's and are far more than needed at those rates; the high worker counts at 50k+ now mainly create queueing on a saturated host rather than being required.
- From 50k up the host is saturated: `top` showed 1-2% idle with 42-49% system time, and p50 jumps from ~10 ms (50k) to 670 ms (120k target) while throughput only rises from 49.5k to 58k.
  Loadgen (up to 1.75 cores, 1.2 GiB), gateway (up to 2 cores), Kafka (up to 1.25 cores), ClickHouse (up to 2.1 cores), sink and the Docker VM all share the same 10 cores. The remaining ceiling is
  therefore "everything on one laptop", not the gateway code path; we cannot attribute it to one component without separate hosts. The earlier fake-publisher ceiling of ~105k ev/s (loadgen + gateway) shows that
  loadgen and the gateway alone would consume a large share of the machine at 120k, leaving little for Kafka and ClickHouse.
- Kafka CPU was lower than in the baseline (peak 125-150% vs 210-255%): fewer, larger produce requests cost the broker less per event.
- The sink keeps up while the host has headroom (drain 1-2 s at <= 25k) but the backlog grows linearly at 50k+ (the sink's single consumer with 5000-row/2 s batches, competing with the loadgen for CPU); the sink was not tuned in this re-run, so the 5 s e2e SLO at 50k+ remains unproven.
- Caveats: one 30 s run per point; the 50k linger comparison is a single pair; ps CPU is a decayed average; the 120k run lasted 30.6 s and the sink drain times include the shared-CPU contention of a just-finished loadgen.

### Updated recommendations
1. Keep batched publish with 2 ms linger as default; it removes the per-event round trip and helps under saturation. Keep `ASTRYON_KAFKA_LINGER=0s` available for latency-first deployments, but note it was ~20% slower at 50k here.
2. Lower loadgen concurrency at low rates (about 8-16 workers suffices for 10k-25k now) to avoid queueing noise, and run loadgen (several processes) on a separate host to test 120k fairly; the current numbers cannot show whether the gateway/Kafka/ClickHouse path scales to 120k.
3. Next measurement: separate hosts (or at least pin loadgen away from Kafka/ClickHouse), RF=3/min ISR=2 Kafka to measure replication ack latency under the batched path, then tune the sink (`CHSINK_BATCH_SIZE` 20k-50k, multiple consumers) to hold p95 e2e <= 5 s above 50k.
4. Keep the earlier sizing guidance (96 partitions in production, 3+ brokers, >= 3 gateway replicas, >= 3 sink consumers); repeat this report with several runs per point to get variance.
5. Metrics endpoints now exist (gateway `ASTRYON_GATEWAY_METRICS_ADDR`, sink `CHSINK_METRICS_ADDR`): use `astryon_gateway_ack_duration_seconds` and sink `consumed/inserted` counters for lag instead of the expvar approximation. (Kafka consumer-group lag is still not sampled.)

Reproduce the re-run: `BENCH_GW_ENV="ASTRYON_GATEWAY_METRICS_ADDR=127.0.0.1:18082" scripts/dev/bench.sh setup`, then the same `run` commands as above; for the linger experiment restart only the gateway with `ASTRYON_KAFKA_LINGER=0s` and repeat `run 50000 30 100 256`.
