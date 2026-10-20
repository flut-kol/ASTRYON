# Performance / workload profile and generator (DPP-002)

Plan refs: §9 DPP-002, §11 (PERF-01..08). Generator: `tests/load` (`loadgen`, module `github.com/astryon/loadgen`). All rates below are **targets from the plan**, not measured results; nothing here claims achieved throughput. The only measured run is the 500 ev/s smoke in `e2e-smoke.md`.

## Required profile (DPP-002) vs. loadgen capability

| Plan requirement | loadgen today | Gap |
|---|---|---|
| Small JSON (~1 KiB) | Yes: `-event-size` (default 1024), payload padded to approx size | none |
| Wide JSON | Partly: larger `-event-size` pads payload, but flat padding, not many fields / nested structure | **gap**: no wide/nested shape, no field-count control; gateway batch limit (default 100 events/request) also bounds total request size |
| Files (CSV/JSON/XML/Parquet, S3 + object_ref) | No: only POST batch to gateway | **gap**: no file generator, no S3 upload, no `object_ref`/checksum events, no filejob topic path |
| Skewed tenant distribution | Yes: `-tenant-skew` (Zipf exponent) with several tenants in `LOADGEN_TENANTS` | PERF-06 asks tenant X at 80%; need to map Zipf exponent to a fixed share or add `-hot-tenant-share` |
| Duplicates | Yes: `-dup-ratio` re-sends earlier event (same event_id and bytes); `-ids-out` lists unique accepted ids | no cross-run / cross-restart duplicate (replay) mode |
| Late events | No: `event_time` is generated at send time | **gap**: need `-late-ratio` and lateness distribution (event_time in the past, out-of-order within and beyond the dedup/window horizon) |
| Invalid schemas | Yes: `-invalid-ratio` (expects quarantine) | single invalid kind mix is generator-defined; no per-kind control (bad schema version, bad timestamp, oversize) |
| Rate / duration / concurrency / batch | Yes: `-rate`, `-duration`, `-concurrency`, `-batch` | open-loop vs closed-loop behaviour at 120k ev/s needs verification; one process may not reach plan peaks (multi-process distribution is a gap) |
| Metrics: events/s, MB/s, p95 ack, failures | Yes: text + JSON (`-json-out`) | **gap**: Kafka lag, sink throughput, ClickHouse parts/merges are not collected by loadgen; must come from Prometheus/dashboards or scripts |
| Reconciliation | Partial: `-ids-out` for comparison against ClickHouse `uniqExact(event_id)` | no automated reconciliation script |

## Proposed generator mix (hypothesis, to confirm in POC)
Starting point only; real mix comes from the source catalog (`../architecture/source-catalog.md`).

| Component | Share (hypothesis) |
|---|---|
| small JSON ~1 KiB | majority |
| wide JSON 10-100 KiB | small |
| duplicates | 1-5 % |
| late events | 1-5 % |
| invalid | <1 % |
| tenant skew | one hot tenant 80 % (PERF-06) |

## Mapping to plan load tests
PERF-01..04 (10k/25k/50k/120k ev/s at 1 KiB) are expressible with loadgen flags, subject to single-host limits. PERF-05 (24 h mix) needs late/wide/file generation. PERF-06 needs the hot-tenant share. PERF-07 (1+ TB backfill) and PERF-08 (query clients) are outside loadgen.

## Follow-up work (loadgen owner, not this doc)
Add late-event mode, wide/nested JSON shapes, file/object generator, hot-tenant share flag, distributed runs, reconciliation helper.
