# Multi-group: recommended reading and bench results

**中文：** [perf-multi-group.zh-CN.md](./perf-multi-group.zh-CN.md)

**Date:** 2026-07-27  
**Scope:** in-process 3 voters; sticky proposers (`worker % groups`)  
**Related:** [perf.md](./perf.md) · [single-symbol](./perf-single-symbol.md) · [M4 sync=1](./specs/2026-07-22-sync1-disk-pipeline-merge.md)

Multi-group is for **isolation / many symbols**, not a free total-TPS multiplier. On file logs, more groups usually **lower** aggregate wall TPS (per-group `log.bin` + fsync contention).

---

## How to read the numbers

- **Wall TPS** = successful entries / wall seconds across **all** groups (aggregate).
- Bench latency with `batch_size > 1` is **amortized** (batch wall / N), not single-entry RTT.
- Compare recipes at the same `conc×batch` and `max_payload_entries` (pe); do not mix mem vs file or sync=0 vs sync=1.

---

## File sync=1 (matching HA grade) — 2026-07-27

Harness: release `multiraft-demo --mode bench`, file log, `sync_level=1`, `coalesce_us=0`, `pe=12288`, `ops=48000`. Cells are **median of 3 runs**.

### Pipeline `192×256` (single-symbol default recipe)

| groups | median TPS | p50 (µs) | p99 (µs) | vs groups=1 |
|-------:|----------:|---------:|---------:|------------:|
| **1** | **195k** | 560 | 882 | 1.00× |
| 4 | 143k | 949 | 1236 | 0.73× |
| 8 | 79k | 1818 | 2346 | 0.40× |
| 16 | 7.1k | 2038 | 3620 | 0.04× |

### Pipeline `128×256`

| groups | median TPS | p50 (µs) | p99 (µs) | vs groups=1 |
|-------:|----------:|---------:|---------:|------------:|
| **1** | **128k** | 674 | 1713 | 1.00× |
| 4 | 85k | 994 | 2392 | 0.67× |
| 8 | 47k | 1284 | 3259 | 0.37× |
| 16 | 6.6k | 2961 | 6073 | 0.05× |

**Knee:** under sync=1 + fat pe, **1–4 groups** stay in the same order of magnitude; **8** halves again; **16** collapses (disk / Raft scheduling contention). Prefer **one group per hot symbol** and scale symbols by process/host, not by packing dozens of busy groups onto one disk.

### Recipe (contrast only)

```bash
cargo build -p multiraft-demo --release
RUST_LOG=error ./target/release/multiraft-demo --mode bench --nodes 3 --groups 8 \
  --bench-file-log --bench-file-sync-level 1 \
  --bench-ops 48000 \
  --bench-concurrency 192 --bench-batch-size 256 \
  --bench-file-coalesce-us 0 \
  --bench-max-payload-entries 12288
```

---

## File sync=0 — 2026-07-27 refresh

Harness: `pe` library default (0 → 300), pipeline `8×16`, `ops=16000`, median of 2 runs.

| groups | median TPS | Notes |
|-------:|----------:|-------|
| 1 | ~182k | Close to historical `8×16` single-group band |
| 4 | ~72k | ~0.4× of groups=1 |
| 8 | ~43k | Matches older ~47k row in [perf.md](./perf.md) |
| 16 | ~45k | Flat vs 8 — already disk-bound |

---

## Memory log (earlier ceiling table)

From [perf.md](./perf.md) multi-group wall ceiling (same machine family):

| Scenario | groups | conc×batch | ~wall TPS | Notes |
|----------|-------:|------------|----------:|-------|
| mem baseline | 1 | 4×8 | ~345k | Doc C″ |
| mem push single group | 1 | 32×16 | **~886k** | Peak single-leader pipeline |
| mem multi-group | 8 | 8×8 | ~389k | Slightly above 4×8 single |
| mem multi-group | 16 | 32×16 | ~561k | Best multi-group mem recipe here |
| mem multi-group | 32 | 64×8 | ~290k | Over-subscribed, drops |

Mem can still gain from more groups **if** the client pipeline is spread; the absolute peak remains a **deep single-group** pipeline.

---

## Practical guidance

| Goal | Prefer |
|------|--------|
| Max TPS for one hot symbol (sync=1) | `groups=1`, pe 8k–12k, `192×256` — see [perf-single-symbol.md](./perf-single-symbol.md) |
| Many symbols, isolation | One group per symbol; expect **lower aggregate** TPS per host as group count rises |
| Lab / page-cache only | sync=0 still drops with group count; do not treat as matching HA |

---

## See also

- Single-symbol sync=1 knees: [perf-single-symbol.md](./perf-single-symbol.md)  
- Full ceilings: [perf.md](./perf.md)  
