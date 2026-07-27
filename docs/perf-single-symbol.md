# Single-symbol matching: recommended config and bench results

**中文：** [perf-single-symbol.zh-CN.md](./perf-single-symbol.zh-CN.md)

**Date:** 2026-07-27  
**Scope:** one Raft group (`--groups 1`), file log, `sync_level = 1`, in-process 3 voters  
**Related:** [perf.md](./perf.md) · [M4 sync=1](./specs/2026-07-22-sync1-disk-pipeline-merge.md) · [hotpath highlights](./spotlight/2026-07-hotpath-sync1.md)

This note is for **one trading symbol per Raft group**. Multi-group layout is for isolation / scale-out shape, not a free TPS multiplier on a single symbol.

---

## Recommended production-oriented config

| Knob | Value | Notes |
|------|-------|-------|
| Groups | `1` | One symbol → one group |
| `file_log_sync_level` | `1` (`Data` / fdatasync) | Keep durable grade for matching HA |
| `max_payload_entries` | `8192`–`12288` | Main lever past the default-300 wall |
| Client pipeline | `128×256` or `192×256` | `concurrency × batch_size` |
| `file_log_coalesce_us` | `0` | Short hold windows rarely help at high pe |
| `hold_overlap` | off | Avoid idle waits when replication batch is already fat |
| Propose API | `propose_batch` | Prefer over `propose_many` for wall TPS |

### Default recipe (balanced)

```bash
cargo build -p multiraft-demo --release
RUST_LOG=error ./target/release/multiraft-demo --mode bench --nodes 3 --groups 1 \
  --bench-file-log --bench-file-sync-level 1 \
  --bench-ops 48000 \
  --bench-concurrency 192 --bench-batch-size 256 \
  --bench-file-coalesce-us 0 \
  --bench-max-payload-entries 12288
```

### Peak probe (lab / ceiling, not a default)

```bash
# pe=16384, pipeline 256×384 — highest median TPS in the 2026-07-27 sweep
  --bench-concurrency 256 --bench-batch-size 384 \
  --bench-max-payload-entries 16384
```

### Application notes

- Timeouts and partial `propose_batch` failures need **idempotent retry**.
- Bench `p50` / `p99` with `batch_size > 1` are **batch-wall / N** (amortized), not single-entry RTT.
- Absolute numbers vary by machine; use the **relative knees** below.

---

## What not to use as defaults

| Setting | Why |
|---------|-----|
| `max_payload_entries` left at library default **300** | 3-node sync=1 stalls around **~10–13k** TPS |
| `sync_level = 0` / memory log for confirmed matching writes | Higher TPS, weaker durability after ack |
| Pipeline `512×*` | Often raises p99 (over-subscribe) |
| Many groups for one symbol’s wall TPS | File path often **drops** total TPS (disk contention) |

---

## Bench results (this machine, 2026-07-27)

Harness: release `multiraft-demo --mode bench`, 3 nodes, 1 group, file sync=1, `coalesce_us=0`, `ops=48000`. Each cell is the **median of 3 runs** unless noted.

### pe sweep at `128×256` (latency-friendly base)

| pe | median TPS | p50 (µs) | p99 (µs) |
|---:|----------:|---------:|---------:|
| 4096 | 77k | 1491 | 3279 |
| 6144 | 92k | 1069 | 2523 |
| **8192** | **118k** | **959** | **1821** |
| 10240 | 101k | 1110 | 2532 |
| **12288** | **117k** | **672** | **1752** |
| 14336 | 133k | 705 | 1640 |
| 16384 | 125k | 648 | 1502 |
| 20480 | 137k | 777 | 1486 |
| 24576 | 155k | 673 | 1378 |

**pe knee:** raising from 4k → **8k–12k** is the main jump (TPS up, latency down). Beyond ~16k, gains continue but variance grows on this pipeline.

### Pipeline at pe=`12288`

| conc×batch | median TPS | p50 (µs) | p99 (µs) |
|------------|----------:|---------:|---------:|
| 64×128 | 71k | 913 | 1251 |
| 96×192 | 93k | 893 | 1506 |
| 128×256 | 117k | 672 | 1752 |
| **192×256** | **169k** | **766** | **1046** |
| 256×256 | 172k | 1121 | 1368 |
| 256×512 | 198k | 908 | 1145 |
| 384×256 | 169k | 1277 | 2037 |
| 512×256 | 180k | 1678 | 2570 |

### Pipeline at pe=`16384`

| conc×batch | median TPS | p50 (µs) | p99 (µs) |
|------------|----------:|---------:|---------:|
| 128×256 | 125k | 648 | 1502 |
| **192×256** | **235k** | **583** | **725** |
| 256×256 | 270k | 644 | 754 |
| **256×384** | **340k** | **595** | **637** |
| 256×512 | 279k | 622 | 726 |

### Contrast: default pe=300 (earlier single run)

| Recipe | TPS | p50 | p99 |
|--------|----:|----:|----:|
| pe=300, `64×128`, coal=5ms, ops=8k | ~12k | ~2.9ms | ~5.3ms |

---

## How to read the knees

1. **Replication batch (`max_payload_entries`)** fills follower group-commit; default 300 starves it.  
2. **Client pipeline (`conc×batch`)** raises in-flight; past ~`512×*` tail latency often worsens.  
3. Matching HA defaults should sit near **pe 8k–12k + `192×256`**, not the absolute TPS champion.

---

## See also

- Full ceilings and mem / sync=0 tables: [perf.md](./perf.md)  
- Design / durability contract: [2026-07-22-sync1-disk-pipeline-merge.md](./specs/2026-07-22-sync1-disk-pipeline-merge.md)  
