# 单币对撮合：推荐配置与压测结果

**English：** [perf-single-symbol.md](./perf-single-symbol.md)

**日期：** 2026-07-27  
**范围：** 单 Raft Group（`--groups 1`）、文件日志、`sync_level = 1`、进程内 3 voter  
**相关：** [perf.zh-CN.md](./perf.zh-CN.md) · [M4 sync=1](./specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md) · [热路径亮点](./spotlight/2026-07-hotpath-sync1.zh-CN.md)

本文针对 **一个交易对对应一个 Raft Group**。多 Group 用于隔离与扩展形态，不能指望靠多 Group 自动抬高单币对墙钟吞吐。

---

## 推荐配置（生产取向）

| 旋钮 | 取值 | 说明 |
|------|------|------|
| Groups | `1` | 单币对 → 单 Group |
| `file_log_sync_level` | `1`（Data / fdatasync） | 撮合 HA 保留本机 data sync |
| `max_payload_entries` | `8192`–`12288` | 突破默认 300 天花板的主杠杆 |
| 客户端流水线 | `128×256` 或 `192×256` | `concurrency × batch_size` |
| `file_log_coalesce_us` | `0` | 高 pe 时短窗口通常无益 |
| `hold_overlap` | 关 | 复制批已够肥时避免空等 |
| Propose API | `propose_batch` | 墙钟吞吐优先于 `propose_many` |

### 默认配方（均衡）

```bash
cargo build -p multiraft-demo --release
RUST_LOG=error ./target/release/multiraft-demo --mode bench --nodes 3 --groups 1 \
  --bench-file-log --bench-file-sync-level 1 \
  --bench-ops 48000 \
  --bench-concurrency 192 --bench-batch-size 256 \
  --bench-file-coalesce-us 0 \
  --bench-max-payload-entries 12288
```

### 摸顶探针（实验室，不作日常默认）

```bash
# pe=16384，流水线 256×384 — 2026-07-27 扫点中位 TPS 最高
  --bench-concurrency 256 --bench-batch-size 384 \
  --bench-max-payload-entries 16384
```

### 应用侧注意

- 超时与 `propose_batch` 部分失败须 **幂等重试**。  
- `batch_size > 1` 时 bench 的 p50/p99 是 **整批墙钟 / N（均摊）**，不是单笔 RTT。  
- 绝对值随机器变化；以下方 **相对拐点** 为准。

---

## 不建议作为默认

| 设置 | 原因 |
|------|------|
| `max_payload_entries` 保持库默认 **300** | 3-node sync=1 约卡在 **~1.0–1.3 万** TPS |
| 已确认撮合写用 `sync_level = 0` / 内存日志 | 吞吐更高，确认后耐久更弱 |
| 流水线 `512×*` | 易抬高 p99（过订阅） |
| 为单币对墙钟 TPS 开很多 Group | file 路径总 TPS 常 **下降**（抢盘） |

---

## 压测结果（本机，2026-07-27）

Harness：release `multiraft-demo --mode bench`，3 节点、1 Group、file sync=1、`coalesce_us=0`、`ops=48000`。表内为 **3 次中位**（另有说明除外）。

### pe 扫描（固定 `128×256`）

| pe | 中位 TPS | p50 (µs) | p99 (µs) |
|---:|----------:|---------:|---------:|
| 4096 | 7.7 万 | 1491 | 3279 |
| 6144 | 9.2 万 | 1069 | 2523 |
| **8192** | **11.8 万** | **959** | **1821** |
| 10240 | 10.1 万 | 1110 | 2532 |
| **12288** | **11.7 万** | **672** | **1752** |
| 14336 | 13.3 万 | 705 | 1640 |
| 16384 | 12.5 万 | 648 | 1502 |
| 20480 | 13.7 万 | 777 | 1486 |
| 24576 | 15.5 万 | 673 | 1378 |

**pe 拐点：** 从 4k 提到 **8k–12k** 是主跃迁（吞吐升、延迟降）。超过约 16k 仍可能更高，但在该流水线上抖动变大。

### 流水线扫描（pe=`12288`）

| conc×batch | 中位 TPS | p50 (µs) | p99 (µs) |
|------------|----------:|---------:|---------:|
| 64×128 | 7.1 万 | 913 | 1251 |
| 96×192 | 9.3 万 | 893 | 1506 |
| 128×256 | 11.7 万 | 672 | 1752 |
| **192×256** | **16.9 万** | **766** | **1046** |
| 256×256 | 17.2 万 | 1121 | 1368 |
| 256×512 | 19.8 万 | 908 | 1145 |
| 384×256 | 16.9 万 | 1277 | 2037 |
| 512×256 | 18.0 万 | 1678 | 2570 |

### 流水线扫描（pe=`16384`）

| conc×batch | 中位 TPS | p50 (µs) | p99 (µs) |
|------------|----------:|---------:|---------:|
| 128×256 | 12.5 万 | 648 | 1502 |
| **192×256** | **23.5 万** | **583** | **725** |
| 256×256 | 27.0 万 | 644 | 754 |
| **256×384** | **34.0 万** | **595** | **637** |
| 256×512 | 27.9 万 | 622 | 726 |

### 对照：默认 pe=300（此前单次）

| 配方 | TPS | p50 | p99 |
|------|----:|----:|----:|
| pe=300，`64×128`，coal=5ms，ops=8k | ~1.2 万 | ~2.9ms | ~5.3ms |

---

## 如何读拐点

1. **复制批（`max_payload_entries`）** 决定 follower 组提交能否喂饱；默认 300 会饿死。  
2. **客户端流水线（`conc×batch`）** 加深在途；到约 `512×*` 后尾延迟常变差。  
3. 撮合 HA 日常配置宜落在 **pe 8k–12k + `192×256`**，而不是绝对 TPS 冠军点。

---

## 参见

- 全量上限与 mem / sync=0：[perf.zh-CN.md](./perf.zh-CN.md)  
- 设计与耐久契约：[2026-07-22-sync1-disk-pipeline-merge.zh-CN.md](./specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)  
