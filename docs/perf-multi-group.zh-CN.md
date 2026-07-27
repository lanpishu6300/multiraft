# 多 Group：读法与压测结果

**English：** [perf-multi-group.md](./perf-multi-group.md)

**日期：** 2026-07-27  
**范围：** 进程内 3 voter；proposer 粘性绑定（`worker % groups`）  
**相关：** [perf.zh-CN.md](./perf.zh-CN.md) · [单币对](./perf-single-symbol.zh-CN.md) · [M4 sync=1](./specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)

多 Group 用于 **隔离 / 多交易对**，不是免费的总吞吐放大器。file 路径下 Group 越多，合计墙钟 TPS 通常 **越低**（每组 `log.bin` + fsync 抢盘）。

---

## 如何读数字

- **墙钟 TPS** = 全部 Group 合计成功 entry / 墙钟秒。
- `batch_size > 1` 时的延迟是 **均摊**（整批墙钟 / N），不是单笔 RTT。
- 对比时固定 `conc×batch` 与 `max_payload_entries`（pe）；勿混 mem/file 或 sync=0/1。

---

## File sync=1（撮合 HA 等级）— 2026-07-27

Harness：release `multiraft-demo --mode bench`，file、`sync_level=1`、`coalesce_us=0`、`pe=12288`、`ops=48000`。表内为 **3 次中位**。

### 流水线 `192×256`（单币对默认配方）

| groups | 中位 TPS | p50 (µs) | p99 (µs) | 相对 groups=1 |
|-------:|----------:|---------:|---------:|---------------:|
| **1** | **19.5 万** | 560 | 882 | 1.00× |
| 4 | 14.3 万 | 949 | 1236 | 0.73× |
| 8 | 7.9 万 | 1818 | 2346 | 0.40× |
| 16 | 0.71 万 | 2038 | 3620 | 0.04× |

### 流水线 `128×256`

| groups | 中位 TPS | p50 (µs) | p99 (µs) | 相对 groups=1 |
|-------:|----------:|---------:|---------:|---------------:|
| **1** | **12.8 万** | 674 | 1713 | 1.00× |
| 4 | 8.5 万 | 994 | 2392 | 0.67× |
| 8 | 4.7 万 | 1284 | 3259 | 0.37× |
| 16 | 0.66 万 | 2961 | 6073 | 0.05× |

**拐点：** sync=1 + 大 pe 时，**1–4 Group** 仍同量级；**8** 再腰斩；**16** 塌缩（磁盘 / Raft 调度争用）。热币对应 **单 Group**，多币对靠进程/机器扩，而不是在同一盘上堆大量忙 Group。

### 复现命令（对照用）

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

## File sync=0 — 2026-07-27 复测

Harness：`pe` 库默认（0 → 300），流水线 `8×16`，`ops=16000`，2 次中位。

| groups | 中位 TPS | 备注 |
|-------:|----------:|------|
| 1 | ~18.2 万 | 接近历史单 Group `8×16` 区间 |
| 4 | ~7.2 万 | 约 groups=1 的 0.4× |
| 8 | ~4.3 万 | 与 [perf.zh-CN.md](./perf.zh-CN.md) 旧 ~4.7 万一致 |
| 16 | ~4.5 万 | 相对 8 已平台（盘已饱和） |

---

## 内存日志（早期上限表）

摘自 [perf.zh-CN.md](./perf.zh-CN.md) 多 Group 墙钟上限：

| 场景 | groups | conc×batch | ~墙钟 TPS | 备注 |
|------|-------:|------------|----------:|------|
| mem 基线 | 1 | 4×8 | ~34.5 万 | 文档 C″ |
| mem 单 Group 推高 | 1 | 32×16 | **~88.6 万** | 单 Leader 流水线峰值 |
| mem 多 Group | 8 | 8×8 | ~38.9 万 | 略高于 4×8 单 Group |
| mem 多 Group | 16 | 32×16 | ~56.1 万 | 本机较好多 Group 配方 |
| mem 多 Group | 32 | 64×8 | ~29.0 万 | 过订阅回落 |

mem 在负载摊开时仍可能随 Group 数上升；绝对峰值仍多半是 **深流水线单 Group**。

---

## 实务建议

| 目标 | 倾向 |
|------|------|
| 单热币对最大 TPS（sync=1） | `groups=1`，pe 8k–12k，`192×256` — 见 [perf-single-symbol.zh-CN.md](./perf-single-symbol.zh-CN.md) |
| 多币对隔离 | 一币对一 Group；Group 增多时期待 **单机合计 TPS 下降** |
| 仅实验室 / 页缓存 | sync=0 也会随 Group 数掉；勿当撮合 HA |

---

## 参见

- 单币对 sync=1 拐点：[perf-single-symbol.zh-CN.md](./perf-single-symbol.zh-CN.md)  
- 全量上限：[perf.zh-CN.md](./perf.zh-CN.md)  
