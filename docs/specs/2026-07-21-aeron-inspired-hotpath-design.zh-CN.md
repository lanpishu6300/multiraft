# Aeron 启发热路径（吞吐 / 延迟）

**English：** [2026-07-21-aeron-inspired-hotpath-design.md](./2026-07-21-aeron-inspired-hotpath-design.md)

**日期：** 2026-07-21  
**分支：** `feature/aeron-inspired-hotpath`  
**状态：** 已实现（M1–M3 + **M4** sync=1 深流水线 / 复制批）；在 `feature/aeron-inspired-hotpath` 验证  
**约束：** openraft `=0.10.0-alpha.30`；不做 Media Driver / 整栈替换。

**概述：** [技术亮点 — 热路径与 sync=1](../spotlight/2026-07-hotpath-sync1.zh-CN.md)

## 设计理念

我们优化的是 **openraft Multi-Raft 热路径**，借鉴 Aeron 式 *mechanical sympathy* —— 不是 fork Aeron，也不替换共识。

| 原则 | 在 multiraft 中的含义 |
|------|----------------------|
| **同进程尽量零成本** | 类型化 `RaftCall`/`RaftReply` 走 `mpsc`；同进程无 bincode。跨进程 gRPC 仍序列化。 |
| **流水线，不 mega-entry** | 每条业务数据仍是独立 Raft entry；墙钟 TPS 靠重叠多路 `client_write`（`propose_batch` + 并发）。单线程肥提交用 `propose_many`。 |
| **深流水线填延迟空洞** | \(L\) 到 ms（sync=1）时，顺序 TPS≈\(1/L\)。加深在途 \(N\) 使 \(\mathrm{TPS}\approx N/L\)。mem 的 \(L\) 小，同 \(N\)「自然」高；sync=1 要更深 \(N\) *且* 更大摊销 \(E\)。 |
| **摊销，而不是消灭 fsync** | sync=1 仍付 fdatasync；组提交 / 复制批抬高每次 sync 的 entry 数，使每条成本≈\(t_{\mathrm{sync}}/E\)。 |
| **显式标定持久化强度** | `FileLogSyncLevel` 0/1/2 对齐 Aeron `file.sync.level`。多数派 commit ≠ 本机 fsync。 |
| **先合并再 sync** | 组提交 + double-buffer；defer 时绝不在 openraft `append` 内 await sync。 |
| **复制 \(E\) 一等公民** | openraft 默认 `max_payload_entries=300` 会饿死 follower 组提交；sync=1 深流水线必须显式抬高。 |
| **多层对齐在途深度** | 客户端 `conc×batch`、本地 defer/double-buffer、复制 pe 任一层偏浅都会饿死上层。 |
| **少调度噪声** | Router `try_send`、粘性 worker、standby 节流原子快路径。 |
| **如实面对上限** | 顺序 sync≥1 ≈ fsync×quorum（~25 TPS）。sync=1 破十万靠深在途 + pe≫300，不是单条魔法。 |

**借思想，不借运行时：** standby 节流、快照卸载、sync 档位、流水线 propose。**不交付：** Media Driver、SBE、完整 Archive、Consensus Module。与商业 Aeron 定位见 [compare/aeron-commercial.zh-CN.md](../compare/aeron-commercial.zh-CN.md)。

里程碑自下而上（上层可选；只关心持久化时可只用 M2）：

```text
M4  sync=1 叠满          → 复制批 + disk pipeline（见独立规格）
M3c 少调度               → duty-cycle（try_send、worker）
M3b 深流水线             → 墙钟 TPS（conc × batch）
M3a 流式落盘             → Os sync=0 缓冲追加
M2 / M2b 文件合并 + sync 档位
M1-B propose_batch       → 流水线 client_write
M1-A 进程内 Typed        → 同进程 RPC 零编解码
```

## 目标

| 里程碑 | 场景 | 相对基线 `54b1148` |
|--------|------|-------------------|
| **M1** | 3-node 进程内 mem，顺序 | TPS ≥ 35k，p50 ≤ 30µs |
| **M1** | 同上，concurrency=4 | TPS ≥ 100k |
| **M2** | 3-node file | TPS ≥ 6k，p50 ≤ 200µs |

借鉴 Aeron：**同进程不编解码**、**流水线多笔共识**、**落盘合并提交**。

## M1-A — 进程内 Typed 通道

`Router`/`Node` 使用 `RaftCall`/`RaftReply`，热路径无 bincode。  
`GrpcRouter` 仍用 bincode。

## M1-B — 流水线 `propose_batch`

每条业务数据仍是 **独立 Raft entry**；`join_all` 并发 `client_write`。  
全部成功才 `Ok`；若出现 `NotLeader` 等错误则 `Err`（部分可能已提交，依赖幂等键）。  
Demo：`--bench-batch-size N`。

### 压测形态：`conc=4` × `batch=8`

高吞吐 mem 阶梯点（perf 文档 **C″**）。完整说明见 [perf.zh-CN.md — 并发 × 流水线](../perf.zh-CN.md#并发--流水线-conc4--batch8)。

| 层 | 机制 | 作用 |
|----|------|------|
| **batch=8** | 一次 `propose_batch` 同时起 8 个 `client_write` | 单个客户端内重叠 quorum RTT |
| **conc=4** | 四个 Tokio proposer 循环 | 多客户端把 leader 喂满 |
| **叠加** | 峰值约 `4×8` 在途写 | 看墙钟 TPS，不是单条 RTT |

`batch>1` 时 harness 延迟是**均摊**（`批墙钟 / N`），不可与顺序单条 p50 直接当同一指标对比。

## M3b — 深流水线（file + mem 墙钟 TPS）

**目标：** 在不打 mega-entry 的前提下，提高在途 `client_write` / 复制 — 峰值在途 ≈ `concurrency × batch_size`。

| 改动 | 位置 | 作用 |
|------|------|------|
| 每条 payload `try_join_all` | `propose_batch` | N 路 `client_write` 并发重叠 quorum 等待（无逐条 spawn 开销） |
| bench 去掉 `payloads.clone()` | demo `propose_batch_bench` | 热路径只 move 一批 |
| 进程内 `backoff: None` | `network.rs` | 避免 500ms 睡眠拖慢流水线 AppendEntries 重试 |

### Mem

单路 `batch=16` 已 **~177k**；`conc=4 × batch=8` **~344k**（本机）。

### File `sync=0` — 墙钟 TPS（本机已达）

| 配方 | ~TPS | 在途 |
|------|------|------|
| 顺序单条 | ~2.1–2.6k | 1 |
| `batch=16`, `coalesce=50µs` | ~5k | 16 |
| `conc=4`, `batch=8` | ~60–66k | ~32 |
| **`conc=4`, `batch=16`** | **~117k** | ~64 |
| **`conc=8`, `batch=16`** | **~187k** | ~128 |

破 **100k file sync=0** 靠加深在途，不能靠顺序单条；**顺序** `sync≥1` 仍 ~25 TPS（单条付整次 fdatasync×多数派）。  
**深流水线 + 复制批（M4）后**，3-node sync=1 可到 **~15–25万**（见下节与独立规格）——流式延迟刷盘（M3a）单独通常打不过深流水线的墙钟 TPS。

```bash
RUST_LOG=error ./target/release/multiraft-demo --mode bench --nodes 3 --groups 1 \
  --bench-ops 8000 --bench-file-log --data-dir /tmp/multiraft-bench \
  --bench-concurrency 4 --bench-batch-size 16
```

## M4 — sync=1 深流水线 / 组提交 / 复制批量

将客户端在途、本地 disk pipeline、复制 `max_payload_entries` **对齐叠满**，在保留 sync=1 耐久语义下抬墙钟 TPS。

完整理念、实现、拐点与配方：**[2026-07-22-sync1-disk-pipeline-merge.zh-CN.md](./2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)**。

摘要：

| 杠杆 | 作用 |
|------|------|
| `propose_batch` + `conc×batch` | 客户端在途 |
| defer + double-buffer flusher | 本地 \(E\)；append 不 await sync |
| `max_payload_entries`（默认 300→8k/16k） | 复制侧 \(E\)（主跃迁） |
| 实用拐点 | pe≈8192，流水线≈256×256 → ~15万；平台 pe≥16k → ~25万 |

## M2 — 文件日志组提交

`file_log_coalesce_us`：Os 档下与 stream 一样走**后台定时 flusher**合并 `log.bin` write（append 任务不再 `sleep`）；重叠 append（`n_callbacks>1`）立即 flush。truncate/purge 前强制 flush。

## M2b — 文件日志 sync level（本地持久化强度）

业界通常把「数据落在哪」和「每次写刷多深」分开。本地强度档位与 Aeron Archive/Cluster `file.sync.level` 同号：

| 档位 | 名称 | 机制 | 业界对照 |
|-----:|------|------|----------|
| **0** | OS / page cache | 只 `write`，由内核延后刷盘 | Aeron `0`；高吞吐默认常见 |
| **1** | Data sync | `fdatasync` / `File::sync_data` | Aeron `1`；PostgreSQL `fdatasync` |
| **2** | Full sync | 数据+元数据 `fsync` / `File::sync_all` | Aeron `2`；严格 WAL |

正交维度（不被 sync level 替代）：

- **内存 log**（空 `data_dir`）— 本地不落文件（与「文件 + level 0」不同：后者至少有 page cache 中的文件映像）。
- **复制多数派** — commit 等 peer，不等于本机 fsync。
- **组提交**（`file_log_coalesce_us`）— 先合并 write，再按 sync level 刷一次。

配置：`ClusterConfig::file_log_sync_level`（`Os`/`Data`/`All`），默认 **Os**。作用于 `log.bin` 追加/重写与 `hard_state.json`。Demo：`--bench-file-sync-level 0|1|2`。

## M3a — 文件日志流式落盘（Os 档位 0）

Aeron Archive 风格 **顺序流式写**（sync level 0）：

- 长生命周期 append 句柄 + 大 `BufWriter`（默认 64 KiB；可随 `stream_buf_bytes` 放大）。
- 定长帧在 `pending_buf` 合并，一次 `write_all` + `BufWriter::flush` 进 page cache，再触发 [`IOFlushed`]。
- 可选 Os 延迟：`ClusterConfig::file_log_stream_buf_bytes` 和/或 `file_log_stream_flush_ms`（后台 `Notify` + 定时）。`Data`/`All` 仍同步刷盘；truncate/purge/rewrite 前强制 flush。
- API：`FileLogStore::open_with_full_options(..., FileLogStreamOptions { .. })`；默认 `stream_* = 0` 与原先立即 flush 一致。

## M3c — 少调度（进程内 duty cycle）

仍在进程内；降低 typed Raft RPC 一跳上的 Tokio / 锁开销：

| 层 | 改动 | 作用 |
|----|------|------|
| **Router** | 先 `try_send`，满再 `send().await` | 通道有容量时不让出调度 |
| **Node demux** | 固定 worker 池（轮询），非每条 `spawn` | 有界并发处理；AppendEntries 仍并行 |
| **通道** | 更大 ingress + 每 worker 队列 | 流水线 AppendEntries，推迟背压 |
| **StandbyThrottle** | `standby_count == 0` 原子快路径 | 纯 voter 集群 send 零 mutex |

不在 `unregister_node` 之后缓存 `NodeTx` — 每次发送仍在 router mutex 下查表。

## 不做

Media Driver、跨进程共享内存、换共识引擎、SBE 编译器。

## 下一阶段（backlog）

M4（sync=1 叠满）已交付。其后：压缩 FSM `W`、分段延迟指标（N2b）、运维 Archive 语义 — 见 [2026-07-22-aeron-next-borrow.zh-CN.md](./2026-07-22-aeron-next-borrow.zh-CN.md)。

## 验证

- `bench_ceiling` / demo bench / file 深流水线；chaos 仍过；更新 `docs/perf*.md`。
- 热路径单测覆盖（关键 crate）：

```bash
cargo llvm-cov -p multiraft-core --lib --summary-only
cargo llvm-cov -p multiraft-store --lib --tests \
  --test file_log_sync_coverage --test file_log_roundtrip --test restart_recover \
  --ignore-filename-regex 'tests/|sm_bridge|snapshot|mem_log' --summary-only
```

目标：`config.rs` 与 `log_file.rs` 的 sync/stream 路径 **行覆盖约 100%**；不宣称整个 workspace 100%。
