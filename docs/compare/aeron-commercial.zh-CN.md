# multiraft 与 Aeron Cluster / Standby Premium 对比

**English：** [aeron-commercial.md](./aeron-commercial.md)

本文对比面向撮合高可用的开源 Rust 库 **multiraft** 与 Real Logic 商业产品 **Aeron Cluster**、**Aeron Cluster Standby (Premium)**。

---

## 定位

**multiraft 不是 Aeron 的分支或 fork。** 它不嵌入 Aeron Media Driver、Consensus Module 或 Archive，而是在钉死的 **[openraft](https://github.com/databendlabs/openraft) `=0.10.0-alpha.30`** + `openraft-multi` 之上，追求撮合 HA 的**语义对齐**：热备、快照卸载、升降级，以及 **Aeron 启发的热路径**。

| | multiraft | Aeron Cluster / Standby Premium |
|---|-----------|----------------------------------|
| 许可证 | Apache 2.0 | 商业（Real Logic Premium） |
| 语言 | Rust | Java / C++（Aeron 栈） |
| 共识 | openraft Raft | Aeron Consensus Module |
| 传输 | 类型化进程内 + 可选 gRPC | Media Driver + IPC / UDP |
| 主要目标 | 嵌入 Rust 栈的撮合 HA | 通用集群服务 + Premium DR |

我们借鉴 Aeron 的**思路**（standby 节流、快照 daisy、sync 档位、流水线 propose），但不交付其运行时。

---

## 我们对齐的能力

能力映射见 [Aeron Standby Premium 对齐](../specs/2026-07-20-aeron-standby-parity-design.zh-CN.md) 与 [Aeron 启发式热路径](../specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md)：

| 领域 | multiraft 对应实现 | 说明 |
|------|-------------------|------|
| **Standby 卸载** | openraft **Learner**（`add_standby`）；异步快照不阻塞 voter | `StandbyOffload` + `trigger_standby_snapshot` |
| **Standby 不反压 leader** | `standby_max_inflight`、`standby_replicate_delay_ms` 对 standby 节流 | 近似「standby 不得对主集群日志施加反压」 |
| **快照恢复** | `SnapshotAdvertisement`、`try_recover_from_standby_ads`、HTTP `fetch_url` 拉取 | 分块 Range + sha256 校验 |
| **Promote / demote** | `promote_standby`、`demote_to_standby`（`change_membership`） | 暖 DR / TransitionModule 类比（需运维触发） |
| **Daisy 快照链** | `daisy_upstream_base`、`sync_from_daisy_upstream` | **仅快照** daisy，非完整 log 重定向 |
| **Stale 读** | `read_stale`、`enable_stale_queries` | 显式水位；非线性一致 |
| **类型化进程内 RPC** | `RaftCall` / `RaftReply` 走 `mpsc`，同进程无 bincode | 跨进程 gRPC 仍用 bincode |
| **`propose_batch` 流水线** | 每条 payload 独立 Raft entry；`join_all` 并行发起 | 抬墙钟 TPS，非巨型 entry |
| **文件 sync 档位 0 / 1 / 2** | `FileLogSyncLevel::{Os, Data, All}` ↔ Aeron `file.sync.level` | 见下文耐久表 |
| **Stream 选项** | `FileLogStreamOptions`：`stream_buf_bytes`、`stream_flush_ms` | sync=0 下缓冲顺序 append |

Standby 建模为 openraft Learner，而非第二套共识。Archive 语义用**持久 SnapshotCatalog + HTTP/gRPC 拉取**近似，非完整 Aeron Archive。

---

## 我们刻意不做的部分

以下为**设计边界**，不是 multiraft 内部计划「追平」的遗漏项：

| Aeron 组件 | multiraft 立场 |
|-----------|----------------|
| **Media Driver** | 无跨进程共享内存 IPC；进程内类型化 + 可选 gRPC |
| **SBE 编译器** | bincode / Rust 结构体；无 FIX/SBE 代码生成流水线 |
| **完整 Aeron Archive** | 目录 catalog + HTTP Range；无录制/回放语义 |
| **商业 Cluster 运行时** | 无 ClusteredService 容器、会话模型或 PremiumClusterTool |
| **替换共识** | 固定 openraft；不移植 Aeron Consensus Module |

若需要跨 JVM/C++ 的 Media Driver 延迟、线上 SBE、或 Real Logic 支持合同，应选 **Aeron 商业版**。multiraft 面向已在 Rust/openraft 上、希望撮合 HA 语义而不引入第二套栈的团队。

---

## 性能（实测与上限）

以下数字均来自 [perf.zh-CN.md](../perf.zh-CN.md)：**本机** SSD、**3 voter**、**进程内** bench（默认 harness）、release 构建。它们**不是**与 Aeron 的同机对标——**不声称比 Aeron Cluster 更快**。

### 内存日志（无磁盘）

| 场景 | ~TPS | p50 | 含义 |
|------|------|-----|------|
| 顺序单条（C） | ~17–18k | ~53–55µs | Quorum RTT 软天花板 |
| 流水线 `batch=8`（C′） | ~110k | ~8µs† | 单 proposer，重叠 quorum 等待 |
| **conc=4 × batch=8（C″）** | **~300k+** | ~10µs† | 并行 proposer × 流水线 |

† batch 延迟为**均摊**（`batch_wall / N`），不可与单条 RTT 直接对比。

### 文件日志（每副本本地盘）

| 场景 | ~TPS | p50 | 含义 |
|------|------|-----|------|
| 顺序，sync=0 | ~2.1–2.6k | ~360–425µs | 常开 `log.bin` + page cache 写 |
| **sync=0，conc×batch 深流水线** | **~117k–187k** | ~30–37µs† | `conc=4×batch=16` ~117k；`conc=8×batch=16` ~187k |
| sync=1 顺序 | ~25 | ~38ms | 每 append data fsync × 多数派 |
| **sync=1 深流水线 + pe≫300** | **~150k–250k** | 均摊 | pe≈8k/`256×256` ~15万；pe≥16k 平台 ~25万；见 [M4](../specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md) |
| sync=2 顺序 | ~23 | ~42ms | 每 append 全量 fsync × 多数派 |

### 硬上限（当前设计内无法靠小改消除）

1. **顺序单条 mem** 仍约 ~18–25k：每笔一次 quorum RTT。
2. **顺序 file sync=0** 仍约 ~2k：每笔磁盘写 × 多数派。
3. **`sync_level ≥ 1` 顺序单条** 在本机约 **~25 TPS**：每 append fsync × 多数派。深流水线 + 大 `max_payload_entries` 可把 sync=1 墙钟拉到 **~15–25万**（摊销 \(E\)，不消除 fsync）。
4. openraft 单条 `client_write` 须等 commit+apply；leader 为 AppendEntries **clone** entry。

**墙钟过 100k TPS 的方式：** `propose_batch` + 并发——重叠多笔独立 Raft 写，而非一条巨型 entry。file sync=0 / sync=1 过 100k+ 都要深流水线；sync=1 还需抬高复制批（见 [M4](../specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)）。

跨进程 gRPC 需单独压测；默认 bench 为进程内。

---

## 耐久档位（`file_log_sync_level`）

与 Aeron Archive/Cluster **`file.sync.level`** 对齐（见 [热路径 M2b](../specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md#m2b--文件日志-sync-level本地持久化强度)）：

| 档位 | 名称 | 机制 | multiraft 配置 | 典型 TPS 影响（3-node file，本机） |
|------|------|------|----------------|-------------------------------------|
| **0** | OS / page cache | 仅 `write`；内核可延迟刷盘 | `FileLogSyncLevel::Os`（默认） | 顺序 ~2k；深流水线 sync=0 墙钟 ~117k–187k |
| **1** | Data sync | `fdatasync` / `File::sync_data` | `FileLogSyncLevel::Data` | 顺序 ~25 TPS；深流水线 + pe≫300 墙钟 ~15–25万 |
| **2** | Full sync | `fsync` 数据+元数据 / `File::sync_all` | `FileLogSyncLevel::All` | 顺序 ~23 TPS |

正交维度（与业界惯例一致）：

- **内存日志**（`data_dir` 空）——本地无盘；强于「档位 0 文件」。
- **复制多数派**——commit 等 peer；不意味着本地 fsync。
- **组提交**（`file_log_coalesce_us`）——合并写后再应用 sync 档位。

Demo：`--bench-file-sync-level 0|1|2`。

---

## 何时选 multiraft vs Aeron 商业版

### 选 multiraft 当

- 撮合 / 交易栈是 **Rust**，且已确定 **openraft** Multi-Raft。
- 需要 **Standby Premium 类 HA**（learner standby、快照卸载、promote、快照 daisy、stale 读），**不要** JVM/C++ Aeron 依赖。
- 可接受 **HTTP 快照拉取** 替代完整 Archive，以及**运维触发**的 promote/demote。
- 需要 **Apache 2.0** 源码、chaos/Jepsen 钩子、可嵌入的薄库——而非 clustered-service 容器。
- 吞吐目标符合**流水线 propose**（mem 墙钟 100k+；file sync=0 深流水线 100k+），或可接受 **~2k 顺序 file** / **fsync 下 ~25 TPS**。

### 选 Aeron Cluster / Standby Premium 当

- 需要 **Media Driver** IPC、UDP 组播，或线上多语言 Aeron 客户端。
- 需要 **SBE** 编译 schema、完整 **Archive** 录制/回放，或 Real Logic **商业支持**。
- 运行 **ClusteredService** 会话语义、PremiumClusterTool，或已有 Aeron 运维手册。
- 要 Real Logic **Consensus Module** 与产品路线图，而非 openraft。

### 关于速度的中立表述

multiraft 优化的是 **openraft 热路径**（类型化进程内 RPC、流水线 batch、合并文件 append）。Aeron 优化的是**另一套栈**（Media Driver、零拷贝 IPC）。**本文不声称 multiraft 比 Aeron 更快**——只有在相同 workload、硬件与耐久档位下才可比较。

---

## 延伸阅读

| 文档 | 主题 |
|------|------|
| [perf.md](../perf.md) · [perf.zh-CN.md](../perf.zh-CN.md) | 实测上限、bench 命令、C″ 流水线 |
| [Aeron Standby 对齐](../specs/2026-07-20-aeron-standby-parity-design.zh-CN.md) | Premium 能力矩阵 P0–P3 |
| [Aeron 启发式热路径](../specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) | M1 类型 RPC、M2 文件合并/sync、M3 stream |
| [Aeron 下一阶段借鉴 backlog](../specs/2026-07-22-aeron-next-borrow.zh-CN.md) | N1 FSM `W`、N2 复制指标、N3 Archive 运维 |
| [Standby 异步快照](../specs/2026-07-20-standby-async-snapshot-design.zh-CN.md) | MVP 快照机制 |
| [ARCHITECTURE.zh-CN.md](../ARCHITECTURE.zh-CN.md) | crate 边界与契约 |

---

## 小结

multiraft 提供**开源、Rust 原生撮合 HA**：与 Aeron Standby Premium **语义对齐**（standby、快照、promote、daisy、stale 读），并采用 Aeron Cluster 的**机械共情模式**（类型化热路径、流水线 propose、分级耐久）——**基于 openraft**，无 Media Driver 与商业 Cluster。嵌入 Rust Multi-Raft 选 multiraft；要完整 Real Logic 平台选 Aeron 商业版。
