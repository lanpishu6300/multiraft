# multiraft：热路径与 file sync=1 吞吐

**English：** [2026-07-hotpath-sync1.md](./2026-07-hotpath-sync1.md)  
**分支：** `feature/aeron-inspired-hotpath` · openraft / openraft-multi `=0.10.0-alpha.30`  
**日期：** 2026-07

面向撮合高可用的薄 Multi-Raft 运行时：每交易对一个 Raft Group，节点间连接按节点数复用，FSM 可插拔。实现基于 openraft（Apache 2.0），借鉴 Aeron 的机械同情思路（sync 档位、流水线、组提交），不引入 Media Driver，也不替换共识引擎。本文说明近期热路径与 **file sync=1** 工作的定位、实测与方案要点。

---

## 定位

| 需求 | 做法 |
|------|------|
| 按 symbol 隔离故障域 | 一 Group 一交易对；无跨 Group 事务 |
| 嵌入式 Rust、依赖可审计 | openraft / openraft-multi 精确锁版本 |
| 本地耐久可分级 | sync **0 / 1 / 2**（编号对齐 Aeron `file.sync.level`） |
| 高吞吐仍走共识 | 重叠多路独立 `client_write`，每条业务仍是一条 Raft entry |
| 与 Aeron Cluster 商业版 | 语义与热路径借鉴；无完整 Archive / ClusteredService — [对照](../compare/aeron-commercial.zh-CN.md) |

一期范围：库、多进程 Demo、acceptance / chaos / Jepsen。撮合引擎与消息队列接入留在下游应用。

---

## 实测（本机，3 voter，进程内）

绝对值随硬件变化；下表用于说明数量级与可复现配方。

| 场景 | 约墙钟 TPS | 说明 |
|------|------------|------|
| Mem，深流水线 | ~30 万+ | 共识路径仍在；跳延迟约 µs |
| File sync=0，深流水线 | ~12–19 万 | page cache 写 × 多数派 |
| File sync=1，顺序单条 | ~25 | 每条 fdatasync × 多数派 |
| File sync=1，深流水线 + 复制批 | ~15–25 万 | 仍为 sync=1；提高每次 durable 的 entry 数 |
| 工作点 | pe≈8192，`conc×batch=256×256` → ~14.5 万 | pe≥16k 时平台约 ~25 万 |

完整数据与命令：[perf.zh-CN.md](../perf.zh-CN.md)、[M4 规格](../specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)。

---

## 技术方案

### 1. 模块边界

```text
multiraft-demo  →  Demo / Admin HTTP / bench
multiraft-net   →  MultiRaft、共享 Router（进程内 Typed RPC / gRPC）
multiraft-core / fsm / store  →  类型与配置、状态机、每 Group 持久化
```

- Peer 连接复杂度为 O(节点)。  
- `propose` 返回 Ok 表示多数派已 commit 且 apply。  
- 超时或失败时结果不确定，调用方须用同一幂等键重试。  

详见 [ARCHITECTURE.zh-CN.md](../ARCHITECTURE.zh-CN.md)。

### 2. 热路径（M1–M3）

| 里程碑 | 内容 | 作用 |
|--------|------|------|
| M1 | 进程内 Typed RPC；`propose_batch` | 去掉同进程编解码；重叠 quorum 等待 |
| M2 / M2b | 文件组提交；sync 0/1/2 | 合并写入后再按档位刷盘 |
| M3a | Os 档流式缓冲 | sync=0 顺序追加 |
| M3b | `concurrency × batch` | mem / sync=0 墙钟进入十万级 |
| M3c | `try_send`、粘性 worker、standby 快路径 | 降低调度与锁开销 |

不做：Media Driver、SBE、完整 Archive、Consensus Module 替换。  
规格：[热路径设计](../specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md)。

### 3. sync=1（M4）

顺序 sync=1 约 25 TPS 来自「每条付整次 fdatasync × 多数派」。仅加深客户端流水线、复制批仍为默认 `max_payload_entries=300` 时，墙钟大约停在一万量级。将复制批与本地 disk pipeline 一并抬高后，在保留 sync=1 语义下可到十余万至约二十五万 TPS。

四层需同时足够深：

```text
① 客户端     conc × batch      propose_batch（try_join_all(client_write)）
② Raft 核心  api_batch_*       合并连续 ClientWrite
③ 本地落盘   defer + double-buffer flusher（刷盘时释放 state 锁）
④ 复制路径   max_payload_entries ≫ 300
```

耐久约定：

- defer 开启时，`append()` 可先返回，避免堵住 openraft 命令环。  
- `IOFlushed` 仅在 write（及 sync=1/2 的 sync）之后回调；客户端成功仍等待该回调。  
- sync=1 且多数派确认后，按设计单机掉电不应丢失已成功返回的写入。  

墙钟吞吐近似 \(N/L\)（在途深度 / 有效延迟）；每条摊销成本近似 \(t_{\mathrm{sync}}/E\)（单次 sync 时间 / 合并 entry 数）。流水线提高 \(N\)，组提交与复制批提高 \(E\)。

复现示例：

```bash
cargo build -p multiraft-demo --release
RUST_LOG=error ./target/release/multiraft-demo --mode bench --nodes 3 --groups 1 \
  --bench-file-log --bench-file-sync-level 1 \
  --bench-ops 48000 --bench-concurrency 256 --bench-batch-size 256 \
  --bench-file-coalesce-us 0 \
  --bench-max-payload-entries 8192
```

---

## 与 Aeron Cluster 商业版

- **multiraft：** 嵌入式 Rust / openraft、撮合 HA、分级本地耐久、可复现热路径数字。  
- **Aeron Cluster 商业版：** Media Driver、完整 Archive、ClusteredService 与厂商支持。  

对照说明：[compare/aeron-commercial.zh-CN.md](../compare/aeron-commercial.zh-CN.md)。

---

## 正确性相关

| 项 | 说明 |
|----|------|
| Consistency Contract | propose / linearizable / stale 语义分档 |
| porcupine | 线性一致历史检查（便于 CI） |
| acceptance / chaos | 端到端与切主等脚本场景 |
| Jepsen | 多进程故障注入（可选） |

[jepsen.zh-CN.md](../jepsen.zh-CN.md) · [chaos-checklist.zh-CN.md](../chaos-checklist.zh-CN.md)

---

## 相关文档

| 主题 | 链接 |
|------|------|
| 热路径里程碑 | [hotpath 规格](../specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) |
| sync=1 实现与拐点 | [M4 规格](../specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md) |
| 压测数据 | [perf.zh-CN.md](../perf.zh-CN.md) |
| Demo 上手 | [Wiki 快速开始](../wiki/zh/Getting-Started.md) |
| 文档索引 | [docs/README.zh-CN.md](../README.zh-CN.md) |

## 非目标

- 不对齐 Aeron kernel-bypass 延迟数字。  
- 不把「commit 与本地盘解耦」表述为 sync=1。  
- 不用 mega-entry 换吞吐；不擅自提升 openraft 大版本。  
