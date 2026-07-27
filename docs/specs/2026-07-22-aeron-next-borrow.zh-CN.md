# Aeron 下一阶段借鉴 backlog（M3 / Standby P3 之后）

**English：** [2026-07-22-aeron-next-borrow.md](./2026-07-22-aeron-next-borrow.md)

**日期：** 2026-07-22  
**状态：** Backlog（未排期）  
**约束：** openraft `=0.10.0-alpha.30`；**借思想，不借 Aeron 运行时** — 不做 Media Driver、SBE 编译器、完整 Archive 引擎、Consensus Module 替换。

**已交付（勿再当缺口）：**

- Standby Premium 对等 P0–P3 — [aeron-standby-parity](./2026-07-20-aeron-standby-parity-design.zh-CN.md)
- 热路径 M1–M3 — [aeron-inspired-hotpath](./2026-07-21-aeron-inspired-hotpath-design.zh-CN.md)
- 与商业 Aeron 定位 — [compare/aeron-commercial](../compare/aeron-commercial.zh-CN.md)

---

## 论点

下一阶段收益 **不是**「再抄一个 Aeron 组件」，而是三条仍落在薄 Multi-Raft 库内的工作流：

```text
1. 压缩 FSM / 撮合侧工作量 W     → 真实 TPS ≈ 1 / (W + 共识开销)
2. 复制路径 batch + 分段延迟指标 → 补 propose_batch 之外的半边
3. 运维向 Archive 语义           → 位点、导出、恢复剧本
```

Aeron Cluster 文档同一套上限：顺序 RSM 下，传输足够便宜后吞吐由 **业务逻辑时间 `W`** 主导。multiraft 已优化 openraft 外壳；未压的 `W` 与不透明阶段仍遮住真实天花板。

---

## N1 — 压缩 FSM / 撮合 `W`（P1）

### Aeron 思路

集群服务逻辑保持 **确定性、少分配、apply 路径无阻塞 IO**。命令编码便宜（SBE 级定长布局）。校验前置到边缘，避免 apply 中途回滚。

### multiraft 映射

| 项 | 范围 | 说明 |
|----|------|------|
| **N1a** 下游 FSM 指南 | 文档 + 示例 | apply 应在 µs 级结束；`apply` 内无盘/网；幂等键 |
| **N1b** Demo / CounterFsm 预算 | 可选微基准 | `bench_ceiling` 已有 FSM-only（~9M）；写清撮合 FSM 目标带 |
| **N1c** 紧凑命令编码 | 可选 helper | 热路径定长或 rkyv/flat — **不是** SBE 编译器 |

### 成功标准

- 书面约定：目标墙钟 TPS 下推荐的最大 `W`（例：`W ≤ 5µs` ⇒ 逻辑软顶约 20 万，尚未计 quorum）。
- 至少一个撮合形态 FSM 示例，在 demo 或微基准中报告 apply p50/p99。
- 不改动 openraft 锁版本。

### 非目标

替换撮合引擎；在 apply 内嵌 DB；用 LeaseRead 代替快速 apply。

---

## N2 — 复制路径 batch + 分段延迟（P1）

### Aeron 思路

**复制**路径上的自然 batch 与流水线 ack；各阶段指标让运维看清时间在网络、磁盘还是 apply。

### multiraft 映射

propose 侧流水线（`propose_batch`、`conc×batch`）已大体完成。仍薄的部分：

| 项 | 范围 | 说明 |
|----|------|------|
| **N2a** AppendEntries batch / in-flight 旋钮 | `multiraft-net` + openraft 配置面 | **已做（2026-07-22）：** 露出 `max_payload_entries`；拐点 pe≈8192 + `256×256` → ~15万；平台 pe≥16k → ~25万（3-node sync=1）。见 [M4](./2026-07-22-sync1-disk-pipeline-merge.zh-CN.md) |
| **N2b** 分段延迟直方图 | Metrics API + demo | 阶段：`入队 → leader append → quorum RTT → commit → apply`（尽力打点） |
| **N2c** Bench 报告增强 | `multiraft-demo --mode bench` | 可选 JSON 分段 p50/p99；头条仍是墙钟 TPS |

### 成功标准

- 有一份配方：主要靠复制侧调参（而不只抬 `conc×batch`）改善墙钟 TPS 或 p99。
- Bench 或 admin 至少暴露三个阶段延迟。
- Chaos / 线性一致测试仍绿。

### 非目标

自研共识；Media Driver；宣称对齐 Aeron Premium kernel-bypass 数字。

---

## N3 — 运维向 Archive 语义（P2）

### Aeron 思路

Archive：按位点持久录制、回放与运维工具 — 不只是「最新快照文件」。

### multiraft 映射（目录 + HTTP 近似）

| 项 | 范围 | 说明 |
|----|------|------|
| **N3a** 位点模型 | 规格 + API 草图 | `(group, term, index)` / snapshot id 作为一等运维句柄 |
| **N3b** 导出工具 | Admin 或 CLI | 按位点导出快照（+ 可选 log 切片元数据） |
| **N3c** 恢复剧本 | 文档 | 冷启 / 换 voter / standby 升格：curl + 期望 `RecoverOutcome` |
| **N3d** 加固 | 可选 | Range 续传已有部分；catalog GC / 保留策略 |

建立在现有 SnapshotCatalog + HTTP Range + sha256 上 — 加深 **运维语义**，不造 recording 引擎。

### 成功标准

- 双语运维文档：「恢复到位点 X」可复制粘贴命令。
- 导出 + 安装路径有自动化测试（可为 demo/admin 集成测）。
- 明确声明：不是 Aeron Archive 录制/回放。

### 非目标

完整 recording 流、全历史 time-travel、PremiumClusterTool 克隆、鉴权网关（实验室 Admin 仍默认无鉴权，除非单独立项）。

---

## 更后 / 可选（P3 — 仅产品需要时）

| 项 | 可借鉴 | 远离 |
|----|--------|------|
| 跨进程低延迟传输 | 撮合与 raft 进程间 SHM / io_uring framing | 嵌入 Media Driver |
| Duty-cycle / 亲和实验 | 少 yield、热 worker 绑核 | 宣称 Aeron IPC 延迟 |
| 客户端会话精简版 | 幂等 propose + 关联回执（RMQ 路径 B） | 完整 ClusteredService 会话模型 |

---

## 推荐优先级

```text
N1a 文档/契约     ─┐
N2b 分段指标      ─┼─► 快赢，分清 W vs quorum vs 磁盘
N2a 复制 batch    ─┘
N1b/c FSM 示例
N3a–c Archive 运维语义
仅当出现跨进程 SLO 时再做 P3 传输实验
```

---

## 仍不做（不变）

Media Driver · SBE 编译器 · 完整 Aeron Archive · Consensus Module 移植 · 无人值守跨 DC 切换 · 「比 Aeron 更快」宣传口径。

---

## 参考

| 文档 | 作用 |
|------|------|
| [Aeron Cluster performance limits](https://aeron.io/docs/aeron-cluster/performance-limits/) | `W` 决定的顺序 RSM 上限 |
| [Efficient business logic](https://aeron.io/docs/aeron-cluster/efficient-business-logic/) | 编码 / 校验 / apply 无阻塞 |
| [热路径设计](./2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) | M1–M3 已完成 |
| [Standby 对等](./2026-07-20-aeron-standby-parity-design.zh-CN.md) | HA 语义已完成 |
| [perf.zh-CN.md](../perf.zh-CN.md) | 墙钟上限与多 Group 说明 |
