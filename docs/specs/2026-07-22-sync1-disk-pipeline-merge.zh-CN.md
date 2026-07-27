# 深流水线与 sync=1 组提交 / 复制批量（M4）

**English：** [2026-07-22-sync1-disk-pipeline-merge.md](./2026-07-22-sync1-disk-pipeline-merge.md)

**日期：** 2026-07-22  
**状态：** 已实现并压测标定  
**前置：** [热路径 M1–M3](./2026-07-21-aeron-inspired-hotpath-design.zh-CN.md)  
**约束：** openraft `=0.10.0-alpha.30`；一条业务数据仍是一条 Raft entry（不 mega-entry）。

**概述：** [技术亮点 — 热路径与 sync=1](../spotlight/2026-07-hotpath-sync1.zh-CN.md)。本文含实现细节与完整拐点表。

---

## 1. 问题陈述

M3b 深流水线把 **mem / file sync=0** 墙钟 TPS 拉到十万级，但 **file sync=1** 曾长期停在：

| 形态 | ~TPS | 误判 |
|------|------|------|
| 顺序单条 | ~25 | 「sync=1 只能几十」 |
| 深流水线 + 默认复制批 | ~10–13k | 「多数派 fdatasync 钉死在一万」 |

真正瓶颈往往是 **多层在途深度不足** 或 **复制 RPC 批太小**，而不是「有 fdatasync 就不能流水线」。

本规格沉淀 2026-07-22 的实现与调优理念：**在保留 sync=1 耐久语义的前提下，把 client → storage → replication 三层都叠满。**

---

## 2. 调优理念

### 2.1 Little’s Law（墙钟吞吐）

\[
\text{墙钟 TPS} \approx \frac{\text{同时 in-flight 数}}{\text{单次有效延迟}}
\]

- **延迟高不一定吞吐低**：只要 in-flight 同比例加深。  
- **空等会掉吞吐**：拉长 coalesce/hold 窗口却喂不饱 \(E\)（每次 sync 的 entry 数）→ TPS 下降。  
- 压测看 **墙钟 TPS**；`batch>1` 时 p50 常是均摊，勿与顺序单条 RTT 直接对比。

### 2.1b 深流水线在时间轴上「叠什么」

顺序单条（sync=1）时间轴大致串行：

```text
propose₁ ──[append+fdatasync+repl]── Ok₁ ── propose₂ ── …     TPS ≈ 1 / L
```

深流水线把 **多笔独立 `client_write` 的等待重叠**：

```text
propose₁ ──[  L₁ (ms 级 quorum+disk)  ]── Ok₁
 propose₂ ──[  L₂  ]── Ok₂
  propose₃ ──[  L₃  ]── Ok₃
  … 同时在途 N 笔 …
墙钟 TPS ≈ N / L_avg，而不是 1 / L
```

与 **组提交 / 复制批**正交但必须同开：流水线提高 \(N\)；合并提高每次 durable 的 \(E\)，从而压低有效 \(L\)（摊销后的每条成本）。只加深 \(N\)、复制仍 pe=300，会卡在「万级」；只抬 pe、客户端仍顺序，仍是「几十 TPS」。

### 2.2 四层流水线（必须对齐）

```text
① 客户端     conc × batch     propose_batch = try_join_all(client_write)
② Raft 核心  api_batch_*      合并连续 ClientWrite → 更肥的 AppendEntries 存储命令
③ 本地落盘   FileLogStore     defer + 后台 flusher + double-buffer（sync 时放锁）
④ 复制路径   max_payload_entries   每条网络 AppendEntries 可带的 entry 上限
```

任一层偏浅，上一层再深也灌不进去：

| 层偏浅时的症状 | 典型数字（本机） |
|----------------|------------------|
| ① 只有顺序 propose | sync=1 ~25 TPS |
| ③ append 路径 await fdatasync | outstanding IO ≈ 1 |
| ④ `max_payload_entries=300`（默认） | 3-node sync=1 卡在 ~1万 |
| ④ 抬到 8k–16k + ① 256×256 | 3-node sync=1 **~15–25万** |

### 2.3 摊销，而不是消灭 fdatasync

sync=1 单次 `fdatasync` 本机约 **4–6ms**，无法调成 mem 的 µs 跳。可做的是：

\[
\text{每条摊销} \approx \frac{t_{\text{fdatasync}}}{E},\quad
E = \text{一次 durable 合并的 entry 数}
\]

- \(E=1\) → 每条付整次 sync（顺序 ~25 TPS）。  
- \(E\) 很大 → 摊销可到 µs/条量级，墙钟接近「贵一点的 mem」。  
- **复制批**决定 follower 侧 \(E\)；**本地 group-commit** 决定本机 \(E\)。

### 2.4 耐久语义不因流水线放松

| 规则 | 含义 |
|------|------|
| `append()` 可先返回 | 不阻塞 openraft 命令环 |
| **`IOFlushed` 仅在 write[+sync] 后回调** | commit / 客户端成功仍等 durable |
| sync=1 + 多数派 | 已 `Ok` 的 propose，按设计不应因单机掉电丢（见 §6） |
| sync=0 / mem | 确认后仍可能丢 — 吞吐用、不是同档耐久 |

### 2.5 调参顺序（推荐）

1. 固定耐久：`--bench-file-sync-level 1`  
2. 打开复制批：`--bench-max-payload-entries`（先 4096，再扫拐点）  
3. 加深客户端：`conc×batch`（目标峰值附近 `256×256`）  
4. 本地 coalesce：高 pe 时多为 `0`；短窗口仅作对照  
5. 用足够 `ops`（≥3–5 万）× 多轮取中位，避免短墙钟噪声  

### 2.6 常见反模式

| 反模式 | 后果 |
|--------|------|
| 只加长 coalesce/hold，不加深在途 / 不抬 pe | 空等 → TPS 掉、延迟升 |
| defer 却在 `append` 内 await sync | outstanding IO≈1，组提交失效 |
| pe 仍默认 300，却把 sync=1「钉死」在一万 | 误判为 fdatasync 物理上限 |
| `conc×batch` 过订阅（如 512×512） | 排队争用，反低于 `256×256` |
| 用 `propose_many` 当墙钟吞吐主路径 | 客户端在途变浅，通常不如 `propose_batch` |
| 把均摊 p50 当顺序 RTT | 指标误读 |

---

## 3. 深流水线：设计与实现

### 3.1 客户端层（①）

**API：** `MultiRaft::propose_batch`

- **一条 payload = 一条 Raft entry**（非 mega-entry）。  
- 实现：`futures::try_join_all` 并发 `client_write`，重叠 quorum 等待。  
- 峰值在途 ≈ `concurrency × batch_size`（demo：多个 sticky proposer × 每批 N）。  
- 另提供 `propose_many`（`client_write_many`）：单次 Core 消息更肥，但客户端在途变浅；**压测墙钟 TPS 优先 `propose_batch`**。

**为何 mem 3-node 也「没改共识」却上限极高：** 复制跳仍在，但每跳是 RAM+进程内 RPC（µs），Little’s Law 下深在途即可到数十万。sync=1 每跳多付 ms 级盘，必须靠更大 \(E\) 与更深在途摊销。

### 3.2 存储层（③）— 实现要点

#### 3.2.1 后台 flusher（不再在 append 上 sleep）

旧实现在 append 任务 `sleep(coalesce_us)` → 深流水线被 `1/coalesce` 卡死。  
现：`coalesce_us` / stream 只 **defer + 唤醒后台 flusher**；append 热路径不睡。

#### 3.2.2 defer 时禁止在 `append` 路径 await sync

openraft 对 `AppendEntries` 存储命令 **不 await `IOFlushed`，但会 await `append()` 返回**。  
若 overlap 时在 `append` 内 `await fdatasync`，命令环 outstanding≈1，组提交失效。

**正确：** `defer_enabled` 时入队 pending、`ensure_flusher`、`notify_one`，立即 `Ok`；由 flusher / `flush_pipeline` 完成 sync 并回调 `IOFlushed`。

#### 3.2.3 Double-buffer（disk pipeline）

`flush_pipeline`：

1. 持 state 锁 `take` pending buf + callbacks  
2. **释放 state 锁**  
3. 在 `io_gate` 下 `write` + `sync_*`  
4. 归还 writer、完成 callbacks；循环排空 sync 期间新入队的批次  

这样 **fdatasync 进行中仍可继续 `append` 入队**，提高 \(E\)。

#### 3.2.4 `hold_overlap`（定时组提交）

可选：重叠也不提前刷，只靠 coalesce 定时器 / 字节·回调上限。  
用途：测「纯窗口」；Raft 浅 IO 时易空等。生产 sync=1 高 pe 配方通常 **不必开**。  
注意：hold 模式 flusher **忽略每条 append 的 notify**（避免 clone/Drop 打穿窗口）；用 `notify_one` 存 permit，避免 flusher 未 park 时丢唤醒。

#### 3.2.5 关键类型 / 配置

| 符号 | 作用 |
|------|------|
| `FileLogStreamOptions::hold_overlap` | 定时持有 |
| `ClusterConfig::file_log_coalesce_us` | 组提交窗口（µs） |
| `ClusterConfig::file_log_sync_level` | 0/1/2 |
| `flush_pipeline` / `io_gate` | 放锁刷盘 |

代码：`crates/multiraft-store/src/log_file.rs`。

### 3.3 复制层（④）— N2a

openraft 默认 **`max_payload_entries = 300`**：每条网络 AppendEntries 最多 300 条日志 → follower 小批、多次 `fdatasync`。

**露出并调大** `ClusterConfig::max_payload_entries`（demo：`--bench-max-payload-entries`）后，follower 单次存储 append 变肥，复制侧 group-commit 生效——这是 sync=1 从 ~1万 跃到十万级的主杠杆。

同时可调：

| 旋钮 | 含义 | 默认（0=库默认） |
|------|------|------------------|
| `max_payload_entries` | 网络复制批 | 300 |
| `max_append_entries` | 存储合并批 | 4096 |
| `api_batch_linger_ms` / `api_batch_capacity` | ClientWrite 合并 | 0 / 4096 |

---

## 4. 实测拐点（本机，ops=48k×3 中位）

### 4.1 `max_payload_entries`（`256×256`，coal=0，sync=1，3-node）

| pe | 中位 TPS | 形态 |
|----|----------|------|
| 2048 | ~51k | 陡升 |
| 4096 | ~91k | 陡升 |
| **8192** | **~145k** | **性价比拐点** |
| 12288 | ~186k | 仍涨 |
| **16384–24576** | **~25万** | **平台**（再大只抖） |

### 4.2 流水线（pe=8192，coal=0）

| conc×batch | TPS | 说明 |
|------------|-----|------|
| 64×128 | ~67k | 欠供 |
| 128×256 | ~117k | |
| **256×256** | **~150k** | 最佳 |
| 256×512 / 512×* | ~112–124k | **过订阅** |

### 4.3 参照

| 场景 | TPS |
|------|-----|
| 3-node 平台 pe≥16k | ~25万 |
| 1-node 同档深流水线 | ~41万 |
| 比例 | 3-node ≈ 1-node **~60%**（多数派盘税） |
| 本地 fdatasync batch=256 | ~4.4万 entry/s（无 Raft） |

### 4.4 推荐工作点

```bash
# 性价比
--bench-file-sync-level 1 \
--bench-max-payload-entries 8192 \
--bench-concurrency 256 --bench-batch-size 256 \
--bench-file-coalesce-us 0 --bench-ops 48000

# 摸平台
# --bench-max-payload-entries 16384
```

---

## 5. 与 mem / sync=0 的对照（理念）

| | Mem 3-node | File sync=0 | File sync=1（调满） |
|--|------------|-------------|---------------------|
| 共识/复制 | 有 | 有 | 有 |
| 每跳成本 | µs | 写 page cache | **fdatasync ~ms** |
| 深流水线效果 | 极好（~30万+） | 好（~15–50万） | 好（~15–25万，靠大 \(E\)） |
| 已确认后掉电 | 丢 | 可能丢 | **多数派 durable 则不应丢** |

**结论：** 不是「改了共识 mem 才快」；是 **每跳成本 × 在途深度 × \(E\)**。sync=1 要叠满，必须同时深 ① 与肥 ④。

---

## 6. 丢消息风险（摘要）

| 场景 | 已向客户端 `Ok` 后掉电 |
|------|------------------------|
| sync=1/2 + 多数派 | 按设计 **不丢**（成功 ⇒ 多数派 `IOFlushed`） |
| sync=0 | **可能丢**（page cache） |
| mem | **丢** |
| `propose_batch` 部分失败 | 前缀可能已提交 → **幂等重试** |
| 尚未返回成功 | 未提交；重试，非「库吞已确认消息」 |

流水线 / group-commit / 高 pe **不改变**「`IOFlushed` 之后才算 durable」的契约。

---

## 7. 代码与文档索引

| 区域 | 路径 |
|------|------|
| FileLogStore defer / flush_pipeline / hold | `crates/multiraft-store/src/log_file.rs` |
| `propose_batch` / `propose_many` | `crates/multiraft-net/src/multiraft.rs` |
| 配置旋钮 | `crates/multiraft-core/src/config.rs` |
| Demo 旗标 | `crates/multiraft-demo`：`--bench-max-payload-entries` 等 |
| 数字与配方 | [perf.zh-CN.md](../perf.zh-CN.md) |
| 热路径 M1–M3 | [2026-07-21-…zh-CN.md](./2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) |
| N2a backlog 勾选 | [2026-07-22-aeron-next-borrow.zh-CN.md](./2026-07-22-aeron-next-borrow.zh-CN.md) |

## 8. 非目标

- mega-entry；改 openraft 版本  
- commit 与盘解耦（那是另一语义档）  
- 宣称对齐 Aeron kernel-bypass 延迟数字  

## 9. 验证

```bash
cargo test -p multiraft-store --test file_log_sync_coverage -- --test-threads=1
# 拐点配方见 §4.4；chaos 回归按仓库惯例
```
