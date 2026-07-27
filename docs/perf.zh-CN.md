# 性能说明

**English：** [perf.md](./perf.md)

## 理论上限（本机 release 实测拆分）

| 阶段 | TPS | p50 | 含义 |
|------|-----|-----|------|
| A FSM-only | ~9M | &lt;1µs | 无共识软上限（无关 Raft） |
| B 1-node mem | ~55–57k | ~12–14µs | **无复制 Raft 软上限** |
| C 3-node mem（顺序单条） | ~17–18k | ~53–55µs | 类型化 in-process + 单次 `client_write` |
| C′ 3-node mem（batch=8） | ~110k | ~8µs† | 流水线多 entry `propose_batch` |
| C″ 3-node mem（conc=4, batch=8） | ~300k+ | ~10µs† | 并行 proposer × 流水线 |
| D 编解码 | ~1.4M | &lt;1µs | bincode 已不再是主因 |
| File 3-node（顺序, sync=0） | ~2.1–2.6k | ~360–425µs | 常开 `log.bin` + page cache 写 |
| File（顺序, sync=1 `sync_data`） | ~25 | ~38ms | 每 append data fsync × 多数派 |
| File（顺序, sync=2 `sync_all`） | ~23 | ~42ms | 每 append 全量 fsync × 多数派 |
| File（batch=16, coalesce=50µs, sync=0） | ~4–5k | ~180–195µs† | 组提交窗口 + 流水线 |
| File（conc=4, batch=8, sync=0） | ~60–66k | ~53–63µs† | 深流水线：4×8 在途 |
| File（conc=4×batch=16, sync=0） | **~117k** | ~30µs† | **越过 100k** |
| File（conc=8×batch=16, sync=0） | **~187k** | ~37µs† | 更高在途（~128） |
| File（conc=4, sync=0, batch=1） | ~10k | ~360µs | 仅并行 proposer |

† 批墙钟时间按 entry 均摊（非单条 RTT）。详见下文 [并发 × 流水线](#并发--流水线-conc4--batch8)。

**墙钟（wall clock）**：从压测开始到结束的**真实经过时间**（墙上挂钟），不是 CPU 时间。  
`TPS = 成功 entry 数 / 墙钟秒`。流水线/多 Group 时单条 p50 可很低，但**总吞吐仍以墙钟 TPS 为准**。

**结论：**

- 顺序单条 3-node mem 仍接近软天花板（~18–25k）：quorum + 两跳。
- **墙钟过 100k** 靠 `propose_batch` 流水线 / 并发，而不是换掉 openraft。
- File 顺序收益来自常开文件句柄；组提交仅在 append 重叠（batch/conc）时有效。
- **File sync=0 墙钟 ≥100k 已达成：** `--bench-concurrency 8 --bench-batch-size 16`（~187k）；`4×16` 约 117k。

## 理论上限（当前设计）

在 **不换 openraft、不做 Media Driver/SHM** 的前提下，本机 SSD、in-process、3 voter 的硬界大致是：

```text
TPS_seq ≈ 1 / (t_leader_local + t_quorum_rtt)
         ≈ 1 / (t_1node + 2 × t_inproc_hop + t_follower_append)
```

| 量 | 约值（本机） | 含义 |
|----|-------------|------|
| `t_1node` | ~18µs（~55k TPS） | 无复制：本地 append + apply |
| `t_inproc_hop` | ~15–20µs | Router→Node demux→`append_entries`→oneshot |
| `t_quorum_rtt` | ~2 hops（多数派） | 至少等一个 follower 完成 |
| **顺序 3-node 理想** | **~18–25k** | 接近实测 C（~17k） |
| **流水线墙钟** | **≪ 1/`t_seq`** | 多笔 `client_write` 重叠 quorum 等待 |
| **File sync=0** | 再 × 磁盘 write（每副本） | 实测 ~2k；sync=1/2 再 × fsync（~25 TPS） |

**扫除后仍在设计外的上限（不能靠小改代码抬高）：**

1. openraft 单条 `client_write` 必须等 commit+apply。
2. Leader 为 AppendEntries **clone** log entries（`RaftLogReader` API）。
3. File `sync_level≥1` 时本机 SSD 仍被 fsync×多数派卡住。

**本轮已扫掉的代码瓶颈（R6）：** Node 串行 demux→并发 spawn；StandbyThrottle 空集快路径；去掉 router 热路径 debug；`hard_state` commit 防抖；coalesce `Notify` 唤醒；encode 直写入 `pending_buf`。

## 并发 × 流水线（`conc=4` × `batch=8`）

这是表中 **C″** 的高吞吐 mem 场景。设计意图（另见 [热路径设计](./specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) M1-B）：用**多笔独立 Raft 写重叠**抬高墙钟 TPS，而不是打成一条巨型 entry。

### 两个旋钮

| 参数 | 作用 |
|------|------|
| `--bench-concurrency 4` | 起 **4** 个 Tokio 任务，各自作为独立 proposer 循环打当前 leader。 |
| `--bench-batch-size 8` | 每个循环一轮调用一次 `MultiRaft::propose_batch`，塞进 **8** 条 payload。 |

默认是 `concurrency=1`、`batch_size=1`（纯顺序单条）。两者叠加会放大在途写。

### 什么叫「不是 batch 成一条」

`propose_batch` **不会**把 8 条业务数据打进同一条 Raft log。每条 payload 仍对应一次 openraft `client_write`、**各自一条 entry**。共识、apply、FSM 幂等语义与「8 个并发客户端」一致。

### 一批内部的流水线

```text
propose_batch(group, [p0..p7])
        │
        ├─► client_write(p0) ──► 多数派 / apply ──► ProposeOk
        ├─► client_write(p1) ──► 多数派 / apply ──► ProposeOk
        ├─► ...
        └─► client_write(p7) ──► 多数派 / apply ──► ProposeOk
              ▲
              └── 经 futures::join_all 一并启动（流水线，非串行 await）
```

entry *i* 在等 AppendEntries / commit 时，entry *i+1* 可以已在 leader 上飞行。顺序 `propose` 要一笔一笔付的 quorum RTT，在这里被重叠掉。

### 再叠四路 proposer

```text
时间 ──────────────────────────────────────────────────────────►

proposer-0:  [一批 8 条 ════╗][一批 8 条 ════╗]...
proposer-1:  [一批 8 条 ════╣][一批 8 条 ════╣]...
proposer-2:  [一批 8 条 ════╣][一批 8 条 ════╣]...
proposer-3:  [一批 8 条 ════╝][一批 8 条 ════╝]...
                      ▲
                      └── 峰值约 4×8 = 32 个在途 client_write
                          （同一 group 的 leader，mem / 进程内）
```

- **流水线**（`batch`）：减少**单个**客户端内部的空等。
- **并发**（`conc`）：多个独立客户端，一批里最后几笔还在 commit 时，别的路仍在喂 leader。

峰值在途大约是 `concurrency × batch_size`（此处 32），不是两者相加。

### 延迟怎么报（易误解点）

`batch_size > 1` 时，harness 记的是**整次 `propose_batch` 的墙钟**，再按 `wall_us / N` 均摊到 N 条，用于 p50/p95/p99。

- 这是**均摊每 entry 份额**，不是单独一次 `propose` 的 RTT。
- 因此顺序单条 p50（~54µs）与 batch 均摊 p50（~10µs）**不能**当成「同一指标变快了」。
- TPS 用 `成功 entry 数 / 墙钟秒`，才是可横向对比的吞吐。

### 生产 API 语义

```rust
// 全部成功才 Ok；任一 NotLeader / 硬错误 → Err；
// 部分 entry 可能已提交 —— 重试必须带同一幂等键。
MultiRaft::propose_batch(group, payloads) -> Result<Vec<ProposeOk>, MultiRaftError>
```

### 如何跑 C″

```bash
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 8000 \
  --bench-concurrency 4 --bench-batch-size 8
```

同一阶梯上的对照：仅 `batch=8`（单 proposer + 流水线）≈ C′；仅 `conc=4` 且 `batch=1`（四路单条、无流水线）是更早的并发形态（R5 前后约 65–68k）。

## 压测入口

```bash
cargo build -p multiraft-demo --release

RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 5000

# 单路流水线
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 5000 --bench-batch-size 8

# 四路并行 × 每路 8 条流水线（C″）
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 8000 \
  --bench-concurrency 4 --bench-batch-size 8

RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 2000 \
  --bench-file-log --data-dir /tmp/multiraft-bench

RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 2000 \
  --bench-file-log --data-dir /tmp/multiraft-bench \
  --bench-batch-size 16 --bench-file-coalesce-us 50

# 本地持久化档位（对齐 Aeron 0/1/2）
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 1500 \
  --bench-file-log --data-dir /tmp/multiraft-bench \
  --bench-file-sync-level 1

# File sync=0 墙钟 ≥100k（深流水线）
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 8000 \
  --bench-file-log --data-dir /tmp/multiraft-bench \
  --bench-concurrency 8 --bench-batch-size 16

cargo test -p multiraft-net --test bench_ceiling --release -- --nocapture
cargo test -p multiraft-store --test bench_file_log_micro --release -- --nocapture
cargo test -p multiraft-net --test bench_codec_micro --release -- --nocapture

# sync-level / FileLogStore 覆盖率
cargo test -p multiraft-store --test file_log_sync_coverage --release
cargo llvm-cov -p multiraft-core --lib -p multiraft-store \
  --test file_log_sync_coverage --test file_log_roundtrip --summary-only
```

## 已处理瓶颈

| 轮次 | 热点 | 改动 | 效果 |
|------|------|------|------|
| R1 | 全量重写 `log.json` | `log.ndjson` 追加 | 微基准 4×+ |
| R2 | wipe/restart log reversion | `allow_log_reversion` | chaos 稳定 |
| R3 | 热路径日志噪音 | trace / 只打字节数 | 降开销 |
| R4 | NDJSON JSON 行写 + JSON RPC | `log.bin` + bincode | file **897→2130 TPS**；mem **14k→16.5k** |
| R5 | in-process 仍编解码；单条 propose；open/close 日志 | 类型化 `RaftCall`；`propose_batch`；常开文件 + coalesce | mem batch **~110k**；file 顺序 **~2.6k**；file conc=4 **~10k** |
| R6 | Node 串行 demux；voter 热路径 throttle 锁；每 commit 写 hard_state；coalesce 空等 | 并发 demux；standby 计数快路径；commit 防抖；Notify 唤醒 | 顺序 mem 仍 ~17k（quorum 界）；batch/conc 维持 **~100k / ~300k+** |
| R7 | bench payload clone；进程内 500ms backoff | bench 只 move；`backoff: None`；`try_join_all` | file conc×batch **~60–66k**（向 100k 目标） |

## 当前瓶颈

1. **顺序单条 mem**：仍受 per-op quorum RTT 限制（~17–18k）；吞吐请用 `propose_batch` / 并发。
2. **File 顺序**：仍是磁盘 × 多数派；coalesce 只在 append 重叠时有用。本地持久化强度由 `file_log_sync_level` 分级（`0` page cache / `1` sync_data / `2` sync_all）；默认 `0` 与改前行为一致——调高用吞吐换掉电安全（见 [热路径 M2b](./specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md#m2b--文件日志-sync-level本地持久化强度)）。
3. **跨进程 gRPC**：需单独压测（本仓库 bench 默认 in-process）。
4. **架构保留**：log entry clone、openraft 调度模型——需换 API/引擎才能再抬顺序单条天花板。

## 多 Group 墙钟上限（本机实测）

Harness：进程内 3 voter；`concurrency>1` 时每个 proposer **粘性绑定** `group_id = worker % groups`（避免 `batch%groups==0` 时全打到 group 0）。

| 场景 | groups | conc×batch | ~墙钟 TPS | 备注 |
|------|-------:|------------|----------:|------|
| mem 基线 | 1 | 4×8 | ~345k | 文档 C″ |
| mem 推高单 Group | 1 | 32×16 | **~886k** | 单 leader 深流水线峰值 |
| mem 多 Group | 8 | 8×8 | ~389k | 略高于 4×8 单 Group |
| mem 多 Group | 16 | 32×16 | ~561k | 多 Group 较好配方 |
| mem 多 Group | 32 | 64×8 | ~290k | 过订阅，回落 |
| file sync=0 | 1 | 8×16 | **~158k–184k** | 此前头条；不是磁盘硬顶 |
| file sync=0 | 1 | **32×16 / 64×16 / 32×32** | **~390k–494k** | 加深在途；page cache 远未打满 |
| file sync=0 | 1 node | 32×16 | **~830k** | 无 quorum — 本机 append 快得多 |
| file sync=0 | 8 | 8×16 | ~47k | 多 `log.bin` 抢盘，总 TPS **下降** |
| file sync=0 | 16 | 16×16 | ~46k | 同上 |

**为何 file sync=0 不是「page cache 那种百万级」：** 本机纯缓冲写 ~500 万+/s；Raft 仍付每条 openraft + **3 副本** append + quorum RTT + `FileLogStore` mutex。抬墙钟优先加大 `conc×batch`。

**Coalesce 调优（2026-07-22）：** 旧实现在 append 任务上 `sleep(coalesce_us)` → 深流水线≈`1/50µs ≈ 2 万` TPS。现改为 coalesce/stream **后台 flusher 延迟刷盘**（append 路径不再睡）。修复后 `8×16` + `coalesce=50` ≈ **~15 万**（原先 ~2.9 万）；仍略低于 `coalesce=0`（~18 万）。

**Timed group-commit / `hold_overlap`（2026-07-22，sync=1）：**
`--bench-file-hold-overlap` + `coalesce_us` 5–50ms：重叠 append **不再提前刷**，只靠定时器（及字节/回调上限）。

| 窗口 | 流水线 | 约 TPS | p50 |
|------|--------|--------|-----|
| 无 hold，coal=5ms | 64×128 | **~11k** | ~3ms |
| hold 5ms | 64×128 | ~10k | ~3ms |
| hold 10ms | 64×128 | ~9k | ~3–4ms |
| hold 20ms | 64×128 | ~8k | ~4ms |
| hold 50ms | 64×128 / 128×256 | **~4–4.5k** | ~7–15ms |

**上限结论（仅在默认 pe=300、hold 对照下）：** 拉长 coalesce/hold 到 20–50ms **不会抬吞吐**——复制批仍饿死时，窗口里堆不出大批次，反而空等掉 TPS。真正突破靠抬 `max_payload_entries` + 深流水线（见下节 / [M4](./specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)），不是拉长空窗口。Hold 配方仅作对照：

```bash
# 纯定时窗口对照（pe 默认时预期更慢，非生产配方）
cargo run -p multiraft-demo --release -- --mode bench --nodes 3 --groups 1 \
  --bench-file-log --bench-file-sync-level 1 \
  --bench-ops 8000 --bench-concurrency 32 --bench-batch-size 64 \
  --bench-file-hold-overlap --bench-file-coalesce-us 50000 \
  --bench-file-stream-buf 67108864
```

**Disk pipeline + 合并（2026-07-22，停损 B = 3-node ≥2× ≈20–25k；其后 N2a 远超）：**

已落地：
1. **Double-buffer flush**：`fdatasync` 期间释放 state 锁，append 可继续入队。
2. **defer 时 `append` 不再 await sync**：避免卡死 openraft 命令循环（原先 outstanding≈1）。
3. **`api_batch_linger_ms` 等 knobs** + **`propose_many`**；压测仍用 **`propose_batch`**（`try_join_all`）保客户端深度。
4. **N2a `max_payload_entries`**：默认 300 是隐藏复制批上限；抬到 4096 后 follower 更肥的 AppendEntries → 更少次 `fdatasync`/entry。

| 场景 | 约墙钟 TPS | 说明 |
|------|------------|------|
| 3-node sync=1，payload=默认(300) | **~10–13k** | N2a 前 |
| 3-node sync=1，`128×256` coal=1ms，**payload=4096** | **~100–110k** | N2a 工作点 |
| 3-node sync=1，`256×256` coal=0，**payload=8192** | **~145k**（中位） | **性价比拐点** |
| 3-node sync=1，`256×256`，**payload=16k–24k** | **~25万**（中位平台） | 再加大 pe 无收益 |
| 1-node sync=1 pe=12k 256×256 | **~41万** | 本地上限参照 |

**N2a 配方（性价比拐点）：**

```bash
cargo run -p multiraft-demo --release -- --mode bench --nodes 3 --groups 1 \
  --bench-file-log --bench-file-sync-level 1 \
  --bench-ops 48000 --bench-concurrency 256 --bench-batch-size 256 \
  --bench-file-coalesce-us 0 \
  --bench-max-payload-entries 8192
# 摸平台：--bench-max-payload-entries 16384
```

完整理念与实现：[M4 规格](./specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)。  
单币对推荐配置与 2026-07-27 拐点表：[perf-single-symbol.zh-CN.md](./perf-single-symbol.zh-CN.md)。

**结论：** mem 下多 Group **不等于**自动更高总吞吐——单 Group 深流水线往往更高；多 Group 价值在隔离与扩展。file 下多 Group 更容易抢盘，墙钟 TPS 可能明显变差。
