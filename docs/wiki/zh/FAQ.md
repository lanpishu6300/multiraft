# 常见问题

**English：** [en/FAQ.md](../en/FAQ.md)

### 单币对 sync=1 该用什么配置？

见 **[单币对推荐配置与压测](../../perf-single-symbol.zh-CN.md)**（pe 8k–12k、`192×256` 等）。实现细节见 [M4](../../specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)。

### 多 Group 会抬高总吞吐吗？

file（尤其 sync=1）下通常 **不会**。Group 数增加时合计墙钟 TPS 往往下降 — 见 **[多 Group 压测](../../perf-multi-group.zh-CN.md)**。多 Group 价值在隔离 / 多币对。

### 热路径 / sync=1 的概述在哪？

见 **[技术亮点：热路径与 sync=1](../../spotlight/2026-07-hotpath-sync1.zh-CN.md)**。实现细节见 [M4 规格](../../specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)。

### 为什么不直接用 SofaJRaft / TiKV raftstore？

Rust 无官方 SofaJRaft；TiKV `raftstore` 厚在 Region / PD / split，撮合按稳定 symbol 分片用不上。本库只做薄 Multi-Raft（共享连接 + 多 Group）。

### `propose` 超时算失败吗？

不算确定失败。超时 / 断连 / 切主窗口为**不确定写**，须用同一幂等键重试。见 Consistency Contract。

### `with_fsm` 能当查单真值吗？

不能。可能 stale；生产读用 `read_linearizable`。

### Jepsen 报告在哪？

跑完后在 `jepsen/multiraft/store/latest/`（已 gitignore）。用例源码在 `jepsen/multiraft/src/`。更多见 [一致性与测试](./Consistency.md)。

### 下游集成（二期）？

本仓一期独立交付运行时。二期（可选，在下游应用中）可由撮合进程 / 入站壳依赖本库做 Leader propose，并接入可插拔撮合引擎 FSM。
