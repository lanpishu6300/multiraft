# 文档索引

**English：** [README.md](./README.md)

**multiraft** GitHub 仓库的自包含文档（导航不必依赖仓外链接）。下表每份文档均有英文 + 中文（`.zh-CN.md`）成对版本。

## 从这里开始

| Doc (EN) | 中文 | 读者 |
|----------|------|------|
| [spotlight/2026-07-hotpath-sync1.md](./spotlight/2026-07-hotpath-sync1.md) | [spotlight/2026-07-hotpath-sync1.zh-CN.md](./spotlight/2026-07-hotpath-sync1.zh-CN.md) | 技术亮点 — 热路径与 sync=1 |
| [wiki/en/Home.md](./wiki/en/Home.md) | [wiki/zh/Home.md](./wiki/zh/Home.md) | Wiki（入门 / FAQ / 路线图） |
| [../README.md](../README.md) | [../README.zh-CN.md](../README.zh-CN.md) | 仓库 README |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | [ARCHITECTURE.zh-CN.md](./ARCHITECTURE.zh-CN.md) | 贡献者 — crate 边界与契约 |
| [jepsen.md](./jepsen.md) | [jepsen.zh-CN.md](./jepsen.zh-CN.md) | Consistency Contract、porcupine、Jepsen |
| [chaos-checklist.md](./chaos-checklist.md) | [chaos-checklist.zh-CN.md](./chaos-checklist.zh-CN.md) | Chaos / 切主覆盖 |
| [upstream.md](./upstream.md) | [upstream.zh-CN.md](./upstream.zh-CN.md) | openraft 锁定与升版说明 |

## 设计（specs）

| Spec (EN) | 中文 | 主题 |
|-----------|------|------|
| [2026-07-18-multiraft-design.md](./specs/2026-07-18-multiraft-design.md) | [2026-07-18-multiraft-design.zh-CN.md](./specs/2026-07-18-multiraft-design.zh-CN.md) | 撮合高可用薄 Multi-Raft |
| [2026-07-20-standby-async-snapshot-design.md](./specs/2026-07-20-standby-async-snapshot-design.md) | [2026-07-20-standby-async-snapshot-design.zh-CN.md](./specs/2026-07-20-standby-async-snapshot-design.zh-CN.md) | Standby 异步快照（对齐 Aeron） |
| [2026-07-20-aeron-standby-parity-design.md](./specs/2026-07-20-aeron-standby-parity-design.md) | [2026-07-20-aeron-standby-parity-design.zh-CN.md](./specs/2026-07-20-aeron-standby-parity-design.zh-CN.md) | Aeron Standby Premium 对等（P0–P3） |
| [2026-07-21-aeron-inspired-hotpath-design.md](./specs/2026-07-21-aeron-inspired-hotpath-design.md) | [2026-07-21-aeron-inspired-hotpath-design.zh-CN.md](./specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) | 热路径 + 设计理念（M1–M4） |
| [2026-07-22-sync1-disk-pipeline-merge.md](./specs/2026-07-22-sync1-disk-pipeline-merge.md) | [2026-07-22-sync1-disk-pipeline-merge.zh-CN.md](./specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md) | M4：深流水线 / sync=1 组提交 / 复制批与拐点 |
| [2026-07-22-aeron-next-borrow.md](./specs/2026-07-22-aeron-next-borrow.md) | [2026-07-22-aeron-next-borrow.zh-CN.md](./specs/2026-07-22-aeron-next-borrow.zh-CN.md) | 下一阶段 Aeron 借鉴 backlog（N1–N3） |

## 对照 / 定位

| Doc (EN) | 中文 | 主题 |
|----------|------|------|
| [compare/aeron-commercial.md](./compare/aeron-commercial.md) | [compare/aeron-commercial.zh-CN.md](./compare/aeron-commercial.zh-CN.md) | multiraft vs Aeron Cluster / Standby Premium |
| [perf.md](./perf.md) | [perf.zh-CN.md](./perf.zh-CN.md) | 实测 TPS 上限与压测配方 |
| [perf-single-symbol.md](./perf-single-symbol.md) | [perf-single-symbol.zh-CN.md](./perf-single-symbol.zh-CN.md) | 单币对推荐配置与 sync=1 拐点 |

## 计划（plans）

| Plan (EN) | 中文 | 主题 |
|-----------|------|------|
| [2026-07-18-multiraft.md](./plans/2026-07-18-multiraft.md) | [2026-07-18-multiraft.zh-CN.md](./plans/2026-07-18-multiraft.zh-CN.md) | 一期库 + Demo |
| [2026-07-18-multiraft-grpc.md](./plans/2026-07-18-multiraft-grpc.md) | [2026-07-18-multiraft-grpc.zh-CN.md](./plans/2026-07-18-multiraft-grpc.zh-CN.md) | 跨进程 gRPC（Phase-1.5） |

## 运维

| Doc (EN) | 中文 | 主题 |
|----------|------|------|
| [jepsen.md](./jepsen.md) | [jepsen.zh-CN.md](./jepsen.zh-CN.md) | 本地 Jepsen + porcupine CI 门禁 |
| [chaos-checklist.md](./chaos-checklist.md) | [chaos-checklist.zh-CN.md](./chaos-checklist.zh-CN.md) | 杀主 / 滚动 / 双杀 |
| [../jepsen/multiraft/README.md](../jepsen/multiraft/README.md) | [../jepsen/multiraft/README.zh-CN.md](../jepsen/multiraft/README.zh-CN.md) | Jepsen 套件布局 |
| [../scripts/](../scripts/) | — | `acceptance.sh`, `chaos.sh`, `run_jepsen.sh`, `test_all.sh` |

## 元文档

| Doc (EN) | 中文 |
|----------|------|
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | [../CONTRIBUTING.zh-CN.md](../CONTRIBUTING.zh-CN.md) |
| [../SUPPORT.md](../SUPPORT.md) | [../SUPPORT.zh-CN.md](../SUPPORT.zh-CN.md) |
| [../SECURITY.md](../SECURITY.md) | [../SECURITY.zh-CN.md](../SECURITY.zh-CN.md) |
