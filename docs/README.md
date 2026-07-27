# Documentation index

**中文：** [README.zh-CN.md](./README.zh-CN.md)

Self-contained docs for the **multiraft** GitHub repository (no links outside this tree required to navigate). Every doc below has an English + Chinese (`.zh-CN.md`) pair.

## Start here

| Doc (EN) | 中文 | Audience |
|----------|------|----------|
| [spotlight/2026-07-hotpath-sync1.md](./spotlight/2026-07-hotpath-sync1.md) | [spotlight/2026-07-hotpath-sync1.zh-CN.md](./spotlight/2026-07-hotpath-sync1.zh-CN.md) | Technical highlights — hot path and sync=1 |
| [wiki/en/Home.md](./wiki/en/Home.md) | [wiki/zh/Home.md](./wiki/zh/Home.md) | Wiki (getting started / FAQ / roadmap) |
| [../README.md](../README.md) | [../README.zh-CN.md](../README.zh-CN.md) | Repository README |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | [ARCHITECTURE.zh-CN.md](./ARCHITECTURE.zh-CN.md) | Contributors — crate boundaries & contract |
| [jepsen.md](./jepsen.md) | [jepsen.zh-CN.md](./jepsen.zh-CN.md) | Consistency Contract, porcupine, Jepsen |
| [chaos-checklist.md](./chaos-checklist.md) | [chaos-checklist.zh-CN.md](./chaos-checklist.zh-CN.md) | Chaos / failover coverage |
| [upstream.md](./upstream.md) | [upstream.zh-CN.md](./upstream.zh-CN.md) | openraft pin & upgrade notes |

## Designs (specs)

| Spec (EN) | 中文 | Topic |
|-----------|------|-------|
| [2026-07-18-multiraft-design.md](./specs/2026-07-18-multiraft-design.md) | [2026-07-18-multiraft-design.zh-CN.md](./specs/2026-07-18-multiraft-design.zh-CN.md) | Thin Multi-Raft for matching HA |
| [2026-07-20-standby-async-snapshot-design.md](./specs/2026-07-20-standby-async-snapshot-design.md) | [2026-07-20-standby-async-snapshot-design.zh-CN.md](./specs/2026-07-20-standby-async-snapshot-design.zh-CN.md) | Standby async snapshot (Aeron-aligned) |
| [2026-07-20-aeron-standby-parity-design.md](./specs/2026-07-20-aeron-standby-parity-design.md) | [2026-07-20-aeron-standby-parity-design.zh-CN.md](./specs/2026-07-20-aeron-standby-parity-design.zh-CN.md) | Aeron Standby premium parity (P0–P3) |
| [2026-07-21-aeron-inspired-hotpath-design.md](./specs/2026-07-21-aeron-inspired-hotpath-design.md) | [2026-07-21-aeron-inspired-hotpath-design.zh-CN.md](./specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) | Hot path + design philosophy (M1–M4) |
| [2026-07-22-sync1-disk-pipeline-merge.md](./specs/2026-07-22-sync1-disk-pipeline-merge.md) | [2026-07-22-sync1-disk-pipeline-merge.zh-CN.md](./specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md) | M4: deep pipeline / sync=1 group-commit / replication batch + knees |
| [2026-07-22-aeron-next-borrow.md](./specs/2026-07-22-aeron-next-borrow.md) | [2026-07-22-aeron-next-borrow.zh-CN.md](./specs/2026-07-22-aeron-next-borrow.zh-CN.md) | Next Aeron borrow backlog (N1–N3) |

## Compare / positioning

| Doc (EN) | 中文 | Topic |
|----------|------|-------|
| [compare/aeron-commercial.md](./compare/aeron-commercial.md) | [compare/aeron-commercial.zh-CN.md](./compare/aeron-commercial.zh-CN.md) | multiraft vs Aeron Cluster / Standby Premium |
| [perf.md](./perf.md) | [perf.zh-CN.md](./perf.zh-CN.md) | Measured TPS ceilings & bench recipes |
| [perf-single-symbol.md](./perf-single-symbol.md) | [perf-single-symbol.zh-CN.md](./perf-single-symbol.zh-CN.md) | Single-symbol recommended config & sync=1 knees |
| [perf-multi-group.md](./perf-multi-group.md) | [perf-multi-group.zh-CN.md](./perf-multi-group.zh-CN.md) | Multi-group wall TPS (mem / sync=0 / sync=1) |

## Plans

| Plan (EN) | 中文 | Topic |
|-----------|------|-------|
| [2026-07-18-multiraft.md](./plans/2026-07-18-multiraft.md) | [2026-07-18-multiraft.zh-CN.md](./plans/2026-07-18-multiraft.zh-CN.md) | Phase-1 library + demo |
| [2026-07-18-multiraft-grpc.md](./plans/2026-07-18-multiraft-grpc.md) | [2026-07-18-multiraft-grpc.zh-CN.md](./plans/2026-07-18-multiraft-grpc.zh-CN.md) | Cross-process gRPC (Phase-1.5) |

## Operations

| Doc (EN) | 中文 | Topic |
|----------|------|-------|
| [jepsen.md](./jepsen.md) | [jepsen.zh-CN.md](./jepsen.zh-CN.md) | Local Jepsen + porcupine CI gate |
| [chaos-checklist.md](./chaos-checklist.md) | [chaos-checklist.zh-CN.md](./chaos-checklist.zh-CN.md) | Kill leader / rolling / double-kill |
| [../jepsen/multiraft/README.md](../jepsen/multiraft/README.md) | [../jepsen/multiraft/README.zh-CN.md](../jepsen/multiraft/README.zh-CN.md) | Jepsen suite layout |
| [../scripts/](../scripts/) | — | `acceptance.sh`, `chaos.sh`, `run_jepsen.sh`, `test_all.sh` |

## Meta

| Doc (EN) | 中文 |
|----------|------|
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | [../CONTRIBUTING.zh-CN.md](../CONTRIBUTING.zh-CN.md) |
| [../SUPPORT.md](../SUPPORT.md) | [../SUPPORT.zh-CN.md](../SUPPORT.zh-CN.md) |
| [../SECURITY.md](../SECURITY.md) | [../SECURITY.zh-CN.md](../SECURITY.zh-CN.md) |
