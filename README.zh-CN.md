# multiraft

面向撮合高可用的 **薄 Multi-Raft** 库：每交易对一个 Raft Group、节点间连接复用、FSM 可插拔。

基于 [openraft](https://github.com/databendlabs/openraft) + `openraft-multi`（精确锁版本）。本仓一期交付运行时与 Demo；二期（可选，在下游应用中）接入 RMQ Leader propose 与可插拔撮合 FSM。

**许可：** [Apache License 2.0](LICENSE)  
**English：** [README.md](README.md)  
**Wiki：** [中文](docs/wiki/zh/Home.md) · [English](docs/wiki/en/Home.md)  
**技术亮点：** [中文](docs/spotlight/2026-07-hotpath-sync1.zh-CN.md) · [English](docs/spotlight/2026-07-hotpath-sync1.md)

---

## 特性

- 同进程多 Group；peer 连接 **O(节点)**，非 O(Group)
- `MultiRaft`：`propose` / `propose_batch` / `read_linearizable` / 领导权回调
- 每 Group 文件持久化（可重启恢复）；sync 档位 **0/1/2**（与 Aeron 对齐）
- Aeron 启发式热路径：类型化进程内 RPC、流水线 propose、合并 / 流式落盘
- 类 Standby Premium HA：learner standby、快照卸载、promote/demote、daisy 链、stale 读
- 多进程 gRPC Demo + Admin HTTP（验收 / Jepsen）
- acceptance、chaos、porcupine、本地 Jepsen

### 与 Aeron Cluster / Standby Premium

multiraft **不是** Aeron 的 fork。在 openraft（Apache 2.0 / Rust）上追求撮合 HA **语义对齐**，并借鉴 Aeron 热路径思路 —— 不做 Media Driver、SBE 或商业 Cluster。

- **本机实测（3 voter、进程内）：** mem 墙钟 **~300k+** TPS；file sync=0 深流水线 **~117k–187k**；顺序 file **~2k**；sync=1 顺序 **~25 TPS**，深流水线 + 大 pe **~15–25万**（见 [技术亮点](docs/spotlight/2026-07-hotpath-sync1.zh-CN.md) / [M4](docs/specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)）。
- **选 multiraft** 做嵌入式 Rust/openraft 撮合 HA；**选 Aeron 商业版** 要 Media Driver、完整 Archive、ClusteredService 与 Real Logic 支持。

完整对照：[docs/compare/aeron-commercial.zh-CN.md](docs/compare/aeron-commercial.zh-CN.md) · [English](docs/compare/aeron-commercial.md)。  
性能上限：[docs/perf.zh-CN.md](docs/perf.zh-CN.md) · [English](docs/perf.md)。

---

## 架构

```text
multiraft-demo → multiraft-net (MultiRaft + 共享 gRPC)
                    ├── multiraft-core
                    ├── multiraft-fsm
                    └── multiraft-store
```

完整说明：[docs/ARCHITECTURE.zh-CN.md](docs/ARCHITECTURE.zh-CN.md) · [English](docs/ARCHITECTURE.md) · Wiki [架构](docs/wiki/zh/Architecture.md)

### 依赖锁定

| Crate | 版本 |
|-------|------|
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

见 [docs/upstream.zh-CN.md](docs/upstream.zh-CN.md) · [English](docs/upstream.md)。

---

## 快速开始

```bash
git clone https://github.com/lanpishu6300/multiraft.git
cd multiraft
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
cargo test --workspace
./scripts/run_demo_cluster.sh
```

端口与 Admin API 见 [快速开始](docs/wiki/zh/Getting-Started.md)。

### Standby 运维（可选）

> **仅限实验室：** Admin HTTP **无鉴权**，绑定 `127.0.0.1`。勿在无鉴权网关前将 `/admin/*`、`/snapshots/*` 暴露到不可信网络。

```bash
STANDBY=1 ./scripts/run_demo_cluster.sh
curl -s http://127.0.0.1:21100/admin/groups/0/status
curl -s -X POST http://127.0.0.1:21100/admin/standby_snapshot/0
curl -s http://127.0.0.1:21103/admin/catalog/0
curl -s -X POST http://127.0.0.1:21100/admin/replicate_standby_snapshot/0
curl -s http://127.0.0.1:21103/groups/0/stale
```

Voter 重启时若本地已有更新的 snapshot ad，会自动调用 `try_recover_from_standby_ads`。

### 一致性（每 Group）

| API | 模型 |
|-----|------|
| `propose` Ok | Linearizable 写 |
| `read_linearizable` | Linearizable 读 |
| `read_stale` | 本地 + applied 水位（`enable_stale_queries`） |
| `with_fsm` | 本地 / 可能 stale（调试 / 指标） |

详见 [docs/jepsen.zh-CN.md](docs/jepsen.zh-CN.md) · [English](docs/jepsen.md)。

---

## 验证

```bash
./scripts/acceptance.sh
SCENARIO=standby ./scripts/chaos.sh
STANDBY=1 ./scripts/run_jepsen.sh
./scripts/test_all.sh
```

大重建前建议清理 `target/`。

---

## 下游集成（二期）

```text
一期（本仓）         → 运行时 + Demo + 一致性测试
二期（下游应用）     → 可选 RMQ Leader propose → 可插拔撮合 FSM
```

---

## 文档（双语）

`docs/` 下运维与设计文档均成对提供英文 `foo.md` 与中文 `foo.zh-CN.md`（标题下有语言切换链接）。Wiki 已在 `docs/wiki/zh/` 与 `docs/wiki/en/` 双语维护。

| 文档 | 说明 |
|------|------|
| [docs/README.zh-CN.md](docs/README.zh-CN.md) · [English](docs/README.md) | 索引（EN \| 中文列） |
| [docs/ARCHITECTURE.zh-CN.md](docs/ARCHITECTURE.zh-CN.md) · [English](docs/ARCHITECTURE.md) | Crate 边界 |
| [设计规格（中文）](docs/specs/2026-07-18-multiraft-design.zh-CN.md) · [English](docs/specs/2026-07-18-multiraft-design.md) | 设计 |
| [热路径 / 设计理念](docs/specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) · [English](docs/specs/2026-07-21-aeron-inspired-hotpath-design.md) | Aeron 启发热路径 |
| [对标商业 Aeron](docs/compare/aeron-commercial.zh-CN.md) · [English](docs/compare/aeron-commercial.md) | 宣传与技术对照 |
| [Wiki 首页](docs/wiki/zh/Home.md) · [English](docs/wiki/en/Home.md) | Wiki |
| [docs/perf.zh-CN.md](docs/perf.zh-CN.md) · [English](docs/perf.md) | 性能 / 压测 |
| [CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md) · [English](CONTRIBUTING.md) | 贡献指南 |
| [SUPPORT.zh-CN.md](SUPPORT.zh-CN.md) · [English](SUPPORT.md) | 支持渠道 |
| [SECURITY.zh-CN.md](SECURITY.zh-CN.md) · [English](SECURITY.md) | 安全报告 |
