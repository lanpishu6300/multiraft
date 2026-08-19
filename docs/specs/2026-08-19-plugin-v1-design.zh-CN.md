# 插件架构 v1

**English：** [2026-08-19-plugin-v1-design.md](./2026-08-19-plugin-v1-design.md)

**日期：** 2026-08-19  
**分支：** `spec/plugin-v1`  
**状态：** 草案（v1 接口 + 首个插件）  
**关联：** [aeron-next-borrow N2b](./2026-07-22-aeron-next-borrow.zh-CN.md) · [ARCHITECTURE.zh-CN.md](../ARCHITECTURE.zh-CN.md)

---

## 0. 目标

在不引入 Aeron 运行时、不把业务 FSM 拉进 core 的前提下，增加**可选、可组合插件**。第一阶段交付：

1. `multiraft-core::plugin` trait 骨架
2. 首个插件：`multiraft-plugin-metrics`（N2b 分段 propose 延迟）
3. `MultiRaft::propose` / 生命周期的最小 hook

后续插件（同一模式）：`multiraft-plugin-archive`、`-transition`、`-auth`、`-ops`。

---

## 1. Crate 边界

```text
multiraft-core/plugin.rs     trait + PluginRegistry（无插件实现）
multiraft-net                分发 hook；不依赖 plugin crate
crates/multiraft-plugin-*    可选 workspace member
multiraft-demo               通过 Cargo feature 组装（后续）
```

**规则：** `multiraft-*` 核心 crate **不得**依赖 `multiraft-plugin-*`。插件只依赖 `multiraft-core`（metrics 插件不依赖 net）。

---

## 2. 核心 trait（v1）

### `Plugin`

任意扩展的生命周期：

| Hook | 时机 |
|------|------|
| `on_node_start` | `MultiRaft::start*` 完成本地节点启动后 |
| `on_node_stop` | `MultiRaft::shutdown` 之后 |
| `on_group_ready` | 本地 `create_group` 完成后 |

### `MetricsPlugin` : `Plugin`

| 方法 | 作用 |
|------|------|
| `record_stage(StageSample)` | 记录一次延迟 |
| `snapshot() -> MetricsSnapshot` | 可 JSON 导出的聚合 |

### `ProposeStage`（N2b）

| 阶段 | v1 是否接线 | 含义 |
|------|-------------|------|
| `client_enqueued` | 否 | 预留 |
| `client_write` | **是** | openraft `client_write` 墙钟（含 quorum + apply） |
| `leader_append` | 否 | 后续 log-store hook |
| `quorum_ack` | 否 | 后续复制 hook |
| `committed` | 否 | 后续 metrics hook |
| `applied` | 否 | 后续 FSM hook |

### `PluginRegistry`

- `register_plugin` / `register_metrics`
- 可 `Clone`（测试里多节点共享配置；生产每进程一份）
- `metrics_snapshots()` 供 admin 导出

---

## 3. Net 集成（v1）

```rust
MultiRaft::start_with_plugins(config, PluginRegistry::new().register_metrics(...))
MultiRaft::start_cluster_with_plugins(configs, plugins)
SharedFabric::start_node_with_plugins(config, plugins)
```

默认路径（`start` / `start_cluster` / `start_node`）使用空 registry —— **现有测试行为不变**。

`propose` / `propose_batch` 在成功与失败时均记录 `ProposeStage::ClientWrite`。

---

## 4. `multiraft-plugin-metrics`

进程内聚合：

- 键：`(group, ProposeStage)`
- 统计 `count`、`ok_count`、`sum_us`、`max_us`、`p50_us`、`p99_us`（最近 4096 样本窗口）
- `SegmentedLatencyMetrics::to_json()` 供 admin / bench

测试：`multiraft-net/tests/plugin_propose_metrics.rs`

---

## 5. 后续插件（不在 v1）

| Crate | Trait | 说明 |
|-------|-------|------|
| `multiraft-plugin-archive` | `ArchiveBackend` | N3 位点导出 |
| `multiraft-plugin-transition` | `TransitionPolicy` | 自动 promote/demote |
| `multiraft-plugin-ops` | CLI | PremiumClusterTool 子集 |
| `multiraft-plugin-auth` | `AuthLayer` | Admin / stale 读鉴权 |

Demo 组装（计划）：

```toml
[features]
plugin-metrics = ["dep:multiraft-plugin-metrics"]
```

Admin 路由（计划）：`GET /metrics/propose-stages` → `PluginRegistry::metrics_snapshots()`。

---

## 6. 非目标（v1）

- OpenRaft 内部阶段 hook（append / AE ack）
- 动态加载插件（`.so` / WASM）
- 在可序列化的 `ClusterConfig` 里放 plugin 句柄

---

## 7. 验收（v1）

- [x] `multiraft-core::plugin` trait + registry 单元测试
- [x] `multiraft-plugin-metrics` crate + 单元测试
- [x] 注册 metrics 插件时 `MultiRaft::propose` 记录 `client_write`
- [x] 现有 `cargo test --workspace` 行为不变（默认空 registry）
- [ ] Demo feature + admin 路由（后续 PR）
