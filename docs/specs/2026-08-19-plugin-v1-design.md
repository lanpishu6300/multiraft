# Plugin architecture v1

**中文：** [2026-08-19-plugin-v1-design.zh-CN.md](./2026-08-19-plugin-v1-design.zh-CN.md)

**Date:** 2026-08-19  
**Branch:** `spec/plugin-v1`  
**Status:** Draft (v1 interfaces + first plugin)  
**Related:** [aeron-next-borrow N2b](./2026-07-22-aeron-next-borrow.md) · [ARCHITECTURE.md](../ARCHITECTURE.md)

---

## 0. Goal

Add **optional, composable plugins** without pulling Aeron runtime or business FSM into `multiraft-core` / `multiraft-net`. Phase 1 ships:

1. Trait skeleton in `multiraft-core::plugin`
2. First plugin: `multiraft-plugin-metrics` (N2b segmented propose latency)
3. Minimal hooks in `MultiRaft::propose` / lifecycle

Future plugins (same pattern): `multiraft-plugin-archive`, `-transition`, `-auth`, `-ops`.

---

## 1. Crate boundaries

```text
multiraft-core/plugin.rs     traits + PluginRegistry (no plugin impls)
multiraft-net                dispatches hooks; no dependency on plugin crates
crates/multiraft-plugin-*    optional workspace members
multiraft-demo               wires plugins via Cargo features (later)
```

**Rule:** `multiraft-*` core crates never depend on `multiraft-plugin-*`. Plugins depend on `multiraft-core` only (metrics plugin has no net dependency).

---

## 2. Core traits (v1)

### `Plugin`

Lifecycle hooks for any extension:

| Hook | When |
|------|------|
| `on_node_start` | After `MultiRaft::start*` returns local node |
| `on_node_stop` | After `MultiRaft::shutdown` |
| `on_group_ready` | After local `create_group` completes |

### `MetricsPlugin` : `Plugin`

| Method | Purpose |
|--------|---------|
| `record_stage(StageSample)` | One latency observation |
| `snapshot() -> MetricsSnapshot` | JSON-exportable aggregate |

### `ProposeStage` (N2b)

| Stage | v1 wired? | Meaning |
|-------|-----------|---------|
| `client_enqueued` | no | Reserved |
| `client_write` | **yes** | Wall time of openraft `client_write` (quorum + apply) |
| `leader_append` | no | Future log-store hook |
| `quorum_ack` | no | Future replication hook |
| `committed` | no | Future metrics hook |
| `applied` | no | Future FSM hook |

### `PluginRegistry`

- `register_plugin` / `register_metrics`
- Cloneable (shared config across nodes in tests; production: one registry per process)
- `metrics_snapshots()` for admin export

---

## 3. Net integration (v1)

```rust
MultiRaft::start_with_plugins(config, PluginRegistry::new().register_metrics(...))
MultiRaft::start_cluster_with_plugins(configs, plugins)
SharedFabric::start_node_with_plugins(config, plugins)
```

Default paths (`start`, `start_cluster`, `start_node`) use an empty registry — **zero behavior change** for existing tests.

`propose` / `propose_batch` record `ProposeStage::ClientWrite` on success and failure.

---

## 4. `multiraft-plugin-metrics`

In-process aggregator:

- Key: `(group, ProposeStage)`
- Tracks `count`, `ok_count`, `sum_us`, `max_us`, `p50_us`, `p99_us` over a bounded recent window (4096 samples)
- `SegmentedLatencyMetrics::to_json()` for admin / bench

Test: `multiraft-net/tests/plugin_propose_metrics.rs`

---

## 5. Next plugins (not in v1)

| Crate | Trait | Notes |
|-------|-------|-------|
| `multiraft-plugin-archive` | `ArchiveBackend` | N3 position export |
| `multiraft-plugin-transition` | `TransitionPolicy` | Auto promote/demote |
| `multiraft-plugin-ops` | CLI wrapper | PremiumClusterTool subset |
| `multiraft-plugin-auth` | `AuthLayer` | Admin / stale read |

Demo wiring (planned):

```toml
[features]
plugin-metrics = ["dep:multiraft-plugin-metrics"]
```

Admin route (planned): `GET /metrics/propose-stages` → `PluginRegistry::metrics_snapshots()`.

---

## 6. Non-goals (v1)

- OpenRaft internal stage hooks (append / AE ack)
- Dynamic plugin loading (`.so` / WASM)
- Putting plugin handles inside serializable `ClusterConfig`

---

## 7. Acceptance (v1)

- [x] `multiraft-core::plugin` traits + registry unit test
- [x] `multiraft-plugin-metrics` crate + unit test
- [x] `MultiRaft::propose` records `client_write` when registry has metrics plugin
- [x] Existing `cargo test --workspace` unchanged (empty registry default)
- [ ] Demo feature + admin route (follow-up PR)
