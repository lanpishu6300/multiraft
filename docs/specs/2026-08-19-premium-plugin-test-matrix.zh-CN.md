# Premium 插件测试矩阵

**English：** [2026-08-19-premium-plugin-test-matrix.md](./2026-08-19-premium-plugin-test-matrix.md)

**分支：** `feature/standby-premium-parity`  
**关联：** [plugin-v1-design](./2026-08-19-plugin-v1-design.zh-CN.md) · [standby-parity](./2026-07-20-aeron-standby-parity-design.zh-CN.md)

---

## 范围

| 层 | 内容 | 必须保持绿 |
|----|------|-----------|
| **Standby P0–P3** | 核心 HA（已在 `main`） | `standby_premium`、`standby_p2`、`standby_p3`、`chaos_standby` |
| **插件 N2/N3** | metrics / archive / transition / auth | `plugin_premium_parity`、`plugin_propose_metrics` |
| **回归** | 默认空 registry | `cargo test --workspace`、`./scripts/test_all.sh` |

---

## 核心测试 ID（插件）

| ID | 插件 | 断言 | 测试位置 |
|----|------|------|----------|
| **T-M1** | metrics | `propose` 记录 `client_write` 且 `count ≥ 1` | `plugin_propose_metrics.rs`、`plugin_premium_parity.rs` |
| **T-M2** | metrics | ≥5 样本后 `p50` / `p99` 非零 | `multiraft-plugin-metrics` 单元测试 |
| **T-A1** | archive | `list_positions` 按 index 返回 catalog 条目 | `plugin_premium_parity.rs`、`multiraft-plugin-archive` 单元 |
| **T-A2** | archive | `export_at(index, term)` 返回 sha256 + size | `multiraft-plugin-archive` 单元 |
| **T-T1** | transition | 禁用策略 → `None` | `multiraft-plugin-transition` 单元 |
| **T-T2** | transition | lag ≥ 阈值且为 learner → `PromoteStandby` | `plugin_premium_parity.rs` |
| **T-X1** | auth | 空 token → 允许（实验室） | `multiraft-plugin-auth` 单元 |
| **T-X2** | auth | Bearer 不匹配 → 拒绝 | `plugin_premium_parity.rs` |
| **T-O1** | ops | `premium_registry` 组装 metrics + transition + auth | `multiraft-plugin-ops` 单元 |

---

## 核心测试 ID（Standby Premium — 回归）

| ID | 阶段 | 断言 | 测试位置 |
|----|------|------|----------|
| **T-P0-1** | P0 | 从 standby ad HTTP 恢复 | `standby_premium.rs` |
| **T-P0-2** | P0 | 节流不阻塞 leader propose | `standby_premium.rs` |
| **T-P0-3** | P0 | 坏 ad → 跳过拉取，log replay OK | `chaos_standby.rs` |
| **T-P1-1** | P1 | Promote 后 kill 旧 voter | `chaos_standby.rs` |
| **T-P1-2** | P1 | Promote 再 demote | `standby_premium.rs` |
| **T-P2-1** | P2 | 多 standby 选最新 ad | `standby_p2.rs` |
| **T-P2-2** | P2 | Daisy 链快照 | `standby_p2.rs` |
| **T-P2-3** | P2 | Range 分块拉取 | `standby_p2.rs` |
| **T-P3-1** | P3 | Standby `read_stale` | `standby_p3.rs` |

---

## Demo / 运维（手工或后续自动化）

| ID | 命令 | 期望 |
|----|------|------|
| **T-D1** | `--premium-plugins --admin-token tok` + Bearer 访问 `GET /metrics/propose-stages` | 200 + stages JSON |
| **T-D2** | 同上 + `GET /admin/archive/0/positions` | 200 或空 positions |
| **T-D3** | 设 token 但缺 Bearer | 401 |

---

## CI 门禁

```bash
cargo test --workspace
cargo test -p multiraft-net --test plugin_premium_parity --test plugin_propose_metrics
cargo test -p multiraft-net --test chaos_standby --test standby_premium --test standby_p2 --test standby_p3
./scripts/test_all.sh
```

合并 `main` 前必须全绿。
