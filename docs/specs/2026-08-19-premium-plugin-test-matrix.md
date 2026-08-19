# Premium plugin test matrix

**中文：** [2026-08-19-premium-plugin-test-matrix.zh-CN.md](./2026-08-19-premium-plugin-test-matrix.zh-CN.md)

**Branch:** `feature/standby-premium-parity`  
**Related:** [plugin-v1-design](./2026-08-19-plugin-v1-design.md) · [standby-parity](./2026-07-20-aeron-standby-parity-design.md)

---

## Scope

| Layer | What | Must stay green |
|-------|------|-----------------|
| **Standby P0–P3** | Core HA (already on `main`) | `standby_premium`, `standby_p2`, `standby_p3`, `chaos_standby` |
| **Plugins N2/N3** | metrics / archive / transition / auth | `plugin_premium_parity`, `plugin_propose_metrics` |
| **Regression** | Empty registry default | `cargo test --workspace`, `./scripts/test_all.sh` |

---

## Core test IDs (plugins)

| ID | Plugin | Assertion | Test location |
|----|--------|-----------|---------------|
| **T-M1** | metrics | `propose` records `client_write` with `count ≥ 1` | `plugin_propose_metrics.rs`, `plugin_premium_parity.rs` |
| **T-M2** | metrics | `p50` / `p99` non-zero after ≥5 samples | `multiraft-plugin-metrics` unit test |
| **T-A1** | archive | `list_positions` returns catalog entries sorted by index | `plugin_premium_parity.rs`, `multiraft-plugin-archive` unit |
| **T-A2** | archive | `export_at(index, term)` returns sha256 + size | `multiraft-plugin-archive` unit |
| **T-T1** | transition | disabled policy → `None` | `multiraft-plugin-transition` unit |
| **T-T2** | transition | lag ≥ threshold + learner → `PromoteStandby` | `plugin_premium_parity.rs` |
| **T-X1** | auth | empty token → allow (lab) | `multiraft-plugin-auth` unit |
| **T-X2** | auth | Bearer mismatch → deny | `plugin_premium_parity.rs` |
| **T-O1** | ops | `premium_registry` wires metrics + transition + auth | `multiraft-plugin-ops` unit |

---

## Core test IDs (Standby Premium — regression)

| ID | Phase | Assertion | Test location |
|----|-------|-----------|---------------|
| **T-P0-1** | P0 | HTTP recover from standby ad | `standby_premium.rs` |
| **T-P0-2** | P0 | Throttle does not block leader propose | `standby_premium.rs` |
| **T-P0-3** | P0 | Bad ad → skip fetch, log replay OK | `chaos_standby.rs` |
| **T-P1-1** | P1 | Promote + kill old voter | `chaos_standby.rs` |
| **T-P1-2** | P1 | Promote then demote membership | `standby_premium.rs` |
| **T-P2-1** | P2 | Multi-standby best ad | `standby_p2.rs` |
| **T-P2-2** | P2 | Daisy chain snapshot | `standby_p2.rs` |
| **T-P2-3** | P2 | Range chunked fetch | `standby_p2.rs` |
| **T-P3-1** | P3 | Standby `read_stale` | `standby_p3.rs` |

---

## Demo / ops

| ID | Command | Expected | Test |
|----|---------|----------|------|
| **T-D1** | `--premium-plugins --admin-token tok` + Bearer on `GET /metrics/propose-stages` | 200 + stages JSON | `plugin_demo_admin.rs` |
| **T-D2** | Same + `GET /admin/archive/0/positions` | 200 or empty positions | `plugin_demo_admin.rs` |
| **T-D3** | Missing Bearer when token set | 401 | `plugin_demo_admin.rs` |
| **T-N2b** | Full staged metrics after propose | sub-stages recorded via openraft runtime-stats | `stage_metrics.rs` |
| **T-N2c** | `--mode bench --bench-stage-metrics` | JSON includes `propose_stages` | manual / demo |
| **T-T3** | `--transition-auto` | background lag promote loop | `transition_loop.rs` |
| **T-N3b-data** | `GET .../export/{i}/{t}/data` or `multiraft-ops archive-export --out` | snapshot bytes | demo + CLI |

See also [recovery playbook](./2026-08-19-premium-recovery-playbook.md).

---

## CI gate

```bash
cargo test --workspace
cargo test -p multiraft-net --test plugin_premium_parity --test plugin_propose_metrics --test plugin_demo_admin
cargo test -p multiraft-net --test chaos_standby --test standby_premium --test standby_p2 --test standby_p3
./scripts/test_all.sh
```

All must pass before merge to `main`.
