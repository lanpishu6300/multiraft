# multiraft

Thin **Multi-Raft** library for matching HA — one Raft group per trading symbol,
shared peer connections, pluggable FSM.

Built on [openraft](https://github.com/databendlabs/openraft) + `openraft-multi`
(exact pin). Phase-1 stops at the runtime + demo. Phase-2 (optional, in a
downstream app): RMQ Leader propose + a pluggable matching FSM.

**License:** [Apache License 2.0](LICENSE)  
**中文：** [README.zh-CN.md](README.zh-CN.md)  
**Wiki：** [English](docs/wiki/en/Home.md) · [中文](docs/wiki/zh/Home.md)  
**Highlights：** [English](docs/spotlight/2026-07-hotpath-sync1.md) · [中文](docs/spotlight/2026-07-hotpath-sync1.zh-CN.md)

---

## Features

- Multi-group Raft in one process; peer links **O(nodes)**, not O(groups)
- `MultiRaft` facade: `propose`, `propose_batch`, `read_linearizable`, leader callbacks
- File-backed log / state / snapshot per group (restart recovery); sync levels **0/1/2** (Aeron-aligned)
- Aeron-inspired hot path: typed in-process RPC, pipelined propose, coalesced / streamed file append
- Standby Premium–like HA: learner standby, snapshot offload, promote/demote, daisy chain, stale reads
- Multi-process gRPC demo (`multiraft-demo`) + admin HTTP for ops / Jepsen
- Acceptance, chaos scripts, porcupine linearizability test, local Jepsen suite

### vs Aeron Cluster / Standby Premium

multiraft is **not** a fork of Aeron. It targets matching-HA **semantic parity** on openraft (Apache 2.0 Rust), with an Aeron-inspired hot path — not Media Driver, SBE, or commercial Cluster.

- **Measured (3 voters, in-process, this machine):** mem wall **~300k+** TPS; file sync=0 deep pipeline **~117k–187k**; sequential file **~2k**; sync=1 sequential **~25 TPS**, deep pipeline + fat pe **~150k–250k** (see [highlights](docs/spotlight/2026-07-hotpath-sync1.md) / [M4](docs/specs/2026-07-22-sync1-disk-pipeline-merge.md)).
- **Choose multiraft** for embedded Rust/openraft matching HA; **choose Aeron commercial** for Media Driver, full Archive, ClusteredService, and Real Logic support.

Full comparison: [docs/compare/aeron-commercial.md](docs/compare/aeron-commercial.md) · [中文](docs/compare/aeron-commercial.zh-CN.md).  
Perf ceilings: [docs/perf.md](docs/perf.md) · [中文](docs/perf.zh-CN.md).

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│ multiraft-demo (3 OS processes × N groups)                  │
│  Admin HTTP  ·  CounterFsm  ·  kill/restart scripts         │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
                       multiraft-net
                    (MultiRaft + shared gRPC)
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   multiraft-core      multiraft-fsm       multiraft-store
   (types/errors)      (StateMachine)     (per-group files)
```

| Crate | Role |
|-------|------|
| `multiraft-core` | `TypeConfig`, `ClusterConfig`, errors / `ProposeOk` |
| `multiraft-net` | Shared router + **`MultiRaft`** facade |
| `multiraft-fsm` | Pluggable state machine trait |
| `multiraft-store` | File-backed Raft storage |
| `multiraft-demo` | 3-node × N-group acceptance target |

Full notes: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · [中文](docs/ARCHITECTURE.zh-CN.md).

### Dependency pin

| Crate | Version |
|-------|---------|
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

See [docs/upstream.md](docs/upstream.md) · [中文](docs/upstream.zh-CN.md).

---

## Quick Start

### Prerequisites

- Rust (recent stable)
- macOS / Linux
- Optional for Jepsen: Java **17+**, `lein`

```bash
git clone https://github.com/lanpishu6300/multiraft.git
cd multiraft
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
cargo test --workspace
```

### Multi-process demo

```bash
./scripts/run_demo_cluster.sh
```

With `--base-port 21000`:

| Node | Raft gRPC | Admin HTTP |
|------|-----------|------------|
| 1 | `127.0.0.1:21000` | `http://127.0.0.1:21100` |
| 2 | `127.0.0.1:21001` | `http://127.0.0.1:21101` |
| 3 | `127.0.0.1:21002` | `http://127.0.0.1:21102` |

```bash
# Linearizable read / propose (for external clients / Jepsen)
curl -s http://127.0.0.1:21100/groups/0/value
curl -s -X POST http://127.0.0.1:21100/groups/0/inc \
  -H 'content-type: application/json' -d '{"delta":1}'
```

CLI: `--mode`, `--node-id`, `--nodes`, `--base-port`, `--groups`, `--data-dir`,
`--no-auto-propose`, `--role voter|standby`. Set `JEPSEN=1` or `NO_AUTO_PROPOSE=1` on
`run_demo_cluster.sh` to disable the background propose loop.

### Standby ops (optional)

> **Lab only:** admin HTTP is unauthenticated and binds to `127.0.0.1`. Do not expose `/admin/*` or `/snapshots/*` beyond the local machine without an auth front-end.

```bash
STANDBY=1 ./scripts/run_demo_cluster.sh
# status / catalog / ads (admin on any voter; Standby is node 4 → :21103)
curl -s http://127.0.0.1:21100/admin/groups/0/status
curl -s -X POST http://127.0.0.1:21100/admin/standby_snapshot/0
curl -s http://127.0.0.1:21103/admin/catalog/0
curl -s http://127.0.0.1:21100/admin/best_snapshot_ad/0
curl -s -X POST http://127.0.0.1:21100/admin/replicate_standby_snapshot/0
curl -s http://127.0.0.1:21103/groups/0/stale
# warm promote (leader):
curl -s -X POST http://127.0.0.1:21100/admin/promote_standby/0/4
```

Voter restart auto-calls `try_recover_from_standby_ads` for each group when local snapshot ads are present and newer than the SM applied watermark.

### Consistency (per group)

| API | Model |
|-----|--------|
| `propose` Ok | Linearizable write |
| `read_linearizable` | Linearizable read (ReadIndex) |
| `read_stale` | Local + applied watermark (`enable_stale_queries`) |
| `with_fsm` | Local / may be stale — debug / metrics |

See [docs/jepsen.md](docs/jepsen.md) · [中文](docs/jepsen.zh-CN.md).

---

## Verification

```bash
./scripts/acceptance.sh          # ≥10 groups, kill real leader PID
./scripts/chaos.sh               # SCENARIO=kill_leader|rolling|standby|...
STANDBY=1 ./scripts/run_jepsen.sh
./scripts/test_all.sh            # unit + chaos_* + acceptance

cargo test -p multiraft-net --test linearizability_porcupine -- --nocapture
cargo test -p multiraft-net --test chaos_failover --test chaos_standby
```

Chaos checklist: [docs/chaos-checklist.md](docs/chaos-checklist.md) · [中文](docs/chaos-checklist.zh-CN.md).
Performance / load: [docs/perf.md](docs/perf.md) · [中文](docs/perf.zh-CN.md).  
Clean `target/` before large rebuilds if disk is tight.

---

## Downstream integration (phase 2)

```text
Phase-1 (this repo)     → runtime + demo + consistency tests
Phase-2 (downstream app) → optional RMQ Leader propose → pluggable matching FSM
```

---

## Documentation (bilingual)

Every file under `docs/` (architecture, Jepsen, chaos, upstream, specs, plans) has an English `foo.md` and Chinese `foo.zh-CN.md` pair with a language switcher under the H1. Wiki pages are already bilingual under `docs/wiki/en/` and `docs/wiki/zh/`.

| Doc | Topic |
|-----|-------|
| [docs/README.md](docs/README.md) · [中文](docs/README.zh-CN.md) | Index (EN \| 中文 columns) |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · [中文](docs/ARCHITECTURE.zh-CN.md) | Crate boundaries |
| [docs/specs/2026-07-18-multiraft-design.md](docs/specs/2026-07-18-multiraft-design.md) · [中文](docs/specs/2026-07-18-multiraft-design.zh-CN.md) | Design |
| [docs/specs/2026-07-21-aeron-inspired-hotpath-design.md](docs/specs/2026-07-21-aeron-inspired-hotpath-design.md) · [中文](docs/specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) | Hot path / design philosophy |
| [docs/compare/aeron-commercial.md](docs/compare/aeron-commercial.md) · [中文](docs/compare/aeron-commercial.zh-CN.md) | vs Aeron commercial |
| [docs/wiki/en/Home.md](docs/wiki/en/Home.md) · [中文](docs/wiki/zh/Home.md) | Wiki |
| [CONTRIBUTING.md](CONTRIBUTING.md) · [中文](CONTRIBUTING.zh-CN.md) | How to contribute |
| [SUPPORT.md](SUPPORT.md) · [中文](SUPPORT.zh-CN.md) | Help channels |
| [SECURITY.md](SECURITY.md) · [中文](SECURITY.zh-CN.md) | Vulnerability reporting |
