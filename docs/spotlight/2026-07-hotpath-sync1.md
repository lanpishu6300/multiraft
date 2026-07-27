# multiraft: hot path and file sync=1 throughput

**中文：** [2026-07-hotpath-sync1.zh-CN.md](./2026-07-hotpath-sync1.zh-CN.md)  
**Branch:** `feature/aeron-inspired-hotpath` · openraft / openraft-multi `=0.10.0-alpha.30`  
**Date:** 2026-07

Thin Multi-Raft for matching HA: one Raft group per trading symbol, peer links shared by node count, pluggable FSM. Built on openraft (Apache 2.0). The design borrows Aeron-style mechanical sympathy (sync grades, pipelining, group-commit) without a Media Driver or a replaced consensus stack. This note covers recent hot-path work and **file sync=1** results.

---

## Positioning

| Need | Approach |
|------|----------|
| Fault isolation per symbol | One group per pair; no cross-group transactions |
| Embeddable, auditable Rust | Exact-pin openraft / openraft-multi |
| Graded local durability | sync **0 / 1 / 2** (same numbering as Aeron `file.sync.level`) |
| Throughput with consensus | Overlap many independent `client_write`s; one Raft entry per payload |
| vs Aeron Cluster commercial | Semantic / hot-path borrow; no full Archive / ClusteredService — [compare](../compare/aeron-commercial.md) |

Phase-1 scope: library, multi-process Demo, acceptance / chaos / Jepsen. Matching engine and messaging stay in the application repo.

---

## Measured results (this machine, 3 voters, in-process)

Absolute numbers vary by hardware; use the table for order of magnitude and recipes.

| Scenario | ~wall TPS | Notes |
|----------|-----------|-------|
| Mem, deep pipeline | ~300k+ | Consensus path still present; µs-scale hops |
| File sync=0, deep pipeline | ~117k–187k | Page-cache write × quorum |
| File sync=1, sequential | ~25 | fdatasync per entry × quorum |
| File sync=1, deep pipeline + repl batch | ~150k–250k | Still sync=1; larger entries per durable flush |
| Working point | pe≈8192, `256×256` → ~145k | Plateau ~250k at pe≥16k |

Full data: [perf.md](../perf.md), [M4](../specs/2026-07-22-sync1-disk-pipeline-merge.md).

---

## Technical approach

### 1. Module boundary

```text
multiraft-demo  →  Demo / Admin HTTP / bench
multiraft-net   →  MultiRaft, shared router (typed in-process / gRPC)
multiraft-core / fsm / store  →  types & config, state machine, per-group persistence
```

- Peer links are O(nodes).  
- `propose` Ok means quorum commit and apply.  
- On timeout or error the outcome is uncertain; callers retry with the same idempotency key.  

See [ARCHITECTURE.md](../ARCHITECTURE.md).

### 2. Hot path (M1–M3)

| Milestone | What | Effect |
|-----------|------|--------|
| M1 | Typed in-process RPC; `propose_batch` | No same-process encode; overlap quorum waits |
| M2 / M2b | File group-commit; sync 0/1/2 | Coalesce writes, then flush by grade |
| M3a | Os-level stream buffer | Sequential append at sync=0 |
| M3b | `concurrency × batch` | 100k+ wall TPS on mem / sync=0 |
| M3c | `try_send`, sticky workers, standby fast path | Less scheduler / lock noise |

Out of scope: Media Driver, SBE, full Archive, Consensus Module replacement.  
Spec: [hotpath design](../specs/2026-07-21-aeron-inspired-hotpath-design.md).

### 3. sync=1 (M4)

Sequential sync=1 at ~25 TPS is “pay a full fdatasync × quorum per entry.” Deepening only the client pipeline while leaving `max_payload_entries` at the library default (300) stalls around ~10k wall TPS. Raising the replication batch together with the local disk pipeline reaches ~150k–250k while keeping sync=1 semantics.

Four layers must all be deep enough:

```text
① Client       conc × batch      propose_batch (try_join_all(client_write))
② Raft core    api_batch_*       merge consecutive ClientWrites
③ Local disk   defer + double-buffer flusher (release state lock during sync)
④ Replication  max_payload_entries ≫ 300
```

Durability rules:

- With defer enabled, `append()` may return before sync so the openraft command loop is not blocked.  
- `IOFlushed` runs only after write (and sync for levels 1/2); client success still waits on it.  
- After sync=1 majority confirmation, a successful propose should not be lost on single-node power loss.  

Wall TPS ≈ \(N/L\) (in-flight / effective latency). Cost per entry ≈ \(t_{\mathrm{sync}}/E\). Pipelining raises \(N\); group-commit and replication batching raise \(E\).

Reproduce:

```bash
cargo build -p multiraft-demo --release
RUST_LOG=error ./target/release/multiraft-demo --mode bench --nodes 3 --groups 1 \
  --bench-file-log --bench-file-sync-level 1 \
  --bench-ops 48000 --bench-concurrency 256 --bench-batch-size 256 \
  --bench-file-coalesce-us 0 \
  --bench-max-payload-entries 8192
```

---

## vs Aeron Cluster commercial

- **multiraft:** embedded Rust / openraft, matching HA, graded local durability, reproducible hot-path numbers.  
- **Aeron Cluster commercial:** Media Driver, full Archive, ClusteredService, vendor support.  

[compare/aeron-commercial.md](../compare/aeron-commercial.md)

---

## Correctness

| Item | Role |
|------|------|
| Consistency Contract | propose / linearizable / stale grades |
| porcupine | Linearizability history checks (CI-friendly) |
| acceptance / chaos | End-to-end and failover scripts |
| Jepsen | Multi-process fault injection (optional) |

[jepsen.md](../jepsen.md) · [chaos-checklist.md](../chaos-checklist.md)

---

## Related docs

| Topic | Link |
|-------|------|
| Hot-path milestones | [hotpath spec](../specs/2026-07-21-aeron-inspired-hotpath-design.md) |
| sync=1 implementation & knees | [M4](../specs/2026-07-22-sync1-disk-pipeline-merge.md) |
| Bench numbers | [perf.md](../perf.md) |
| Demo bootstrap | [Getting Started](../wiki/en/Getting-Started.md) |
| Docs index | [docs/README.md](../README.md) |

## Out of scope

- No claim of Aeron kernel-bypass latency.  
- Commit decoupled from local disk is not described as sync=1.  
- No mega-entry throughput shortcuts; no silent openraft major-version bumps.  
