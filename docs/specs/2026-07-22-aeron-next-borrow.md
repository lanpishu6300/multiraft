# Aeron next-borrow backlog (post M3 / Standby P3)

**中文：** [2026-07-22-aeron-next-borrow.zh-CN.md](./2026-07-22-aeron-next-borrow.zh-CN.md)

**Date:** 2026-07-22  
**Status:** Backlog (not scheduled)  
**Constraints:** openraft `=0.10.0-alpha.30`; **borrow ideas, not the Aeron runtime** — no Media Driver, SBE compiler, full Archive engine, or Consensus Module replacement.

**Already shipped (do not re-list as gaps):**

- Standby Premium parity P0–P3 — [aeron-standby-parity](./2026-07-20-aeron-standby-parity-design.md)
- Hot path M1–M3 — [aeron-inspired-hotpath](./2026-07-21-aeron-inspired-hotpath-design.md)
- Positioning vs commercial Aeron — [compare/aeron-commercial](../compare/aeron-commercial.md)

---

## Thesis

The next gains are **not** “ship another Aeron component.” They are three workstreams that raise real matching ceilings and operability while staying a thin Multi-Raft library:

```text
1. Compress FSM / matching work W          → real TPS ≈ 1 / (W + consensus)
2. Replication-path batch + stage metrics  → the half outside propose_batch
3. Ops-facing Archive semantics            → positions, export, recovery playbooks
```

Aeron Cluster documents the same ceiling math: with sequential RSM apply, throughput is dominated by **business logic time `W`** once transport is cheap. multiraft already optimized the openraft shell; unmatched `W` and opaque stages still hide the true limit.

---

## N1 — Compress FSM / matching `W` (P1)

### Aeron idea

Keep clustered service logic **deterministic, allocation-light, and free of blocking I/O** on the apply path. Encode commands cheaply (SBE-class fixed layout). Push validation to the edge so the RSM does not roll back mid-apply.

### multiraft mapping

| Item | Scope | Notes |
|------|--------|-------|
| **N1a** Downstream FSM guide | Docs + examples | “Apply must finish in µs”; no disk/network inside `apply`; idempotent keys |
| **N1b** Demo / CounterFsm budget | Optional microbench | `bench_ceiling` already has FSM-only (~9M); document matching FSM target bands |
| **N1c** Compact command codecs | Optional crate helper | Fixed-width or rkyv/flat layout for hot commands — **not** an SBE compiler |

### Success criteria

- Written contract: max recommended `W` for target wall TPS (e.g. `W ≤ 5µs` ⇒ soft logic cap ~200k before quorum).
- At least one matching-shaped FSM example that reports apply p50/p99 in the demo or a microbench.
- No change required to openraft pin.

### Non-goals

Replacing the matching engine; embedding a DB inside apply; LeaseRead as a substitute for fast apply.

---

## N2 — Replication-path batch + staged latency (P1)

### Aeron idea

Natural batching and pipelined acks on the **replication** path; metrics for each stage so operators see whether time is in network, disk, or apply.

### multiraft mapping

Propose-side pipeline (`propose_batch`, `conc×batch`) is largely done. Still thin:

| Item | Scope | Notes |
|------|--------|-------|
| **N2a** AppendEntries batching / in-flight knobs | `multiraft-net` + openraft config surface | **Done (2026-07-22):** expose `max_payload_entries`; knee pe≈8192 + `256×256` → ~150k; plateau pe≥16k → ~250k on 3-node sync=1. See [M4](./2026-07-22-sync1-disk-pipeline-merge.md) |
| **N2b** Staged latency histograms | Metrics API + demo | Stages: `enqueue → leader append → quorum RTT → commit → apply` (best-effort timestamps) |
| **N2c** Bench report enrichment | `multiraft-demo --mode bench` | Optional JSON fields for stage p50/p99; keep wall TPS as the headline |

### Success criteria

- One documented recipe where replication-side tuning moves wall TPS or p99 **without** only raising `conc×batch`.
- Bench or admin endpoint exposes at least three stage latencies.
- Chaos / linearizability tests still green.

### Non-goals

Custom consensus; Media Driver; claiming parity with Aeron Premium kernel-bypass numbers.

---

## N3 — Ops-facing Archive semantics (P2)

### Aeron idea

Archive: durable recording by position, replay, and operational tooling — not only “latest snapshot file.”

### multiraft mapping (approximate, directory + HTTP)

| Item | Scope | Notes |
|------|--------|-------|
| **N3a** Position model | Spec + API sketch | `(group, term, index)` / snapshot id as first-class ops handles |
| **N3b** Export tool | Admin or CLI | Export snapshot (+ optional log slice metadata) for a position range |
| **N3c** Recovery playbooks | Docs | Cold start / voter replace / standby promote sequences with curl + expected `RecoverOutcome` |
| **N3d** Hardening | Optional | Resume already partial via Range; catalog GC / retention policy |

Builds on existing SnapshotCatalog + HTTP Range + sha256 — deepen **ops semantics**, do not build a recording engine.

### Success criteria

- Bilingual ops doc: “recover to position X” with copy-paste commands.
- Export + install path covered by an automated test (can be demo/admin integration).
- Explicit statement: not Aeron Archive recording/replay.

### Non-goals

Full recording stream, time-travel query over all history, PremiumClusterTool clone, auth gateway (still out of lab Admin scope unless separately requested).

---

## Later / optional (P3 — only if product needs)

| Item | Borrow | Stay away from |
|------|--------|----------------|
| Cross-process low latency | Shared-memory or io_uring framing between matching and raft processes | Embedding Media Driver |
| Duty-cycle / affinity experiments | Fewer yields, sticky OS threads for hot workers | Claiming Aeron IPC latency |
| Client session lite | Idempotent propose + correlated acks for RMQ path B | Full ClusteredService session model |

---

## Priority order (recommended)

```text
N1a docs/contract  ─┐
N2b stage metrics  ─┼─► quick wins, informs where W vs quorum vs disk sits
N2a repl batch     ─┘
N1b/c FSM examples
N3a–c Archive ops semantics
P3 transport experiments only if cross-process SLO appears
```

---

## Out of scope (unchanged)

Media Driver · SBE schema compiler · full Aeron Archive · Consensus Module port · unattended cross-DC transition · “faster than Aeron” marketing claims.

---

## References

| Doc | Role |
|-----|------|
| [Aeron Cluster performance limits](https://aeron.io/docs/aeron-cluster/performance-limits/) | `W`-bound sequential RSM ceiling |
| [Efficient business logic](https://aeron.io/docs/aeron-cluster/efficient-business-logic/) | Encode / validate / no blocking apply |
| [hotpath design](./2026-07-21-aeron-inspired-hotpath-design.md) | M1–M3 already done |
| [standby parity](./2026-07-20-aeron-standby-parity-design.md) | HA semantics already done |
| [perf.md](../perf.md) | Measured wall ceilings & multi-group notes |
