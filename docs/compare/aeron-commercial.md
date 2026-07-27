# multiraft vs Aeron Cluster / Standby Premium

**中文：** [aeron-commercial.zh-CN.md](./aeron-commercial.zh-CN.md)

This document compares **multiraft** — an open-source, Rust Multi-Raft library for matching HA — with **Aeron Cluster** and **Aeron Cluster Standby (Premium)**, the commercial Real Logic stack.

---

## Positioning

**multiraft is not a fork of Aeron.** It does not embed the Aeron Media Driver, Consensus Module, or Archive. Instead, it targets **semantic parity** for matching-engine high availability: warm standby, snapshot offload, promote/demote, and an **Aeron-inspired hot path** — all on top of pinned **[openraft](https://github.com/databendlabs/openraft) `=0.10.0-alpha.30`** + `openraft-multi`.

| | multiraft | Aeron Cluster / Standby Premium |
|---|-----------|----------------------------------|
| License | Apache 2.0 | Commercial (Real Logic Premium) |
| Language | Rust | Java / C++ (Aeron stack) |
| Consensus | openraft Raft | Aeron Consensus Module |
| Transport | Typed in-process + optional gRPC | Media Driver + IPC / UDP |
| Primary goal | Matching HA in your Rust stack | General clustered services + Premium DR |

We borrow **ideas** from Aeron (standby throttle, snapshot daisy-chain, sync levels, pipelined propose) without shipping their runtime.

---

## What we match

Capabilities mapped from [Aeron Standby Premium parity](../specs/2026-07-20-aeron-standby-parity-design.md) and [Aeron-inspired hot path](../specs/2026-07-21-aeron-inspired-hotpath-design.md):

| Area | multiraft equivalent | Notes |
|------|---------------------|-------|
| **Standby offload** | openraft **Learner** via `add_standby`; async snapshot without stopping voters | `StandbyOffload` + `trigger_standby_snapshot` |
| **Non-blocking standby** | `standby_max_inflight`, `standby_replicate_delay_ms` throttle toward standby peers | Approximates “standby must not back-pressure the leader” |
| **Snapshot recovery** | `SnapshotAdvertisement`, `try_recover_from_standby_ads`, HTTP `fetch_url` pull | Chunked Range fetch + sha256 verify |
| **Promote / demote** | `promote_standby`, `demote_to_standby` via `change_membership` | Warm DR / TransitionModule analogue (operator-gated) |
| **Daisy snapshot chain** | `daisy_upstream_base`, `sync_from_daisy_upstream` | **Snapshot** daisy only — not full log redirect |
| **Stale reads** | `read_stale`, `enable_stale_queries` | Explicit watermark; not linearizable |
| **Typed in-process RPC** | `RaftCall` / `RaftReply` over `mpsc` — no bincode on same-process hops | gRPC path keeps bincode for cross-process |
| **`propose_batch` pipeline** | Each payload = its own Raft entry; sends pipelined via `join_all` | Wall TPS lever, not mega-entries |
| **File sync levels 0 / 1 / 2** | `FileLogSyncLevel::{Os, Data, All}` ↔ Aeron `file.sync.level` | See durability table below |
| **Stream options** | `FileLogStreamOptions`: `stream_buf_bytes`, `stream_flush_ms` | Os-level buffered append at sync=0 |

Standby is modeled as an openraft Learner, not a second consensus implementation. Archive semantics are approximated with **durable SnapshotCatalog + HTTP/gRPC fetch**, not a full Aeron Archive.

---

## What we deliberately do not ship

These are **out of scope by design**, not missing features we plan to “catch up” on inside multiraft:

| Aeron component | multiraft stance |
|-----------------|------------------|
| **Media Driver** | No shared-memory IPC across processes; in-process typed wire + optional gRPC |
| **SBE schema compiler** | bincode / typed Rust structs; no FIX/SBE codegen pipeline |
| **Full Aeron Archive** | Directory catalog + HTTP Range fetch; not recording/replay semantics |
| **Commercial Cluster runtime** | No ClusteredService container, session model, or PremiumClusterTool |
| **Replacing consensus** | Stays on openraft; no Aeron Consensus Module port |

If you need Media Driver latency across JVM/C++ services, SBE on the wire, or Real Logic support contracts, **Aeron commercial is the right product**. multiraft is for teams already on Rust/openraft who want matching-HA semantics without a second stack.

---

## Performance (measured ceilings)

All numbers below come from [perf.md](../perf.md) on **this machine**: SSD, **3 voters**, **in-process** bench (default harness), release build. They are **not** head-to-head benchmarks against Aeron — we do not claim to be faster than Aeron Cluster.

### Memory log (no disk)

| Scenario | ~TPS | p50 | Meaning |
|----------|------|-----|---------|
| Sequential single-entry (C) | ~17–18k | ~53–55µs | Quorum RTT soft ceiling |
| Pipeline `batch=8` (C′) | ~110k | ~8µs† | One proposer, overlapped quorum waits |
| **Conc=4 × batch=8 (C″)** | **~300k+** | ~10µs† | Parallel proposers × pipeline |

† Batch latency is **amortized** (`batch_wall / N`); not comparable to single-entry RTT.

### File log (local disk per replica)

| Scenario | ~TPS | p50 | Meaning |
|----------|------|-----|---------|
| Sequential, sync=0 | ~2.1–2.6k | ~360–425µs | Open `log.bin` + page-cache writes |
| **sync=0, conc×batch deep pipeline** | **~117k–187k** | ~30–37µs† | `conc=4×batch=16` ~117k; `conc=8×batch=16` ~187k |
| sync=1 sequential | ~25 | ~38ms | fsync per append × quorum |
| **sync=1 deep pipeline + pe≫300** | **~150k–250k** | amortized | pe≈8k/`256×256` ~150k; pe≥16k plateau ~250k; see [M4](../specs/2026-07-22-sync1-disk-pipeline-merge.md) |
| sync=2 sequential | ~23 | ~42ms | full fsync per append × quorum |

### Hard walls (cannot be patched away in current design)

1. **Sequential single-entry mem** stays near ~18–25k: one quorum RTT per op.
2. **Sequential file sync=0** stays near ~2k: disk write × quorum per op.
3. **`sync_level ≥ 1` sequential** collapses to ~25 TPS on this hardware: fsync × quorum every append. Deep pipeline + large `max_payload_entries` can raise sync=1 wall TPS to **~150k–250k** (amortize \(E\); do not erase fsync).
4. openraft per-`client_write` waits for commit+apply; leader clones entries for AppendEntries.

**How we cross 100k wall TPS:** `propose_batch` + concurrency — overlapping many independent Raft writes, not one mega-entry. File sync=0 / sync=1 at 100k+ both need deep pipeline; sync=1 also needs a fat replication batch (see [M4](../specs/2026-07-22-sync1-disk-pipeline-merge.md)).

Cross-process gRPC is a separate harness; default bench numbers are in-process.

---

## Durability grades (`file_log_sync_level`)

Aligned with Aeron Archive/Cluster **`file.sync.level`** (see [hotpath M2b](../specs/2026-07-21-aeron-inspired-hotpath-design.md#m2b--file-log-sync-level-local-durability-grade)):

| Level | Name | Mechanism | multiraft config | Typical TPS impact (3-node file, this machine) |
|------:|------|-----------|------------------|------------------------------------------------|
| **0** | OS / page cache | `write` only; kernel may delay flush | `FileLogSyncLevel::Os` (default) | Sequential ~2k; deep pipeline sync=0 ~117k–187k wall |
| **1** | Data sync | `fdatasync` / `File::sync_data` | `FileLogSyncLevel::Data` | Sequential ~25 TPS; deep pipeline + pe≫300 ~150k–250k wall |
| **2** | Full sync | `fsync` data+metadata / `File::sync_all` | `FileLogSyncLevel::All` | ~23 TPS sequential |

Orthogonal axes (same as industry practice):

- **Memory log** (`data_dir` empty) — nothing on local disk; stronger than “level 0 file”.
- **Replication quorum** — commit waits for peers; does not imply local fsync.
- **Group commit** (`file_log_coalesce_us`) — batch writes, then apply sync level once.

Demo: `--bench-file-sync-level 0|1|2`.

---

## When to choose multiraft vs Aeron commercial

### Choose multiraft when

- Your matching / trading stack is **Rust** and you already want **openraft** Multi-Raft.
- You need **Standby Premium–like HA** (learner standby, snapshot offload, promote, daisy snapshot chain, stale reads) **without** a JVM/C++ Aeron dependency.
- You accept **HTTP snapshot fetch** instead of full Archive, and **operator-gated** promote/demote.
- You want **Apache 2.0** source, chaos/Jepsen hooks, and a thin library you embed — not a clustered-service container.
- Throughput goals fit **pipelined propose** (100k+ mem wall; 100k+ file at sync=0 with deep pipeline) or you can tolerate **~2k sequential file** / **~25 TPS with fsync**.

### Choose Aeron Cluster / Standby Premium when

- You need **Media Driver** IPC, UDP multicast, or cross-language Aeron clients on the wire.
- You require **SBE**-compiled schemas, full **Archive** recording/replay, or Real Logic **commercial support**.
- You run **ClusteredService** session semantics, PremiumClusterTool, or existing Aeron ops playbooks.
- You want Real Logic’s **Consensus Module** and product roadmap, not openraft.

### Neutral statement on speed

multiraft optimizes the **openraft hot path** (typed in-process RPC, pipelined batch, coalesced file append). Aeron optimizes a **different stack** (Media Driver, zero-copy IPC). **Neither doc claims multiraft is faster than Aeron** — compare only after identical workloads, hardware, and durability grade.

---

## Further reading

| Doc | Topic |
|-----|-------|
| [perf.md](../perf.md) · [perf.zh-CN.md](../perf.zh-CN.md) | Measured ceilings, bench commands, C″ pipeline |
| [Aeron Standby parity](../specs/2026-07-20-aeron-standby-parity-design.md) | Premium capability matrix P0–P3 |
| [Aeron-inspired hot path](../specs/2026-07-21-aeron-inspired-hotpath-design.md) | M1 typed RPC, M2 file coalesce/sync, M3 stream |
| [Aeron next-borrow backlog](../specs/2026-07-22-aeron-next-borrow.md) | N1 FSM `W`, N2 repl metrics, N3 Archive ops |
| [Standby async snapshot](../specs/2026-07-20-standby-async-snapshot-design.md) | MVP snapshot mechanics |
| [ARCHITECTURE.md](../ARCHITECTURE.md) | Crate boundaries and contracts |

---

## Summary

multiraft offers **open-source, Rust-native matching HA** with **semantic alignment** to Aeron Standby Premium (standby, snapshots, promote, daisy, stale reads) and **mechanical-sympathy patterns** from Aeron Cluster (typed hot path, pipelined propose, graded durability) — **on openraft**, without Media Driver or commercial Cluster. Pick multiraft for embedded Rust Multi-Raft; pick Aeron commercial for the full Real Logic platform.
