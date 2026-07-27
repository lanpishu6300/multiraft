# Aeron-inspired hot path (throughput / latency)

**中文：** [2026-07-21-aeron-inspired-hotpath-design.zh-CN.md](./2026-07-21-aeron-inspired-hotpath-design.zh-CN.md)

**Date:** 2026-07-21  
**Branch:** `feature/aeron-inspired-hotpath`  
**Status:** Implemented (M1–M3 + **M4**); verified on `feature/aeron-inspired-hotpath`  
**Constraints:** openraft `=0.10.0-alpha.30`; no Media Driver / full Aeron stack.

**Overview:** [Highlights — hot path & sync=1](../spotlight/2026-07-hotpath-sync1.md)

## Design philosophy

We optimize the **openraft Multi-Raft hot path** with Aeron-style *mechanical sympathy* — not by forking Aeron or replacing consensus.

| Principle | Meaning in multiraft |
|-----------|----------------------|
| **Same-process is free** | Typed `RaftCall`/`RaftReply` over `mpsc`; no bincode on in-process hops. gRPC keeps serialization for cross-process. |
| **Pipeline, don’t mega-batch** | Each app payload stays its own Raft entry; wall TPS comes from overlapping many `client_write`s (`propose_batch` + concurrency). Fat single-thread submits use `propose_many` / `client_write_many`. |
| **Deep pipeline fills latency holes** | When \(L\) is milliseconds (sync=1), sequential TPS is \(1/L\). Raise in-flight \(N\) so \(\mathrm{TPS}\approx N/L\). Mem has small \(L\) so the same \(N\) looks “naturally” high; sync=1 needs deeper \(N\) *and* large amortizing \(E\). |
| **Amortize, don’t erase, fsync** | sync=1 still pays fdatasync; group-commit / replication batching raise entries per sync so cost per entry is \(t_{\mathrm{sync}}/E\). |
| **Grade durability explicitly** | `FileLogSyncLevel` 0/1/2 mirrors Aeron `file.sync.level`. Quorum commit ≠ local fsync. |
| **Coalesce then sync** | Group-commit + double-buffer flush; never await sync inside openraft `append` when deferred. |
| **Replication \(E\) is first-class** | openraft default `max_payload_entries=300` starves follower group-commit; raise it for sync=1 deep pipeline. |
| **Cut scheduler noise** | Router `try_send`, sticky workers, standby-throttle atomic fast path. |
| **Honest ceilings** | Sequential sync≥1 ≈ fsync×quorum (~25 TPS). 100k+ on sync=1 needs deep in-flight + pe≫300 — not a single-entry miracle. |

**Borrow ideas, not the runtime:** standby throttle, snapshot offload, sync grades, pipelined propose, Archive-like local durability grades. **Do not ship:** Media Driver, SBE, full Archive, Consensus Module. Positioning: [compare/aeron-commercial.md](../compare/aeron-commercial.md).

Milestone stack (build upward):

```text
M4  sync=1 deep pipeline / group-commit / max_payload_entries
M3c less scheduling     → duty-cycle (try_send, workers)
M3b deep pipeline       → wall TPS (conc × batch)
M3a stream flush        → Os sync=0 buffered append
M2 / M2b file coalesce + sync levels
M1-B propose_batch      → pipelined client_write
M1-A typed in-process   → zero encode on same-process RPC
```

## Goals

| Milestone | Scenario | Target vs baseline (`54b1148`) |
|-----------|----------|--------------------------------|
| **M1** | 3-node in-process mem, sequential | TPS ≥ 35k, p50 ≤ 30µs (was ~16.5k / ~58µs) |
| **M1** | same, concurrency=4 | TPS ≥ 100k (was ~65k) |
| **M2** | 3-node file | TPS ≥ 6k, p50 ≤ 200µs (was ~2.1k / ~420µs) |

Inspired by Aeron Cluster (mechanical sympathy): **no encode on same-process hops**, **pipeline many consensus ops**, **coalesce durable appends** — without replacing consensus.

## M1-A — Typed in-process wire

`Router` / `Node` use a typed `RaftCall` / `RaftReply` over `mpsc` (no `bincode` on the hot path).

`GrpcRouter` keeps bincode payloads (cross-process).

## M1-B — Pipelined `propose_batch`

```text
MultiRaft::propose_batch(group, Vec<Vec<u8>>) -> Result<Vec<ProposeOk>, MultiRaftError>
```

- Each app payload is its **own Raft entry** (`client_write` per item).
- Sends are **pipelined** (`join_all` / concurrent tasks), not one giant entry.
- **Final semantics:** pipeline all; return `Ok(Vec<ProposeOk>)` only if **all** succeed; on any `NotLeader` / hard error return `Err` (some entries may already be committed — caller must use idempotency keys).

Demo: `--bench-batch-size N` issues batches of N pipelined proposes.

### Bench shape: `conc=4` × `batch=8`

High-throughput mem ladder point (perf doc **C″**). Full write-up: [perf.md — Concurrency × pipeline](../perf.md#concurrency--pipeline-conc4--batch8).

| Layer | Mechanism | Effect |
|-------|-----------|--------|
| **batch=8** | One `propose_batch` starts 8 `client_write`s together | Overlap quorum RTT *inside* one client |
| **conc=4** | Four Tokio proposer loops | Keep the leader busy across clients |
| **Combined** | Peak ~`4×8` in-flight writes | Wall TPS, not single-entry RTT |

Latency in the harness for `batch>1` is **amortized** (`batch_wall / N`); do not compare to sequential p50 as the same metric.

## M3b — Deep pipeline (file + mem wall TPS)

**Goal:** keep more `client_write` / replication in flight without mega-entries — peak in-flight ≈ `concurrency × batch_size`.

| Change | Where | Effect |
|--------|-------|--------|
| `try_join_all` per payload | `propose_batch` | N concurrent `client_write`s overlap quorum wait (no per-entry task spawn) |
| No bench `payloads.clone()` | demo `propose_batch_bench` | Hot path moves batch once |
| In-process `backoff: None` | `network.rs` | Avoid 500ms sleep stalling pipelined AppendEntries retry |

### Mem (sync=0 equivalent)

Already crosses **100k** with one proposer × batch (`batch=16` ≈ **~177k** on this machine) or `conc=4 × batch=8` ≈ **~344k**.

### File `sync=0` — wall TPS (achieved on this machine)

| Recipe | ~TPS (this machine) | In-flight |
|--------|---------------------|-----------|
| Sequential single-entry | ~2.1–2.6k | 1 |
| `batch=16`, `coalesce=50µs` | ~5k | 16 |
| `conc=4`, `batch=8` | ~60–66k | ~32 |
| **`conc=4`, `batch=16`** | **~117k** | ~64 |
| **`conc=8`, `batch=16`** | **~187k** | ~128 |

Crossing **100k file sync=0** needs deeper overlap than sequential disk×quorum allows:

```text
TPS_wall ≈ in_flight / t_commit
100k     ≈ 64 / t  →  t ≈ 640µs   (achieved with conc×batch)
100k     ≈ 1  / t  →  t ≈ 10µs    (sequential — not achievable on file)
```

Still one Raft entry per payload; **sequential** `sync≥1` remains fsync×quorum (~25 TPS).  
After **M4** (deep pipeline + fat replication batches), 3-node sync=1 reaches **~150k–250k** — see the dedicated spec. Stream defer (M3a) alone does not beat deep pipeline for wall TPS.

```bash
# File deep pipeline ≥100k (verify)
RUST_LOG=error ./target/release/multiraft-demo --mode bench --nodes 3 --groups 1 \
  --bench-ops 8000 --bench-file-log --data-dir /tmp/multiraft-bench \
  --bench-concurrency 4 --bench-batch-size 16
```

## M4 — sync=1 deep pipeline / group-commit / replication batching

Align client in-flight, local disk pipeline, and `max_payload_entries` so sync=1 wall TPS rises without relaxing durability.

Full design, implementation, knees, and recipes: **[2026-07-22-sync1-disk-pipeline-merge.md](./2026-07-22-sync1-disk-pipeline-merge.md)**.

| Lever | Effect |
|-------|--------|
| `propose_batch` + `conc×batch` | Client in-flight |
| defer + double-buffer flusher | Local \(E\); no await sync on `append` |
| `max_payload_entries` (300 → 8k/16k) | Replication \(E\) (main jump) |
| Working knee | pe≈8192, pipeline≈256×256 → ~150k; plateau pe≥16k → ~250k |

## M2 — File log group commit

`FileLogStore` coalesces durable appends:

- Config: `ClusterConfig::file_log_coalesce_us` (0 = flush immediately). At Os level, non-zero coalesce uses the **same background flusher** as stream options — append tasks do **not** sleep (avoids deep-pipeline `1/coalesce_us` tax). Overlapping appends (`n_callbacks > 1`) flush immediately.
- Multiple `append` calls within the window share one `write_all` to `log.bin`.
- `truncate` / `purge` / drop force flush first.

## M2b — File log sync level (local durability grade)

Industry practice separates **where** data lives from **how hard** each write hits stable storage. Common local grades (same numbers as Aeron Archive/Cluster `file.sync.level`):

| Level | Name | Mechanism | Peers in industry |
|------:|------|-----------|-------------------|
| **0** | OS / page cache | `write` only; kernel may delay flush | Aeron `0`; many throughput defaults |
| **1** | Data sync | `fdatasync` / `File::sync_data` | Aeron `1`; PostgreSQL `fdatasync` |
| **2** | Full sync | `fsync` data+metadata / `File::sync_all` | Aeron `2`; strict WAL |

Orthogonal axes (not replaced by sync level):

- **Memory log** (`data_dir` empty) — no local disk (stronger than “level 0 file”: nothing on disk at all).
- **Replication quorum** — commit waits for peers; does not imply local fsync.
- **Group commit** (`file_log_coalesce_us`) — batch writes, then apply sync level once.

Config: `ClusterConfig::file_log_sync_level` (`FileLogSyncLevel::{Os,Data,All}`), default **Os**. Applied after `log.bin` appends, rewrites, and `hard_state.json` atomic writes. Demo: `--bench-file-sync-level 0|1|2`.

## M3a — File log stream flush (Os level 0)

Aeron Archive-style **sequential streaming** at sync level 0:

- Long-lived append handle in a large `BufWriter` (default 64 KiB; scales with `stream_buf_bytes`).
- Length-prefixed frames accumulate in `pending_buf`; one `write_all` + `BufWriter::flush` hits page cache, then [`IOFlushed`] callbacks run.
- Optional Os-only defer: `ClusterConfig::file_log_stream_buf_bytes` and/or `file_log_stream_flush_ms` (background `Notify` + timer). `Data`/`All` always sync-flush; truncate/purge/rewrite force flush first.
- API: `FileLogStore::open_with_full_options(..., FileLogStreamOptions { .. })`; defaults (`stream_* = 0`) keep immediate flush.

## M3c — Less scheduling (in-process duty cycle)

Stay in-process; reduce Tokio / lock noise on the typed Raft RPC hop:

| Layer | Change | Effect |
|-------|--------|--------|
| **Router** | `try_send` before `send().await` | Skip scheduler yield when node channel has capacity |
| **Node demux** | Fixed worker pool (round-robin), not `spawn` per message | Bounded concurrent handlers; AppendEntries still parallel |
| **Channels** | Larger ingress + per-worker queues | Pipeline AppendEntries without early backpressure |
| **StandbyThrottle** | `standby_count == 0` atomic fast path | Voter-only clusters pay no mutex on send |

No cached `NodeTx` across `unregister_node` — lookup stays under the router mutex each call.

## Out of scope

Media Driver, shared-memory IPC across processes, replacing openraft, SBE schema compiler.

## Next (backlog)

M4 (fill sync=1) is done. Next: compress FSM `W`, staged latency (N2b), ops Archive semantics — see [2026-07-22-aeron-next-borrow.md](./2026-07-22-aeron-next-borrow.md).

## Verification

- `bench_ceiling`, `multiraft-demo --mode bench`, file bench with coalesce / deep pipeline.
- `chaos_standby` + `chaos_failover` still pass.
- Update `docs/perf.md` / `perf.zh-CN.md`.
- Hot-path unit coverage (critical crates):

```bash
cargo llvm-cov -p multiraft-core --lib --summary-only
cargo llvm-cov -p multiraft-store --lib --tests \
  --test file_log_sync_coverage --test file_log_roundtrip --test restart_recover \
  --ignore-filename-regex 'tests/|sm_bridge|snapshot|mem_log' --summary-only
```

Target: `config.rs` / sync+stream paths in `log_file.rs` at **~100% lines**; whole workspace is not claimed at 100%.
