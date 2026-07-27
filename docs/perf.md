# Performance notes

**中文：** [perf.zh-CN.md](./perf.zh-CN.md)

## Theoretical ceilings (local release phase breakdown)

| Phase | TPS | p50 | Meaning |
|-------|-----|-----|---------|
| A FSM-only | ~9M | &lt;1µs | Soft ceiling without consensus |
| B 1-node mem | ~55–57k | ~12–14µs | **Raft soft ceiling without replication** |
| C 3-node mem (seq) | ~17–18k | ~53–55µs | Typed in-process wire + single `client_write` |
| C′ 3-node mem (batch=8) | ~110k | ~8µs† | Pipelined multi-entry `propose_batch` |
| C″ 3-node mem (conc=4, batch=8) | ~300k+ | ~10µs† | Parallel proposers × pipeline |
| D codec | ~1.4M | &lt;1µs | bincode no longer dominant |
| File 3-node (seq, sync=0) | ~2.1–2.6k | ~360–425µs | Open `log.bin` + page-cache writes |
| File (seq, sync=1 `sync_data`) | ~25 | ~38ms | Data fsync per append × quorum |
| File (seq, sync=2 `sync_all`) | ~23 | ~42ms | Full fsync per append × quorum |
| File (batch=16, coalesce=50µs, sync=0) | ~4–5k | ~180–195µs† | Group-commit window + pipeline |
| File (conc=4, batch=8, sync=0) | ~60–66k | ~53–63µs† | Deep pipeline: 4×8 in-flight |
| File (conc=4×batch=16, sync=0) | **~117k** | ~30µs† | **Crosses 100k** |
| File (conc=8×batch=16, sync=0) | **~187k** | ~37µs† | Higher in-flight (~128) |
| File (conc=4, sync=0, batch=1) | ~10k | ~360µs | Parallel proposers only |

† Per-entry share of batch wall time (not single-entry RTT). See [Concurrency × pipeline](#concurrency--pipeline-conc4--batch8) below.

**Wall clock:** real elapsed time from bench start to finish (not CPU time).  
`TPS = successful entries / wall seconds`. Pipelined / multi-group runs can show low amortized p50; **compare throughput with wall TPS**.

**Takeaways:**

- Sequential single-entry 3-node mem stays near the soft cap (~18–25k): quorum + 2 hops.
- **Pipeline (`propose_batch`) and concurrency** are how wall TPS crosses 100k without replacing openraft.
- File sequential gains from keeping the log file open; group-commit helps when appends overlap (batch / conc).
- **File sync=0 wall ≥100k achieved:** `--bench-concurrency 8 --bench-batch-size 16` (~187k); `4×16` ~117k.

## Theoretical ceiling (current design)

Holding **openraft + no Media Driver/SHM**, on this machine (SSD, in-process, 3 voters):

```text
TPS_seq ≈ 1 / (t_leader_local + t_quorum_rtt)
         ≈ 1 / (t_1node + 2 × t_inproc_hop + t_follower_append)
```

| Term | ~value | Meaning |
|------|--------|---------|
| `t_1node` | ~18µs (~55k TPS) | No replication: local append + apply |
| `t_inproc_hop` | ~15–20µs | Router→Node demux→`append_entries`→oneshot |
| `t_quorum_rtt` | ~2 hops | Wait for one follower |
| **Ideal sequential 3-node** | **~18–25k** | Matches measured C (~17k) |
| **Pipelined wall TPS** | **≪ 1/`t_seq`** | Overlapping `client_write` quorum waits |
| **File sync=0** | × disk write per replica | ~2k; sync=1/2 × fsync (~25 TPS) |

**Still outside this design (not removable by small patches):**

1. openraft per-`client_write` waits for commit+apply.
2. Leader **clones** log entries for AppendEntries (`RaftLogReader`).
3. File `sync_level≥1` is fsync × quorum even on SSD.

**R6 code bottlenecks removed:** concurrent Node demux; StandbyThrottle empty-set fast path; router hot-path debug removed; `hard_state` commit debounce; coalesce `Notify`; encode straight into `pending_buf`.

## Concurrency × pipeline (`conc=4` × `batch=8`)

This is the high-throughput mem scenario reported as **C″**. Design intent (see also [hotpath design](./specs/2026-07-21-aeron-inspired-hotpath-design.md) M1-B): raise **wall-clock TPS** by overlapping many independent Raft writes, without collapsing them into one mega-entry.

### Knobs

| Flag | Role |
|------|------|
| `--bench-concurrency 4` | Spawn **4** Tokio tasks; each is an independent proposer loop against the current leader. |
| `--bench-batch-size 8` | Each loop iteration calls `MultiRaft::propose_batch` with **8** payloads. |

Defaults are `concurrency=1`, `batch_size=1` (pure sequential single-entry). Combining both multiplies outstanding work.

### What is *not* batched

`propose_batch` does **not** pack 8 payloads into one Raft log entry. Each payload becomes its **own** entry via a separate openraft `client_write`. Consensus, apply, and FSM idempotency stay per-entry (same as eight concurrent clients).

### Pipeline inside one batch

```text
propose_batch(group, [p0..p7])
        │
        ├─► client_write(p0) ──► quorum / apply ──► ProposeOk
        ├─► client_write(p1) ──► quorum / apply ──► ProposeOk
        ├─► ...
        └─► client_write(p7) ──► quorum / apply ──► ProposeOk
              ▲
              └── started together via futures::join_all (pipelined, not serial)
```

While entry *i* waits on AppendEntries / commit, entry *i+1* can already be in flight on the leader. That overlaps quorum RTTs that sequential `propose` would pay one-after-another.

### Four proposers on top

```text
time ──────────────────────────────────────────────────────────►

proposer-0:  [batch 8 entries ════╗][batch 8 ════╗]...
proposer-1:  [batch 8 entries ════╣][batch 8 ════╣]...
proposer-2:  [batch 8 entries ════╣][batch 8 ════╣]...
proposer-3:  [batch 8 entries ════╝][batch 8 ════╝]...
                      ▲
                      └── up to ~4×8 = 32 client_writes outstanding
                          against the same group leader (mem, in-process)
```

- **Pipeline** (`batch`) reduces idle time *within* one client.
- **Concurrency** (`conc`) adds independent clients so the leader/replication path stays busy when one batch is waiting on the last of its eight commits.

Peak in-flight work is about `concurrency × batch_size` (here 32), not `concurrency + batch_size`.

### Latency reporting (important)

For `batch_size > 1`, the harness records **wall time of the whole `propose_batch`**, then attributes `wall_us / N` to each of the N entries for p50/p95/p99.

- That number is an **amortized per-entry share**, not the RTT of a lone `propose`.
- Sequential single-entry p50 (~54µs) and batch-amortized p50 (~10µs) are therefore **not comparable** as “same metric, faster API”.
- TPS uses `ok_entries / wall_seconds` and *is* the right apples-to-apples throughput figure.

### API semantics (production)

```rust
// Ok only if every entry succeeds. On NotLeader / hard error → Err;
// some entries may already be committed — retry with the same idempotency keys.
MultiRaft::propose_batch(group, payloads) -> Result<Vec<ProposeOk>, MultiRaftError>
```

### How to run C″

```bash
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 8000 \
  --bench-concurrency 4 --bench-batch-size 8
```

Related points on the same ladder: `batch=8` alone (one proposer, pipeline only) ≈ C′; `conc=4` with `batch=1` (four single-entry proposers, no pipeline) is the older concurrency-only shape (~65–68k before / around R5).

## Harness

```bash
cargo build -p multiraft-demo --release

# M1 sequential
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 5000

# M1 pipelined multi-entry (one proposer)
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 5000 --bench-batch-size 8

# M1 concurrency × pipeline (C″)
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 8000 \
  --bench-concurrency 4 --bench-batch-size 8

# M2 file (+ optional coalesce with batch)
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 2000 \
  --bench-file-log --data-dir /tmp/multiraft-bench

RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 2000 \
  --bench-file-log --data-dir /tmp/multiraft-bench \
  --bench-batch-size 16 --bench-file-coalesce-us 50

# Local durability grades (Aeron-aligned 0/1/2)
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 1500 \
  --bench-file-log --data-dir /tmp/multiraft-bench \
  --bench-file-sync-level 1

# File sync=0 wall-clock ≥100k (deep pipeline)
RUST_LOG=error ./target/release/multiraft-demo \
  --mode bench --nodes 3 --groups 1 --bench-ops 8000 \
  --bench-file-log --data-dir /tmp/multiraft-bench \
  --bench-concurrency 8 --bench-batch-size 16

cargo test -p multiraft-net --test bench_ceiling --release -- --nocapture
cargo test -p multiraft-store --test bench_file_log_micro --release -- --nocapture
cargo test -p multiraft-net --test bench_codec_micro --release -- --nocapture

# Sync-level / FileLogStore coverage
cargo test -p multiraft-store --test file_log_sync_coverage --release
cargo llvm-cov -p multiraft-core --lib -p multiraft-store \
  --test file_log_sync_coverage --test file_log_roundtrip --summary-only
```

## Bottlenecks addressed

| Round | Hotspot | Change | Effect |
|-------|---------|--------|--------|
| R1 | Full `log.json` rewrite | NDJSON append | micro 4×+ |
| R2 | wipe/restart log reversion | `allow_log_reversion` | stable chaos |
| R3 | hot-path log noise | trace / byte sizes | lower overhead |
| R4 | NDJSON JSON + JSON RPC | `log.bin` + bincode RPC/commands | file **897→2130 TPS**; 3-node mem **14k→16.5k** |
| R5 | bincode on in-process hops; single-entry propose; open/close log | typed `RaftCall`/`RaftReply`; `propose_batch`; open file + coalesce | mem batch **~110k**; file seq **~2.6k**; file conc=4 **~10k** |
| R6 | serial Node demux; voter throttle lock; hard_state per commit; coalesce idle sleep | concurrent demux; standby count fast path; commit debounce; Notify wake | seq mem still ~17k (quorum bound); batch/conc hold **~100k / ~300k+** |
| R7 | bench `payloads.clone()`; in-process 500ms network backoff | move-once bench path; `backoff: None`; `try_join_all` | file conc×batch **~60–66k** toward 100k target |

## Remaining bottlenecks

1. **Sequential single-entry mem:** still bound by per-op quorum RTT (~17–18k). Use `propose_batch` / concurrency for throughput.
2. **File sequential:** still disk × quorum; coalesce only helps overlapping appends. Local durability is graded by `file_log_sync_level` (`0` page cache / `1` sync_data / `2` sync_all); default `0` matches prior behavior — raising it trades TPS for crash safety (see [hotpath M2b](./specs/2026-07-21-aeron-inspired-hotpath-design.md#m2b--file-log-sync-level-local-durability-grade)).
3. **Cross-process gRPC:** separate harness (default bench is in-process).
4. **Architectural residue:** log entry clone + openraft scheduling — needs API/engine change to lift sequential single-entry further.

## Multi-group wall ceiling (this machine)

Harness: in-process 3 voters; with `concurrency>1` each proposer is **sticky** to `group_id = worker % groups` (avoids aliasing onto group 0 when `batch % groups == 0`).

| Scenario | groups | conc×batch | ~wall TPS | Notes |
|----------|-------:|------------|----------:|-------|
| mem baseline | 1 | 4×8 | ~345k | Doc C″ |
| mem push single group | 1 | 32×16 | **~886k** | Peak single-leader pipeline |
| mem multi-group | 8 | 8×8 | ~389k | Slightly above 4×8 single |
| mem multi-group | 16 | 32×16 | ~561k | Best multi-group recipe here |
| mem multi-group | 32 | 64×8 | ~290k | Over-subscribed, drops |
| file sync=0 | 1 | 8×16 | **~158k–184k** | Prior headline; not a disk hard cap |
| file sync=0 | 1 | **32×16 / 64×16 / 32×32** | **~390k–494k** | Deeper in-flight; page cache still not saturated |
| file sync=0 | 1 node | 32×16 | **~830k** | No quorum — local append is much faster |
| file sync=0 | 8 | 8×16 | ~47k | Many `log.bin` contend for disk |
| file sync=0 | 16 | 16×16 | ~46k | Same |
| file sync=1 pe=12288 | 1 | 192×256 | **~195k** | Median of 3 (2026-07-27) |
| file sync=1 pe=12288 | 4 | 192×256 | ~143k | ~0.73× of groups=1 |
| file sync=1 pe=12288 | 8 | 192×256 | ~79k | ~0.40× |
| file sync=1 pe=12288 | 16 | 192×256 | ~7k | Collapse — do not pack this densely |

Full multi-group tables and guidance: [perf-multi-group.md](./perf-multi-group.md).

**Why file sync=0 is not “millions like page cache”:** raw buffered writes on this machine are ~5M+/s; Raft still pays per-entry openraft + **3-replica** append + quorum RTT + `FileLogStore` mutex. Prefer higher `conc×batch` for wall TPS.

**Coalesce tuning (2026-07-22):** old path slept `coalesce_us` on the append task → deep pipeline ~`1/50µs ≈ 20k` TPS. Now coalesce/stream **defer to a background flusher** (no sleep on append). After fix, `8×16` + `coalesce=50` ≈ **~152k** (was ~29k); still slightly below `coalesce=0` (~184k).

**Timed group-commit / `hold_overlap` (2026-07-22, sync=1):**
`--bench-file-hold-overlap` + `coalesce_us` 5–50ms holds across overlapping appends until the timer (or size/count caps).

| Window | Pipeline | ~TPS | p50 |
|--------|----------|------|-----|
| no hold, coal=5ms | 64×128 | **~11k** | ~3ms |
| hold 5ms | 64×128 | ~10k | ~3ms |
| hold 10ms | 64×128 | ~9k | ~3–4ms |
| hold 20ms | 64×128 | ~8k | ~4ms |
| hold 50ms | 64×128 / 128×256 | **~4–4.5k** | ~7–15ms |

**Ceiling (hold / pe=300 only):** stretching coalesce/hold to 20–50ms **does not raise** throughput — with a starved replication batch the window idles. The real jump is larger `max_payload_entries` + deep pipeline (see below / [M4](./specs/2026-07-22-sync1-disk-pipeline-merge.md)), not a longer empty window. Hold recipe is a contrast only:

```bash
cargo run -p multiraft-demo --release -- --mode bench --nodes 3 --groups 1 \
  --bench-file-log --bench-file-sync-level 1 \
  --bench-ops 8000 --bench-concurrency 32 --bench-batch-size 64 \
  --bench-file-hold-overlap --bench-file-coalesce-us 50000 \
  --bench-file-stream-buf 67108864
```

**Disk pipeline + merge (2026-07-22, stop B = ≥2× ≈20–25k on 3-node; N2a later far exceeds):**

Shipped:
1. **Double-buffer flush** — state mutex released during `write`+`fdatasync`; appends queue the next batch.
2. **Non-blocking `append` when coalesce/stream defer is on** — never await sync on the openraft command path (that was capping outstanding IO at ~1).
3. **`api_batch_linger_ms` / capacity** on `ClusterConfig` + demo flags; **`propose_many`** (`client_write_many`) for fat single-thread submits. Benches keep **`propose_batch`** (`try_join_all`) for client pipeline depth.

| Scenario | ~wall TPS | Notes |
|----------|-----------|-------|
| 3-node sync=1 `64×128` coal=5ms, **payload=default(300)** | **~10–13k** | Pre-N2a; replication RPC capped at 300 entries |
| 3-node sync=1 `128×256` coal=1ms, **payload=4096** | **~100–110k** | N2a working point |
| 3-node sync=1 `256×256` coal=0, **payload=8192** | **~145k** (median) | **cost/benefit knee** |
| 3-node sync=1 `256×256`, **payload=16k–24k** | **~250k** (median plateau) | larger pe → no gain |
| 1-node sync=1 pe=12k 256×256 | **~410k** | local ceiling reference |
| Local `fdatasync` batch=256 | **~44k entry/s** | Disk micro ceiling (no Raft) |

**Knee (ops=48k × 3 runs, median):** raise `max_payload_entries` until **~8k–16k**; pipeline sweet spot **`256×256`** (deeper over-subscribes). Plateau ~250k on 3-node ≈ 60% of 1-node (~410k).

**N2a recipe (3-node sync=1):**

```bash
cargo run -p multiraft-demo --release -- --mode bench --nodes 3 --groups 1 \
  --bench-file-log --bench-file-sync-level 1 \
  --bench-ops 48000 --bench-concurrency 256 --bench-batch-size 256 \
  --bench-file-coalesce-us 0 \
  --bench-max-payload-entries 8192
# plateau probe: --bench-max-payload-entries 16384
```

Full design / philosophy: [M4 spec](./specs/2026-07-22-sync1-disk-pipeline-merge.md).  
Single-symbol recommended config and 2026-07-27 knees: [perf-single-symbol.md](./perf-single-symbol.md).  
Multi-group (mem / sync=0 / sync=1): [perf-multi-group.md](./perf-multi-group.md).

Default openraft `max_payload_entries` is **300** — that was the hidden replication-batch ceiling. Raising it lets followers apply fatter AppendEntries → fewer `fdatasync`s per entry (group-commit on the replication path).

**Takeaway:** multi-group does **not** automatically raise wall TPS — single-group deep pipeline often wins on mem; on file, more groups can **hurt** total TPS due to disk contention. Multi-group’s value is isolation / scale-out shape, not a free throughput multiplier.
