# Deep pipeline & sync=1 group-commit / replication batching (M4)

**中文：** [2026-07-22-sync1-disk-pipeline-merge.zh-CN.md](./2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)

**Date:** 2026-07-22  
**Status:** Implemented and bench-calibrated  
**Depends on:** [Hot path M1–M3](./2026-07-21-aeron-inspired-hotpath-design.md)  
**Constraint:** openraft `=0.10.0-alpha.30`; one app payload = one Raft entry (no mega-entry).

**Overview:** [Highlights — hot path & sync=1](../spotlight/2026-07-hotpath-sync1.md). This doc has implementation detail and full knee tables.

---

## 1. Problem

M3b deep pipeline pushed **mem / file sync=0** wall TPS into the 100k+ range, while **file sync=1** looked stuck:

| Shape | ~TPS | Misread |
|-------|------|---------|
| Sequential single-entry | ~25 | “sync=1 can only do tens” |
| Deep pipeline + default repl batch | ~10–13k | “quorum fdatasync caps at ~10k” |

The real limits were often **shallow in-flight at some layer** or **tiny replication RPCs**, not “fdatasync forbids pipelining.”

This spec records the 2026-07-22 work: **keep sync=1 durability, fill client → storage → replication depth.**

---

## 2. Tuning philosophy

### 2.1 Little’s Law (wall throughput)

\[
\text{wall TPS} \approx \frac{\text{in-flight}}{\text{effective latency}}
\]

- Higher latency need not lower TPS if in-flight scales with it.  
- Idle waits hurt: long coalesce/hold without larger \(E\) (entries per sync) drops TPS.  
- Prefer **wall TPS**; amortized p50 when `batch>1` is not the same metric as sequential RTT.

### 2.1b What deep pipeline overlaps on the timeline

Sequential sync=1 is serial:

```text
propose₁ ──[append+fdatasync+repl]── Ok₁ ── propose₂ ── …     TPS ≈ 1 / L
```

Deep pipeline overlaps many independent `client_write` waits:

```text
propose₁ ──[  L₁ (ms quorum+disk)  ]── Ok₁
 propose₂ ──[  L₂  ]── Ok₂
  propose₃ ──[  L₃  ]── Ok₃
  … N in flight …
wall TPS ≈ N / L_avg, not 1 / L
```

Orthogonal to **group-commit / replication batching**, but both must be on: pipeline raises \(N\); merging raises \(E\) per durable flush (and thus cuts effective cost per entry). Deep \(N\) with pe=300 stalls around ~10k; fat pe with sequential client stays at ~tens of TPS.

### 2.2 Four pipeline layers (must align)

```text
① Client      conc × batch     propose_batch = try_join_all(client_write)
② Raft core   api_batch_*      merge ClientWrites → fatter storage AppendEntries
③ Local disk  FileLogStore     defer + background flusher + double-buffer
④ Replication max_payload_entries   max entries per network AppendEntries
```

| Shallow layer | Typical symptom (this machine) |
|---------------|--------------------------------|
| ① sequential propose only | sync=1 ~25 TPS |
| ③ await fdatasync inside `append` | outstanding IO ≈ 1 |
| ④ default `max_payload_entries=300` | 3-node sync=1 ~10k |
| ④ pe=8k–16k + ① 256×256 | 3-node sync=1 **~150k–250k** |

### 2.3 Amortize fdatasync; do not erase it

One `fdatasync` is ~4–6ms here. Aim for large \(E\):

\[
\text{cost per entry} \approx t_{\text{fdatasync}} / E
\]

Replication batch size dominates follower \(E\); local group-commit dominates leader \(E\).

### 2.4 Durability is not relaxed by pipelining

- `append()` may return before durable IO (keeps the openraft command loop moving).  
- **`IOFlushed` runs only after write[+sync]** — commit / client success still wait on that.  
- sync=1 + majority: successful proposes should survive node power loss (see §6).  
- sync=0 / mem: confirmed writes can still be lost.

### 2.5 Knob order

1. Fix durability: `--bench-file-sync-level 1`  
2. Raise `--bench-max-payload-entries` (try 4096, then sweep)  
3. Deepen `conc×batch` (sweet spot near `256×256`)  
4. Local coalesce: often `0` at high pe  
5. Large `ops` (≥30–50k) × several runs; use medians  

### 2.6 Anti-patterns

| Anti-pattern | Result |
|--------------|--------|
| Longer coalesce/hold without deeper in-flight / larger pe | Idle wait → TPS down, latency up |
| Await sync inside `append` while deferred | outstanding IO ≈ 1; group-commit dies |
| Leave pe=300 and call ~10k a hard fdatasync wall | Misdiagnosis |
| Over-subscribe pipeline (e.g. 512×512) | Queue contention; often worse than `256×256` |
| Use `propose_many` as the wall-TPS path | Shallower client pipeline than `propose_batch` |
| Treat amortized p50 as sequential RTT | Wrong metric |

---

## 3. Deep pipeline: design & implementation

### 3.1 Client (①)

`MultiRaft::propose_batch`: one entry per payload; `try_join_all` overlaps quorum waits; peak in-flight ≈ `concurrency × batch_size`.

`propose_many` (`client_write_many`): fatter Core message, shallower client pipeline — prefer `propose_batch` for wall TPS benches.

**Why mem 3-node is high without changing consensus:** replication still runs, but each hop is µs (RAM + in-process RPC). sync=1 adds ms-scale disk per hop unless \(E\) and in-flight are large.

### 3.2 Storage (③) — implementation notes

#### 3.2.1 Background flusher (no sleep on append)

Old path slept `coalesce_us` on the append task → deep pipeline capped at ~`1/coalesce`. Now coalesce/stream only **defer + wake the flusher**; append never sleeps.

#### 3.2.2 Never await sync inside `append` when deferred

openraft does **not** await `IOFlushed` for storage AppendEntries, but **does** await `append()` return. Awaiting `fdatasync` inside `append` on overlap caps outstanding IO ≈ 1 and kills group-commit.

**Correct:** when `defer_enabled`, enqueue pending, `ensure_flusher`, `notify_one`, return `Ok` immediately; flusher / `flush_pipeline` syncs and fires `IOFlushed`.

#### 3.2.3 Double-buffer (`flush_pipeline`)

1. Under state lock, `take` pending buf + callbacks  
2. **Release state lock**  
3. Under `io_gate`: `write` + `sync_*`  
4. Return writer, complete callbacks; loop to drain batches enqueued during sync  

Appends can keep enqueueing while fdatasync runs → larger \(E\).

#### 3.2.4 `hold_overlap` (timer-only group-commit)

Optional: never flush early on overlap; only timer / byte·callback caps. Useful to measure pure windows; idle-prone when Raft IO is shallow. Production high-pe sync=1 recipes usually **leave it off**. Hold mode ignores per-append notify (so clone/Drop cannot punch the window); use `notify_one` so a wake is not lost if the flusher is not yet parked.

| Knob | Role |
|------|------|
| `FileLogStreamOptions::hold_overlap` | Timer hold |
| `ClusterConfig::file_log_coalesce_us` | Group-commit window (µs) |
| `ClusterConfig::file_log_sync_level` | 0/1/2 |
| `flush_pipeline` / `io_gate` | Unlock-during-IO |

Code: `crates/multiraft-store/src/log_file.rs`.

### 3.3 Replication (④) — N2a

Default openraft **`max_payload_entries = 300`** starved follower group-commit. Surfacing `ClusterConfig::max_payload_entries` (`--bench-max-payload-entries`) was the main lever from ~10k → 100k+.

| Knob | Meaning | Default (0 = library) |
|------|---------|------------------------|
| `max_payload_entries` | Network replication batch | 300 |
| `max_append_entries` | Storage merge batch | 4096 |
| `api_batch_linger_ms` / `api_batch_capacity` | ClientWrite merge | 0 / 4096 |

---

## 4. Measured knees (ops=48k × 3, median)

### 4.1 `max_payload_entries` (`256×256`, coal=0, sync=1, 3-node)

| pe | median TPS | Shape |
|----|------------|-------|
| 2048 | ~51k | steep |
| 4096 | ~91k | steep |
| **8192** | **~145k** | **cost/benefit knee** |
| 12288 | ~186k | still rising |
| **16384–24576** | **~250k** | **plateau** |

### 4.2 Pipeline (pe=8192, coal=0)

| conc×batch | TPS | Note |
|------------|-----|------|
| 64×128 | ~67k | underfed |
| 128×256 | ~117k | |
| **256×256** | **~150k** | best |
| 256×512 / 512×* | ~112–124k | over-subscribed |

### 4.3 References

| Scenario | TPS |
|----------|-----|
| 3-node plateau pe≥16k | ~250k |
| 1-node same deep recipe | ~410k |
| Ratio | 3-node ≈ **~60%** of 1-node (quorum disk tax) |
| Local fdatasync batch=256 (no Raft) | ~44k entry/s |

### 4.4 Recipe

```bash
--bench-file-sync-level 1 \
--bench-max-payload-entries 8192 \
--bench-concurrency 256 --bench-batch-size 256 \
--bench-file-coalesce-us 0 --bench-ops 48000
# plateau probe: --bench-max-payload-entries 16384
```

---

## 5. Mem / sync=0 contrast (philosophy)

| | Mem 3-node | File sync=0 | File sync=1 (tuned) |
|--|------------|-------------|---------------------|
| Consensus/repl | yes | yes | yes |
| Cost per hop | µs | page-cache write | **fdatasync ~ms** |
| Deep pipeline | excellent (~300k+) | good (~150k–500k) | good (~150k–250k via large \(E\)) |
| Power loss after `Ok` | lost | may lose | **should not** if majority durable |

Not “consensus only for mem”; it is **hop cost × in-flight × \(E\)**. sync=1 needs both deep ① and fat ④.

---

## 6. Durability / message loss (summary)

| Mode | Confirmed `Ok` after power loss |
|------|----------------------------------|
| sync=1/2 + majority | Should **not** lose (success ⇒ majority `IOFlushed`) |
| sync=0 | **May** lose (page cache) |
| mem | **Lost** |
| Partial `propose_batch` failure | Prefix may be committed → **idempotent retry** |
| Not yet returned | Not committed; retry ≠ “library ate a confirmed message” |

Pipelining / group-commit / large pe do **not** change “durable only after `IOFlushed`.”

---

## 7. Index

| Area | Path |
|------|------|
| FileLogStore defer / flush_pipeline / hold | `crates/multiraft-store/src/log_file.rs` |
| `propose_batch` / `propose_many` | `crates/multiraft-net/src/multiraft.rs` |
| Knobs | `crates/multiraft-core/src/config.rs` |
| Demo flags | `multiraft-demo`: `--bench-max-payload-entries`, etc. |
| Numbers | [perf.md](../perf.md) |
| Hot path M1–M3 | [2026-07-21-…](./2026-07-21-aeron-inspired-hotpath-design.md) |
| N2a backlog tick | [2026-07-22-aeron-next-borrow.md](./2026-07-22-aeron-next-borrow.md) |

## 8. Out of scope

Mega-entry; bumping openraft; commit≠durable semantics; claiming Aeron kernel-bypass latency.

## 9. Verification

```bash
cargo test -p multiraft-store --test file_log_sync_coverage -- --test-threads=1
# Knee recipe: §4.4; chaos as usual
```
