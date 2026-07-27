# FAQ

**中文：** [zh/FAQ.md](../zh/FAQ.md)

### What config for single-symbol sync=1 matching?

See **[single-symbol recommended config & benches](../../perf-single-symbol.md)** (pe 8k–12k, `192×256`, etc.). Implementation detail: [M4](../../specs/2026-07-22-sync1-disk-pipeline-merge.md).

### Does more groups raise total TPS?

Usually **no** on file (especially sync=1). Aggregate wall TPS falls as group count rises — see **[multi-group benches](../../perf-multi-group.md)**. Multi-group is for isolation / many symbols.

### Where is the hot path / sync=1 summary?

See **[highlights: hot path & sync=1](../../spotlight/2026-07-hotpath-sync1.md)**. Implementation detail: [M4](../../specs/2026-07-22-sync1-disk-pipeline-merge.md).

### Why not SofaJRaft / TiKV raftstore?

No official Rust SofaJRaft. TiKV `raftstore` is thick (Region / PD / split);
matching shards on stable symbols do not need that. This library is thin
Multi-Raft (shared links + many groups).

### Is a timed-out `propose` a definite failure?

No. Timeout / disconnect / failover windows are **indeterminate** — retry with
the same idempotency key. See the Consistency Contract.

### Can `with_fsm` be used as source of truth?

No. It may be stale. Use `read_linearizable` for production reads.

### Where are Jepsen reports?

After a run: `jepsen/multiraft/store/latest/` (gitignored). Case source lives
under `jepsen/multiraft/src/`. More: [Consistency & testing](./Consistency.md).

### Downstream integration (phase 2)?

Phase-1 is this independent runtime. Phase-2 (optional, in a downstream app):
a matching process / ingress shell can depend on this crate for Leader-side
propose, with a pluggable matching engine FSM.
