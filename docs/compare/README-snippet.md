## vs Aeron Cluster / Standby Premium

Paste into the repository README (see [full comparison](./compare/aeron-commercial.md)):

- **Not a fork of Aeron** — open-source Rust Multi-Raft on **openraft `=0.10.0-alpha.30`**, targeting matching-HA **semantic parity**, not Media Driver / commercial Cluster.
- **Standby Premium–like HA:** learner standby, throttled replication, async snapshot offload, HTTP recover, **promote/demote**, snapshot **daisy-chain**, **stale reads** (`read_stale`).
- **Aeron-inspired hot path:** typed in-process `RaftCall`/`RaftReply`, **`propose_batch` pipeline**, file **sync levels 0/1/2** (Aeron-aligned), stream buffer options.
- **Measured on this machine (3 voters, in-process):** mem **~300k+** wall TPS (conc=4×batch=8); file sync=0 **~117k–187k** with deep pipeline; sequential file **~2k**; sync=1 sequential **~25 TPS**, deep pipeline + fat pe **~150k–250k** — honest quorum/fsync walls vs amortized \(E\).
- **Choose multiraft** for embedded Rust/openraft matching HA; **choose Aeron commercial** for Media Driver, SBE, full Archive, ClusteredService, and Real Logic support.
