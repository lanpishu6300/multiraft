## 与 Aeron Cluster / Standby Premium 对比

可粘贴进仓库 README（完整版见 [aeron-commercial.zh-CN.md](./aeron-commercial.zh-CN.md)）：

- **不是 Aeron 的 fork** — 开源 Rust Multi-Raft，基于 **openraft `=0.10.0-alpha.30`**，追求撮合 HA **语义对齐**，而非 Media Driver / 商业 Cluster。
- **类 Standby Premium HA：** learner standby、复制节流、异步快照卸载、HTTP 恢复、**promote/demote**、快照 **daisy 链**、**stale 读**（`read_stale`）。
- **Aeron 启发式热路径：** 类型化进程内 `RaftCall`/`RaftReply`、**`propose_batch` 流水线**、文件 **sync 档位 0/1/2**（与 Aeron 对齐）、stream 缓冲选项。
- **本机实测（3 voter、进程内）：** mem 墙钟 **~300k+**（conc=4×batch=8）；file sync=0 深流水线 **~117k–187k**；顺序 file **~2k**；sync=1 顺序 **~25 TPS**，深流水线 + 大 pe **~15–25万** — 如实呈现 quorum/fsync 上限与摊销 \(E\)。
- **选 multiraft** 做嵌入式 Rust/openraft 撮合 HA；**选 Aeron 商业版** 要 Media Driver、SBE、完整 Archive、ClusteredService 与 Real Logic 支持。
