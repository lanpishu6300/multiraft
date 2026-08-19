//! Cluster configuration for MultiRaft.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::NodeId;

/// Local Raft role for this process/node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NodeRole {
    #[default]
    Voter,
    /// openraft Learner; never becomes Leader. Used for async snapshot offload.
    Standby,
}

/// How snapshots are produced on this cluster.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SnapshotMode {
    /// Legacy: voters may sync-dump FSM in `build_snapshot`.
    #[default]
    Disabled,
    /// Voters never hot-dump FSM; Standby builds durable snapshots asynchronously.
    StandbyOffload,
}

/// Local file-log durability after each durable write (aligned with Aeron Archive/Cluster
/// `file.sync.level`, PostgreSQL `fdatasync`/`fsync`, and common WAL sync grades).
///
/// Orthogonal to replication quorum and to [`ClusterConfig::file_log_coalesce_us`]
/// (group commit): coalesce batches writes, then this level decides how hard they hit disk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum FileLogSyncLevel {
    /// `write` only — data may sit in the OS page cache (Aeron sync level **0**).
    /// Fastest; crash/power loss can lose the tail of the log on that node.
    #[default]
    Os = 0,
    /// `File::sync_data` / fdatasync — file data forced to stable storage (Aeron **1**).
    Data = 1,
    /// `File::sync_all` / fsync — data + metadata (Aeron **2**). Strongest, slowest.
    All = 2,
}

impl FileLogSyncLevel {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Os),
            1 => Some(Self::Data),
            2 => Some(Self::All),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Static cluster membership and Raft timing knobs.
///
/// Phase-1 in-process networking ignores `peers` socket addresses (channel
/// Router linking). Empty `data_dir` → memory log; non-empty → file store
/// under `{data_dir}/group-{id}/`.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub node_id: NodeId,
    pub peers: Vec<(NodeId, SocketAddr)>,
    /// When empty, MultiRaft uses an in-memory log. When set, uses a file-backed
    /// log at `{data_dir}/group-{id}/`.
    pub data_dir: PathBuf,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    /// Local role (voter vs standby learner).
    pub role: NodeRole,
    /// Snapshot policy; see [`SnapshotMode`].
    pub snapshot_mode: SnapshotMode,
    /// How many durable catalog snapshots to retain per group (StandbyOffload).
    pub snapshot_keep: usize,
    /// Admin HTTP bind / advertise address for snapshot fetch URLs (demo / recovery).
    pub admin_advertise_addr: Option<SocketAddr>,
    /// Max outstanding AppendEntries toward Standby node ids (soft).
    pub standby_max_inflight: u32,
    /// Artificial delay before sending RPC to standby peers (ms).
    pub standby_replicate_delay_ms: u64,
    /// Node ids treated as standby for throttling (seed; also updated at runtime).
    pub standby_node_ids: Vec<NodeId>,
    /// Chunk size for HTTP Range snapshot downloads (bytes). Default 64 KiB.
    pub snapshot_fetch_chunk_bytes: usize,
    /// Background daisy sync interval when [`Self::daisy_upstream_base`] is set (ms).
    pub daisy_sync_interval_ms: u64,
    /// If set on a Standby, pull snapshots from this upstream Standby's base URL
    /// (e.g. `"http://127.0.0.1:23103"`) instead of only following the leader.
    /// Path used: `{base}/snapshots/{group}/latest`.
    pub daisy_upstream_base: Option<String>,
    /// Allow local stale FSM queries on this node (Standby service offload).
    /// `for_test` defaults to `false`; enable for Standby (or any analytics replica).
    pub enable_stale_queries: bool,
    /// File log group-commit window in microseconds (`0` = flush each append immediately).
    /// Only used when [`Self::data_dir`] is set. Hotpath file benches typically use `100`.
    pub file_log_coalesce_us: u64,
    /// Local disk sync strength for file-backed logs; ignored when `data_dir` is empty.
    /// Default [`FileLogSyncLevel::Os`] (page-cache writes, Aeron level 0).
    pub file_log_sync_level: FileLogSyncLevel,
    /// Minimum pending `log.bin` bytes before a stream flush at [`FileLogSyncLevel::Os`]
    /// (`0` = no size threshold). Ignored for Data/All sync.
    pub file_log_stream_buf_bytes: usize,
    /// Max milliseconds to hold pending log bytes before a background Os-level flush
    /// (`0` = no time-based defer). Ignored for Data/All sync.
    pub file_log_stream_flush_ms: u64,
    /// Timed group-commit: do **not** flush early on overlapping appends; only the
    /// coalesce timer (and size/count caps) flush. Raises sync=1 wall TPS at the
    /// cost of higher propose latency (use with [`Self::file_log_coalesce_us`] 5–50ms).
    pub file_log_hold_overlap: bool,
    /// openraft `api_batch_linger_ms`: wait this long to merge ClientWrites (`0` = no linger).
    pub api_batch_linger_ms: u64,
    /// openraft `api_batch_capacity` (`0` = library default 4096).
    pub api_batch_capacity: u64,
    /// openraft `max_append_entries` when merging storage AppendEntries (`0` = default 4096).
    pub max_append_entries: u64,
    /// openraft `max_payload_entries`: max log entries per **network** AppendEntries RPC
    /// (`0` = library default 300). Raise (e.g. 4096) for sync=1 replication group-commit.
    pub max_payload_entries: u64,
    /// Background transition policy poll interval (`0` = disabled).
    pub transition_poll_interval_ms: u64,
}

impl ClusterConfig {
    /// Sensible defaults for local / in-process tests (memory log).
    pub fn for_test(node_id: NodeId, peer_ids: &[NodeId]) -> Self {
        let peers = peer_ids
            .iter()
            .map(|&id| {
                (
                    id,
                    format!("127.0.0.1:{}", 19000 + id)
                        .parse()
                        .expect("static test addr"),
                )
            })
            .collect();
        Self {
            node_id,
            peers,
            data_dir: PathBuf::new(),
            heartbeat_interval_ms: 100,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
            role: NodeRole::Voter,
            snapshot_mode: SnapshotMode::Disabled,
            snapshot_keep: 2,
            admin_advertise_addr: None,
            standby_max_inflight: 8,
            standby_replicate_delay_ms: 0,
            standby_node_ids: Vec::new(),
            snapshot_fetch_chunk_bytes: 65_536,
            daisy_sync_interval_ms: 2_000,
            daisy_upstream_base: None,
            enable_stale_queries: false,
            file_log_coalesce_us: 0,
            file_log_sync_level: FileLogSyncLevel::Os,
            file_log_stream_buf_bytes: 0,
            file_log_stream_flush_ms: 0,
            file_log_hold_overlap: false,
            api_batch_linger_ms: 0,
            api_batch_capacity: 0,
            max_append_entries: 0,
            max_payload_entries: 0,
            transition_poll_interval_ms: 0,
        }
    }

    /// Enable local stale queries (typically called after setting [`Self::role`]
    /// to [`NodeRole::Standby`]).
    pub fn with_stale_queries(mut self, enabled: bool) -> Self {
        self.enable_stale_queries = enabled;
        self
    }
}

#[cfg(test)]
mod sync_level_tests {
    use super::ClusterConfig;
    use super::FileLogSyncLevel;
    use super::NodeRole;

    #[test]
    fn from_u8_roundtrip_and_reject() {
        assert_eq!(FileLogSyncLevel::from_u8(0), Some(FileLogSyncLevel::Os));
        assert_eq!(FileLogSyncLevel::from_u8(1), Some(FileLogSyncLevel::Data));
        assert_eq!(FileLogSyncLevel::from_u8(2), Some(FileLogSyncLevel::All));
        assert_eq!(FileLogSyncLevel::from_u8(3), None);
        assert_eq!(FileLogSyncLevel::from_u8(255), None);

        assert_eq!(FileLogSyncLevel::Os.as_u8(), 0);
        assert_eq!(FileLogSyncLevel::Data.as_u8(), 1);
        assert_eq!(FileLogSyncLevel::All.as_u8(), 2);
    }

    #[test]
    fn default_is_os() {
        assert_eq!(FileLogSyncLevel::default(), FileLogSyncLevel::Os);
    }

    #[test]
    fn debug_and_eq() {
        assert_eq!(format!("{:?}", FileLogSyncLevel::Data), "Data");
        assert_ne!(FileLogSyncLevel::Os, FileLogSyncLevel::All);
    }

    #[test]
    fn for_test_defaults_and_stale_queries() {
        let c = ClusterConfig::for_test(1, &[1, 2, 3]);
        assert_eq!(c.file_log_sync_level, FileLogSyncLevel::Os);
        assert_eq!(c.file_log_coalesce_us, 0);
        assert_eq!(c.role, NodeRole::Voter);
        let c = c.with_stale_queries(true);
        assert!(c.enable_stale_queries);
    }
}
