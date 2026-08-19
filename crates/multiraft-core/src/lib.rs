//! Shared types and OpenRaft TypeConfig for multiraft.
//!
//! Config / error types for the public MultiRaft API live here.
//! The `MultiRaft` runtime facade is implemented in `multiraft-net`
//! (`use multiraft_net::MultiRaft`) to avoid a core↔net dependency cycle.

mod config;
mod error;
mod multiraft;
mod snapshot;
mod type_config;

pub use config::ClusterConfig;
pub use config::FileLogSyncLevel;
pub use config::NodeRole;
pub use config::SnapshotMode;
pub use error::MultiRaftError;
pub use error::ProposeOk;
pub use error::StaleRead;
pub use snapshot::is_standby_snapshot_trigger;
pub use snapshot::RecoverOutcome;
pub use snapshot::SnapshotAdvertisement;
pub use snapshot::STANDBY_SNAPSHOT_TRIGGER;
pub use type_config::typ;
pub use type_config::GroupId;
pub use type_config::NodeId;
pub use type_config::Raft;
pub use type_config::Request;
pub use type_config::Response;
pub use type_config::SnapshotData;
pub use type_config::TypeConfig;
