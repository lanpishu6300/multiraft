//! OpenRaft type configuration adapted from openraft `examples/multi-raft-kv`
//! at tag `v0.10.0-alpha.30` (`types-kv` + `declare_raft_types!`).
//!
//! Group IDs are `u64` (not string names like upstream `"users"`).

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

pub use multiraft_fsm::GroupId;
pub use multiraft_fsm::NodeId;

/// Snapshot byte stream type used by the memory SM bridge.
pub type SnapshotData = std::io::Cursor<Vec<u8>>;

/// Application write request: opaque bytes applied by [`multiraft_fsm::StateMachine`].
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub data: Vec<u8>,
}

impl Request {
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self { data: data.into() }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Request({} bytes)", self.data.len())
    }
}

/// Application response returned after a log entry is applied.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Response {
    pub effects: Vec<u8>,
}

impl Response {
    pub fn new(effects: impl Into<Vec<u8>>) -> Self {
        Self {
            effects: effects.into(),
        }
    }

    pub fn none() -> Self {
        Self::default()
    }
}

openraft::declare_raft_types!(
    /// Declare the type configuration for multiraft.
    pub TypeConfig:
        D = Request,
        R = Response,
);

/// Common Raft-related type aliases (from openraft `examples/utils/declare_types.rs`).
pub mod typ {
    pub use super::TypeConfig;

    pub type Vote = <TypeConfig as openraft::RaftTypeConfig>::Vote;
    pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;
    pub type LogId = openraft::alias::LogIdOf<TypeConfig>;
    pub type Entry = <TypeConfig as openraft::RaftTypeConfig>::Entry;
    pub type EntryPayload = openraft::alias::EntryPayloadOf<TypeConfig>;
    pub type Membership = openraft::membership::Membership<
        <TypeConfig as openraft::RaftTypeConfig>::NodeId,
        <TypeConfig as openraft::RaftTypeConfig>::Node,
    >;
    pub type StoredMembership = openraft::alias::StoredMembershipOf<TypeConfig>;

    pub type ApplyResponder = openraft::storage::ApplyResponder<TypeConfig>;
    pub type EntryResponder = openraft::storage::EntryResponder<TypeConfig>;

    pub type Node = <TypeConfig as openraft::RaftTypeConfig>::Node;

    pub type LogState = openraft::storage::LogState<TypeConfig>;

    pub type SnapshotMeta = openraft::alias::SnapshotMetaOf<TypeConfig>;
    pub type SnapshotData = super::SnapshotData;
    pub type Snapshot = openraft::alias::SnapshotOf<TypeConfig, SnapshotData>;

    pub type IOFlushed = openraft::storage::IOFlushed<TypeConfig>;

    pub type Infallible = openraft::errors::Infallible;
    pub type Fatal = openraft::errors::Fatal<TypeConfig>;
    pub type RaftError<E = openraft::errors::Infallible> =
        openraft::errors::RaftError<TypeConfig, E>;
    pub type RPCError<E = openraft::errors::Infallible> = openraft::errors::RPCError<TypeConfig, E>;

    pub type ErrorSubject = openraft::ErrorSubject<TypeConfig>;
    pub type StorageError = openraft::StorageError<TypeConfig>;
    pub type StreamingError = openraft::errors::StreamingError<TypeConfig>;

    pub type RaftMetrics = openraft::RaftMetrics<TypeConfig>;

    pub type ClientWriteError = openraft::errors::ClientWriteError<TypeConfig>;
    pub type LinearizableReadError = openraft::errors::LinearizableReadError<TypeConfig>;
    pub type ForwardToLeader = openraft::errors::ForwardToLeader<TypeConfig>;
    pub type InitializeError = openraft::errors::InitializeError<TypeConfig>;

    pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
    pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;
    pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
    pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
    pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
    pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<TypeConfig>;
    pub type SnapshotResponse = openraft::raft::SnapshotResponse<TypeConfig>;
    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
    pub type StreamAppendResult = openraft::raft::StreamAppendResult<TypeConfig>;
}

/// Raft handle parameterized by the openraft state-machine store type.
pub type Raft<SM> = openraft::Raft<TypeConfig, SM>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_new_display_and_serde() {
        let req = Request::new(b"abc");
        assert_eq!(req.data, b"abc");
        assert_eq!(format!("{req}"), "Request(3 bytes)");
        let s = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&s).unwrap();
        assert_eq!(back.data, b"abc");
    }

    #[test]
    fn response_new_none_and_serde() {
        let none = Response::none();
        assert!(none.effects.is_empty());
        let resp = Response::new(vec![1, 2]);
        assert_eq!(resp.effects, vec![1, 2]);
        let s = serde_json::to_string(&resp).unwrap();
        let back: Response = serde_json::from_str(&s).unwrap();
        assert_eq!(back.effects, vec![1, 2]);
    }
}
