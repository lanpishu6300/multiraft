//! Public errors and propose results for MultiRaft.

use thiserror::Error;

use crate::NodeId;

/// Errors returned by MultiRaft propose / group APIs.
#[derive(Debug, Error)]
pub enum MultiRaftError {
    #[error("not leader; hint={hint:?}")]
    NotLeader { hint: Option<NodeId> },

    #[error("unknown group {0}")]
    UnknownGroup(u64),

    #[error("stale queries disabled (set ClusterConfig::enable_stale_queries)")]
    StaleQueriesDisabled,

    /// Returned when asking for the in-process Router on a gRPC-backed node.
    #[error("in-process router is only available for MultiRaft::start / start_cluster")]
    InProcessRouterUnavailable,

    #[error("{0}")]
    InvalidArgument(String),

    #[error("{0}")]
    Operation(String),
}

impl MultiRaftError {
    pub fn invalid_arg(msg: impl Into<String>) -> Self {
        Self::InvalidArgument(msg.into())
    }

    pub fn operation(msg: impl Into<String>) -> Self {
        Self::Operation(msg.into())
    }
}

/// Successful propose: log index and term after quorum commit + apply
/// (linearizable write for that group).
#[derive(Debug, Clone)]
pub struct ProposeOk {
    pub index: u64,
    pub term: u64,
}

/// Result of a local FSM read for Standby service offload.
///
/// Never linearizable: `applied_index` is this node's last applied log only.
#[derive(Debug, Clone)]
pub struct StaleRead<T> {
    pub value: T,
    pub applied_index: u64,
    pub applied_term: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_and_stale_read() {
        let e = MultiRaftError::NotLeader { hint: Some(2) };
        assert!(e.to_string().contains("not leader"));
        assert!(MultiRaftError::UnknownGroup(7).to_string().contains("7"));
        assert!(MultiRaftError::StaleQueriesDisabled
            .to_string()
            .contains("stale"));
        let arg = MultiRaftError::invalid_arg("at least one member");
        assert!(arg.to_string().contains("at least one member"));
        matches!(arg, MultiRaftError::InvalidArgument(_));
        let op = MultiRaftError::operation("boom");
        assert!(op.to_string().contains("boom"));
        let backend = MultiRaftError::InProcessRouterUnavailable;
        assert!(backend.to_string().contains("in-process"));
        let _ = format!("{:?}", ProposeOk { index: 1, term: 1 });
        let sr = StaleRead {
            value: 42u64,
            applied_index: 3,
            applied_term: 1,
        };
        assert_eq!(sr.value, 42);
        let _ = format!("{sr:?}");
    }
}
