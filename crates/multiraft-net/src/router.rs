//! In-process Multi-Raft router with per-node connection sharing.
//!
//! Hot path uses typed [`RaftCall`] / [`RaftReply`] (no bincode) — Aeron-inspired
//! same-process IPC. Cross-process gRPC still serializes in [`crate::GrpcRouter`].

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::SinkExt;
use openraft::error::Unreachable;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

use crate::conn_metrics::ConnMetrics;
use crate::standby_throttle::StandbyThrottle;
use multiraft_core::typ;
use multiraft_core::typ::RaftError;
use multiraft_core::GroupId;
use multiraft_core::NodeId;
use multiraft_core::TypeConfig;

pub type NodeTx = mpsc::Sender<NodeMessage>;
pub type NodeRx = mpsc::Receiver<NodeMessage>;

#[derive(Debug)]
pub struct RouterError(pub String);

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RouterError {}

/// Typed Raft RPC body for in-process hops (no serialize).
pub enum RaftCall {
    Vote(VoteRequest<TypeConfig>),
    Append(AppendEntriesRequest<TypeConfig>),
    Snapshot {
        vote: typ::Vote,
        meta: typ::SnapshotMeta,
        data: Vec<u8>,
    },
    Transfer(TransferLeaderRequest<TypeConfig>),
}

/// Typed Raft RPC reply for in-process hops.
pub enum RaftReply {
    Vote(Result<VoteResponse<TypeConfig>, RaftError>),
    Append(Result<AppendEntriesResponse<TypeConfig>, RaftError>),
    Snapshot(Result<SnapshotResponse<TypeConfig>, RaftError>),
    Transfer(Result<TransferLeaderResponse<TypeConfig>, RaftError>),
    /// Target group missing on the node.
    MissingGroup,
}

/// Message sent through a node connection; `group_id` selects the Raft group.
pub struct NodeMessage {
    pub group_id: GroupId,
    pub call: RaftCall,
    pub response_tx: oneshot::Sender<RaftReply>,
}

/// Multi-Raft router: one channel per node, shared by all groups on that node.
#[derive(Clone, Default)]
pub struct Router {
    pub nodes: Arc<Mutex<BTreeMap<NodeId, NodeTx>>>,
    metrics: ConnMetrics,
    throttle: StandbyThrottle,
}

impl Router {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(BTreeMap::new())),
            metrics: ConnMetrics::new(),
            throttle: StandbyThrottle::default(),
        }
    }

    pub fn throttle(&self) -> &StandbyThrottle {
        &self.throttle
    }

    pub fn register_node(&self, node_id: NodeId, tx: NodeTx) {
        {
            let mut nodes = self.nodes.lock().unwrap();
            nodes.insert(node_id, tx);
        }
        self.metrics.record_peer(node_id);
    }

    pub fn unregister_node(&self, node_id: NodeId) -> Option<NodeTx> {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.remove(&node_id)
    }

    pub fn unique_peer_links(&self) -> usize {
        self.metrics.unique_peer_links()
    }

    async fn send_call(
        &self,
        to_node: NodeId,
        to_group: GroupId,
        call: RaftCall,
    ) -> Result<RaftReply, Unreachable<TypeConfig>> {
        let _standby_permit = self.throttle.before_send(to_node).await;
        let (resp_tx, resp_rx) = oneshot::channel();

        let mut tx = {
            let nodes = self.nodes.lock().unwrap();
            nodes
                .get(&to_node)
                .ok_or_else(|| {
                    Unreachable::new(&RouterError(format!("node {} not connected", to_node)))
                })?
                .clone()
        };

        let msg = NodeMessage {
            group_id: to_group,
            call,
            response_tx: resp_tx,
        };
        if let Err(e) = tx.try_send(msg) {
            if e.is_full() {
                tx.send(e.into_inner())
                    .await
                    .map_err(|e| Unreachable::new(&RouterError(e.to_string())))?;
            } else {
                return Err(Unreachable::new(&RouterError("node channel closed".into())));
            }
        }

        resp_rx
            .await
            .map_err(|e| Unreachable::new(&RouterError(e.to_string())))
    }

    pub async fn send_vote(
        &self,
        to_node: NodeId,
        to_group: GroupId,
        rpc: VoteRequest<TypeConfig>,
    ) -> Result<VoteResponse<TypeConfig>, Unreachable<TypeConfig>> {
        match self
            .send_call(to_node, to_group, RaftCall::Vote(rpc))
            .await?
        {
            RaftReply::Vote(Ok(r)) => Ok(r),
            RaftReply::Vote(Err(e)) => Err(Unreachable::new(&RouterError(e.to_string()))),
            RaftReply::MissingGroup => Err(Unreachable::new(&RouterError("missing group".into()))),
            _ => Err(Unreachable::new(&RouterError("reply type mismatch".into()))),
        }
    }

    pub async fn send_append(
        &self,
        to_node: NodeId,
        to_group: GroupId,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<TypeConfig>, Unreachable<TypeConfig>> {
        match self
            .send_call(to_node, to_group, RaftCall::Append(rpc))
            .await?
        {
            RaftReply::Append(Ok(r)) => Ok(r),
            RaftReply::Append(Err(e)) => Err(Unreachable::new(&RouterError(e.to_string()))),
            RaftReply::MissingGroup => Err(Unreachable::new(&RouterError("missing group".into()))),
            _ => Err(Unreachable::new(&RouterError("reply type mismatch".into()))),
        }
    }

    pub async fn send_snapshot(
        &self,
        to_node: NodeId,
        to_group: GroupId,
        vote: typ::Vote,
        meta: typ::SnapshotMeta,
        data: Vec<u8>,
    ) -> Result<SnapshotResponse<TypeConfig>, Unreachable<TypeConfig>> {
        match self
            .send_call(to_node, to_group, RaftCall::Snapshot { vote, meta, data })
            .await?
        {
            RaftReply::Snapshot(Ok(r)) => Ok(r),
            RaftReply::Snapshot(Err(e)) => Err(Unreachable::new(&RouterError(e.to_string()))),
            RaftReply::MissingGroup => Err(Unreachable::new(&RouterError("missing group".into()))),
            _ => Err(Unreachable::new(&RouterError("reply type mismatch".into()))),
        }
    }

    pub async fn send_transfer(
        &self,
        to_node: NodeId,
        to_group: GroupId,
        rpc: TransferLeaderRequest<TypeConfig>,
    ) -> Result<TransferLeaderResponse<TypeConfig>, Unreachable<TypeConfig>> {
        match self
            .send_call(to_node, to_group, RaftCall::Transfer(rpc))
            .await?
        {
            RaftReply::Transfer(Ok(r)) => Ok(r),
            RaftReply::Transfer(Err(e)) => Err(Unreachable::new(&RouterError(e.to_string()))),
            RaftReply::MissingGroup => Err(Unreachable::new(&RouterError("missing group".into()))),
            _ => Err(Unreachable::new(&RouterError("reply type mismatch".into()))),
        }
    }

    pub fn has_node(&self, node_id: NodeId) -> bool {
        let nodes = self.nodes.lock().unwrap();
        nodes.contains_key(&node_id)
    }
}
