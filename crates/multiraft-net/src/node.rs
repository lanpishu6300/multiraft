//! In-process Multi-Raft node: one shared receive channel, many Raft groups.
//!
//! Dispatches typed [`RaftCall`] without bincode on the hot path. A fixed worker
//! pool handles RPCs concurrently so pipelined AppendEntries are not serialized
//! on demux and we avoid unbounded `tokio::spawn` per message.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;

use futures::channel::mpsc;
use futures::SinkExt;
use futures::StreamExt;
use multiraft_core::typ::*;
use multiraft_core::GroupId;
use multiraft_core::NodeId;
use multiraft_fsm::StateMachine;
use multiraft_store::MemLogStore;
use multiraft_store::Raft;
use multiraft_store::StateMachineStore;
use openraft::error::Infallible;
use openraft::Config;

use crate::network::NetworkFactory;
use crate::router::NodeMessage;
use crate::router::NodeRx;
use crate::router::NodeTx;
use crate::router::RaftCall;
use crate::router::RaftReply;
use crate::router::Router;

pub type GroupMap<S> = Arc<Mutex<BTreeMap<GroupId, GroupApp<S>>>>;

/// Ingress buffer for pipelined AppendEntries from many groups/peers.
const NODE_CHANNEL: usize = 16384;
/// Fixed RPC worker count (concurrent handlers, not Tokio tasks per message).
const RPC_WORKERS: usize = 32;
/// Per-worker queue before backpressure propagates to ingress.
const RPC_WORKER_QUEUE: usize = 256;

pub struct Node<S: StateMachine> {
    pub node_id: NodeId,
    pub groups: GroupMap<S>,
    pub rx: NodeRx,
    pub router: Router,
}

impl<S: StateMachine> Node<S> {
    pub fn new(node_id: NodeId, router: Router) -> (Self, NodeTx) {
        let (tx, rx) = mpsc::channel(NODE_CHANNEL);
        router.register_node(node_id, tx.clone());

        let node = Self {
            node_id,
            groups: Arc::new(Mutex::new(BTreeMap::new())),
            rx,
            router,
        };
        (node, tx)
    }

    pub fn with_groups(node_id: NodeId, router: Router, groups: GroupMap<S>) -> (Self, NodeTx) {
        let (tx, rx) = mpsc::channel(NODE_CHANNEL);
        router.register_node(node_id, tx.clone());

        let node = Self {
            node_id,
            groups,
            rx,
            router,
        };
        (node, tx)
    }

    pub fn add_group(&self, group_id: GroupId, raft: Raft<S>, state_machine: StateMachineStore<S>) {
        let app = GroupApp {
            node_id: self.node_id,
            group_id,
            raft,
            state_machine,
        };
        self.groups.lock().unwrap().insert(group_id, app);
    }

    pub fn get_raft(&self, group_id: GroupId) -> Option<Raft<S>> {
        self.groups
            .lock()
            .unwrap()
            .get(&group_id)
            .map(|g| g.raft.clone())
    }

    pub async fn run(mut self) -> Option<()> {
        let groups = self.groups.clone();
        let mut workers: Vec<mpsc::Sender<NodeMessage>> = Vec::with_capacity(RPC_WORKERS);
        for _ in 0..RPC_WORKERS {
            let (tx, rx) = mpsc::channel(RPC_WORKER_QUEUE);
            let groups = groups.clone();
            tokio::spawn(async move {
                handle_worker(rx, groups).await;
            });
            workers.push(tx);
        }

        loop {
            let msg = self.rx.next().await?;
            let idx = msg.group_id as usize % RPC_WORKERS;
            let worker = &mut workers[idx];
            if let Err(e) = worker.try_send(msg) {
                if e.is_full() {
                    worker.send(e.into_inner()).await.ok()?;
                } else {
                    return None;
                }
            }
        }
    }
}

async fn handle_worker<S: StateMachine>(mut rx: mpsc::Receiver<NodeMessage>, groups: GroupMap<S>) {
    while let Some(msg) = rx.next().await {
        handle_node_message(groups.clone(), msg).await;
    }
}

async fn handle_node_message<S: StateMachine>(groups: GroupMap<S>, msg: NodeMessage) {
    let NodeMessage {
        group_id,
        call,
        response_tx,
    } = msg;

    let raft = {
        let g = groups.lock().unwrap();
        match g.get(&group_id) {
            Some(app) => app.raft.clone(),
            None => {
                let _ = response_tx.send(RaftReply::MissingGroup);
                return;
            }
        }
    };

    let reply = match call {
        RaftCall::Vote(req) => RaftReply::Vote(raft.vote(req).await),
        RaftCall::Append(req) => RaftReply::Append(raft.append_entries(req).await),
        RaftCall::Snapshot { vote, meta, data } => {
            let snapshot = Snapshot {
                meta,
                snapshot: Cursor::new(data),
            };
            let res = raft
                .install_full_snapshot(vote, snapshot)
                .await
                .map_err(RaftError::<Infallible>::Fatal);
            RaftReply::Snapshot(res)
        }
        RaftCall::Transfer(req) => {
            let res = raft
                .handle_transfer_leader(req)
                .await
                .map_err(RaftError::Fatal);
            RaftReply::Transfer(res)
        }
    };

    let _ = response_tx.send(reply);
}

pub struct GroupApp<S: StateMachine> {
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub raft: Raft<S>,
    pub state_machine: StateMachineStore<S>,
}

pub async fn create_node<S, F>(
    node_id: NodeId,
    group_ids: &[GroupId],
    router: Router,
    mut make_fsm: F,
) -> Node<S>
where
    S: StateMachine,
    F: FnMut(GroupId) -> S,
{
    let (node, _tx) = Node::new(node_id, router.clone());

    for &group_id in group_ids {
        let config = Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        };
        let config = Arc::new(config.validate().unwrap());
        let log_store = MemLogStore::default();
        let state_machine_store = StateMachineStore::new(group_id, make_fsm(group_id));
        let network = NetworkFactory::new(router.clone(), group_id);

        let raft = openraft::Raft::new(
            node_id,
            config,
            network,
            log_store,
            state_machine_store.clone(),
        )
        .await
        .unwrap();

        node.add_group(group_id, raft, state_machine_store);
    }

    node
}
