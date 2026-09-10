//! Raft protocol handlers for the in-process node dispatcher.
//!
//! Adapted from openraft `examples/multi-raft-kv/src/api.rs` at tag
//! `v0.10.0-alpha.30` (Raft paths only — app KV paths omitted).

use std::io::Cursor;

use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;

use crate::decode;
use crate::encode;
use multiraft_core::typ::*;
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;

pub async fn vote<S: StateMachine>(
    raft: &Raft<S>,
    req: &[u8],
) -> Result<Vec<u8>, crate::CodecError> {
    let res = raft.vote(decode(req)?).await;
    encode(res)
}

pub async fn append<S: StateMachine>(
    raft: &Raft<S>,
    req: &[u8],
) -> Result<Vec<u8>, crate::CodecError> {
    let res = raft.append_entries(decode(req)?).await;
    encode(res)
}

pub async fn snapshot<S: StateMachine>(
    raft: &Raft<S>,
    req: &[u8],
) -> Result<Vec<u8>, crate::CodecError> {
    let (vote, snapshot_meta, snapshot_data): (Vote, SnapshotMeta, Vec<u8>) = decode(req)?;
    let snapshot = Snapshot {
        meta: snapshot_meta,
        snapshot: Cursor::new(snapshot_data),
    };
    let res = raft
        .install_full_snapshot(vote, snapshot)
        .await
        .map_err(RaftError::<Infallible>::Fatal);
    encode(res)
}

pub async fn transfer_leader<S: StateMachine>(
    raft: &Raft<S>,
    req: &[u8],
) -> Result<Vec<u8>, crate::CodecError> {
    let transfer_req: TransferLeaderRequest<multiraft_core::TypeConfig> = decode(req)?;
    let res: Result<TransferLeaderResponse<multiraft_core::TypeConfig>, RaftError> = raft
        .handle_transfer_leader(transfer_req)
        .await
        .map_err(RaftError::Fatal);
    encode(res)
}
