//! Shared Multi-Raft networking: O(nodes) peer channels, not O(groups).
//!
//! In-process [`Router`] adapted from openraft `examples/multi-raft-kv` at tag
//! `v0.10.0-alpha.30`, implementing [`openraft_multi::GroupRouter`].
//!
//! Cross-process: [`GrpcRouter`] + tonic [`GrpcServer`] (bincode payloads).
//!
//! Public orchestration facade: [`MultiRaft`] (`use multiraft_net::MultiRaft`).

mod api;
mod conn_metrics;
mod grpc;
mod multiraft;
mod network;
mod node;
mod router;
mod snapshot_fetch;
mod standby_throttle;

pub use conn_metrics::ConnMetrics;
pub use grpc::GrpcRouter;
pub use grpc::GrpcServer;
pub use multiraft::MultiRaft;
pub use multiraft::SharedFabric;
pub use multiraft::wait_for_leader;
pub use network::GrpcNetworkFactory;
pub use network::NetworkFactory;
pub use node::GroupApp;
pub use node::Node;
pub use node::create_node;
pub use router::NodeMessage;
pub use router::NodeRx;
pub use router::NodeTx;
pub use router::Router;
pub use router::RouterError;
pub use snapshot_fetch::FetchedSnapshot;
pub use snapshot_fetch::pull_snapshot_chunked;
pub use standby_throttle::StandbyThrottle;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// bincode failure on the gRPC / wire codec (library paths never panic).
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("raft encode: {0}")]
    Encode(bincode::Error),
    #[error("raft decode: {0}")]
    Decode(bincode::Error),
}

/// Compact binary Raft RPC encoding (`bincode`).
pub fn encode<T: Serialize>(t: T) -> Result<Vec<u8>, CodecError> {
    bincode::serialize(&t).map_err(CodecError::Encode)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    bincode::deserialize(bytes).map_err(CodecError::Decode)
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let bytes = encode(42u32).expect("encode");
        assert_eq!(decode::<u32>(&bytes).expect("decode"), 42);
    }

    #[test]
    fn decode_garbage_returns_err() {
        let err = decode::<u32>(&[0xff, 0xff]).expect_err("garbage must fail");
        assert!(err.to_string().contains("decode"));
    }
}
