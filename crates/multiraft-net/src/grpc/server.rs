//! Inbound tonic server: demux by `group_id` to local Raft handlers.

use std::net::SocketAddr;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::transport::Server;

use crate::api;
use crate::encode;
use crate::grpc::proto::RaftRequest;
use crate::grpc::proto::RaftResponse;
use crate::grpc::proto::raft_service_server::RaftService;
use crate::grpc::proto::raft_service_server::RaftServiceServer;
use crate::node::GroupMap;
use multiraft_core::typ;
use multiraft_fsm::StateMachine;

/// Serves [`RaftService`] and dispatches to the same handlers as in-process [`crate::node::Node`].
pub struct GrpcServer;

impl GrpcServer {
    /// Bind `addr` and serve until the accept loop ends.
    pub async fn serve<S: StateMachine + 'static>(
        addr: SocketAddr,
        groups: GroupMap<S>,
    ) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Self::serve_with_listener(listener, groups).await
    }

    /// Serve on an already-bound listener (so callers can fail-fast on bind).
    pub async fn serve_with_listener<S: StateMachine + 'static>(
        listener: tokio::net::TcpListener,
        groups: GroupMap<S>,
    ) -> anyhow::Result<()> {
        let incoming = TcpListenerStream::new(listener);
        let svc = RaftServiceServer::new(RaftServiceImpl { groups });
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await?;
        Ok(())
    }
}

pub(crate) struct RaftServiceImpl<S: StateMachine> {
    pub(crate) groups: GroupMap<S>,
}

#[tonic::async_trait]
impl<S: StateMachine + 'static> RaftService for RaftServiceImpl<S> {
    async fn call(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        demux_raft_call(&self.groups, request.into_inner()).await
    }
}

pub(crate) async fn demux_raft_call<S: StateMachine>(
    groups: &GroupMap<S>,
    req: RaftRequest,
) -> Result<Response<RaftResponse>, Status> {
    let raft = {
        let groups = groups.lock().unwrap();
        match groups.get(&req.group_id) {
            Some(g) => g.raft.clone(),
            None => {
                let payload = encode::<Result<(), typ::RaftError>>(Err(
                    typ::RaftError::Fatal(openraft::error::Fatal::Stopped),
                ))
                .map_err(|e| Status::internal(e.to_string()))?;
                return Ok(Response::new(RaftResponse { payload }));
            }
        }
    };

    let res = match req.path.as_str() {
        "/raft/append" => api::append(&raft, &req.payload).await,
        "/raft/snapshot" => api::snapshot(&raft, &req.payload).await,
        "/raft/vote" => api::vote(&raft, &req.payload).await,
        "/raft/transfer_leader" => api::transfer_leader(&raft, &req.payload).await,
        _ => {
            tracing::warn!("unknown grpc path: {}", req.path);
            encode::<Result<(), typ::RaftError>>(Err(typ::RaftError::Fatal(
                openraft::error::Fatal::Stopped,
            )))
        }
    }
    .map_err(|e| Status::internal(e.to_string()))?;

    Ok(Response::new(RaftResponse { payload: res }))
}
