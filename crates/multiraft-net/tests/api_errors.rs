//! MultiRaft API error paths (in-process start_cluster).

use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_core::MultiRaftError;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;

#[tokio::test]
async fn propose_unknown_group_errors() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();

    let nodes = MultiRaft::start_cluster(configs).await.expect("start_cluster");
    let err = nodes[0]
        .propose(99, CounterFsm::encode_add(1, 1))
        .await
        .expect_err("unknown group");

    match err {
        MultiRaftError::UnknownGroup(g) => assert_eq!(g, 99),
        other => panic!("expected UnknownGroup, got {other:?}"),
    }
}

#[tokio::test]
async fn create_group_empty_members_errors() {
    let peer_ids = [1u64];
    let configs = vec![ClusterConfig::for_test(1, &peer_ids)];
    let nodes = MultiRaft::start_cluster(configs).await.expect("start_cluster");

    let err = nodes[0]
        .create_group(1, &[])
        .await
        .expect_err("empty members");

    match err {
        MultiRaftError::Other(e) => {
            assert!(
                e.to_string().contains("at least one member"),
                "unexpected: {e}"
            );
        }
        other => panic!("expected Other, got {other:?}"),
    }
}

#[tokio::test]
async fn create_group_local_not_in_members_errors() {
    let peer_ids = [1u64, 2];
    let configs = vec![ClusterConfig::for_test(1, &peer_ids)];
    let nodes = MultiRaft::start_cluster(configs).await.expect("start_cluster");

    // Local node is 1; members omit it.
    let err = nodes[0]
        .create_group(1, &[2])
        .await
        .expect_err("local not in members");

    match err {
        MultiRaftError::Other(e) => {
            let s = e.to_string();
            assert!(
                s.contains("not in members"),
                "unexpected: {s}"
            );
        }
        other => panic!("expected Other, got {other:?}"),
    }
}

#[tokio::test]
async fn propose_after_shutdown_errors() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();

    let nodes = MultiRaft::start_cluster(configs).await.expect("start_cluster");
    let members = peer_ids.to_vec();
    let group = 1u64;

    for n in &nodes {
        n.create_group(group, &members)
            .await
            .expect("create_group");
    }

    wait_for_leader(&nodes, group, Duration::from_secs(5))
        .await
        .expect("leader elected");

    let target = &nodes[0];
    target.shutdown().await.expect("shutdown");

    let err = target
        .propose(group, CounterFsm::encode_add(1, 1))
        .await
        .expect_err("propose after shutdown");

    match err {
        MultiRaftError::UnknownGroup(_) | MultiRaftError::Other(_) => {}
        other => panic!("expected UnknownGroup or Other, got {other:?}"),
    }
}

#[tokio::test]
async fn try_router_ok_in_process() {
    let peer_ids = [1u64];
    let configs = vec![ClusterConfig::for_test(1, &peer_ids)];
    let nodes = MultiRaft::start_cluster(configs).await.expect("start");
    assert!(nodes[0].try_router().is_ok());
}
