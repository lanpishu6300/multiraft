//! Cross-process tonic MultiRaft: 3 nodes, 1 group, O(nodes) peer channels.

use std::net::SocketAddr;
use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_three_node_propose() {
    let addrs = [free_addr(), free_addr(), free_addr()];
    let peer_ids = [1u64, 2, 3];
    let peers: Vec<(u64, SocketAddr)> = peer_ids
        .iter()
        .zip(addrs.iter())
        .map(|(&id, &addr)| (id, addr))
        .collect();

    let mut nodes = Vec::with_capacity(3);
    for &id in &peer_ids {
        let mut config = ClusterConfig::for_test(id, &peer_ids);
        config.peers = peers.clone();
        config.data_dir = Default::default();
        nodes.push(
            MultiRaft::start_grpc(config)
                .await
                .unwrap_or_else(|e| panic!("start_grpc node {id}: {e:#}")),
        );
    }

    let members = peer_ids.to_vec();
    let group = 1u64;
    for n in &nodes {
        n.create_group(group, &members)
            .await
            .unwrap_or_else(|e| panic!("create_group on {}: {e:?}", n.node_id()));
    }

    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("leader elected over gRPC");

    let leader = nodes
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");

    let data = CounterFsm::encode_add(7, /*idem=*/ 42);
    leader
        .propose(group, data)
        .await
        .expect("propose from leader");

    // Wait for all FSMs to apply.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut values = Vec::new();
        for n in &nodes {
            let v = n
                .with_fsm(group, |fsm| fsm.value(group))
                .await
                .expect("fsm present");
            values.push(v);
        }
        if values.iter().all(|&v| v == 7) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("FSM values did not converge to 7: {values:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Each node opens channels to the other 2 peers → sum ≈ 6, O(nodes) not O(groups).
    let link_sum: usize = nodes.iter().map(|n| n.unique_peer_links()).sum();
    assert!(
        link_sum < 10,
        "expected O(nodes) total unique_peer_links (<10), got {link_sum}"
    );
    assert!(
        link_sum <= 6,
        "3 nodes × ≤2 outbound peers → sum ≤ 6, got {link_sum}"
    );

    for n in &nodes {
        match n.try_router() {
            Err(multiraft_core::MultiRaftError::InProcessRouterUnavailable) => {}
            other => panic!("gRPC node must not expose in-process router: {other:?}"),
        }
    }
}
