//! Plugin registry + segmented metrics integration with `MultiRaft::propose`.

use std::sync::Arc;

use multiraft_core::ClusterConfig;
use multiraft_core::MetricsPlugin;
use multiraft_core::PluginRegistry;
use multiraft_core::ProposeStage;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;
use multiraft_plugin_metrics::SegmentedLatencyMetrics;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn propose_records_client_write_latency() {
    let metrics = Arc::new(SegmentedLatencyMetrics::new());
    let plugins = PluginRegistry::new().register_metrics(metrics.clone());
    let configs: Vec<_> = (1u64..=3)
        .map(|id| ClusterConfig::for_test(id, &[1, 2, 3]))
        .collect();
    let nodes = MultiRaft::start_cluster_with_plugins(configs, plugins)
        .await
        .expect("start cluster");

    for n in &nodes {
        n.create_group(0, &[1, 2, 3]).await.expect("create group");
    }
    wait_for_leader(&nodes, 0, std::time::Duration::from_secs(5))
        .await
        .expect("leader");

    let leader = nodes
        .iter()
        .find(|n| n.is_leader(0))
        .expect("leader node");
    leader
        .propose(0, CounterFsm::encode_add(1, 1))
        .await
        .expect("propose");

    let snap = metrics.snapshot();
    assert_eq!(snap.plugin, "segmented-latency");
    assert!(
        snap.stages
            .iter()
            .any(|s| s.stage == ProposeStage::ClientWrite && s.count >= 1),
        "expected client_write sample: {:?}",
        snap.stages
    );
}
