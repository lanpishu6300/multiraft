//! Premium plugin integration: metrics, archive, transition, auth (P0–P3 + N2/N3).

use std::sync::Arc;

use multiraft_core::ClusterConfig;
use multiraft_core::MetricsPlugin;
use multiraft_core::PluginRegistry;
use multiraft_core::ProposeStage;
use multiraft_core::TransitionAction;
use multiraft_core::TransitionContext;
use multiraft_core::TransitionPlugin;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;
use multiraft_plugin_archive::CatalogArchive;
use multiraft_plugin_auth::BearerTokenAuth;
use multiraft_plugin_metrics::SegmentedLatencyMetrics;
use multiraft_plugin_ops::premium_registry;
use multiraft_plugin_transition::LagPromotePolicy;
use multiraft_store::SnapshotCatalog;

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "multiraft-premium-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn premium_metrics_on_propose() {
    let metrics = Arc::new(SegmentedLatencyMetrics::new());
    let plugins = PluginRegistry::new().register_metrics(metrics.clone());
    let configs: Vec<_> = (1u64..=3)
        .map(|id| ClusterConfig::for_test(id, &[1, 2, 3]))
        .collect();
    let nodes = MultiRaft::start_cluster_with_plugins(configs, plugins)
        .await
        .expect("start");
    for n in &nodes {
        n.create_group(0, &[1, 2, 3]).await.expect("group");
    }
    wait_for_leader(&nodes, 0, std::time::Duration::from_secs(5))
        .await
        .expect("leader");
    let leader = nodes.iter().find(|n| n.is_leader(0)).expect("leader");
    leader
        .propose(0, CounterFsm::encode_add(1, 1))
        .await
        .expect("propose");
    assert!(
        metrics
            .snapshot()
            .stages
            .iter()
            .any(|s| s.stage == ProposeStage::ClientWrite && s.count >= 1)
    );
}

#[test]
fn premium_archive_lists_catalog_positions() {
    let dir = temp_dir("archive");
    let catalog = Arc::new(SnapshotCatalog::new(dir.join("catalog"), 2));
    catalog
        .write(0, 15, 1, "15-1", b"payload")
        .expect("write");
    let archive = Arc::new(CatalogArchive::new(catalog));
    let plugins = PluginRegistry::new().register_archive(archive);
    let positions = plugins.archive_plugins()[0].list_positions(0);
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].index, 15);
    assert!(plugins.archive_plugins()[0].export_at(0, 15, 1).is_some());
}

#[test]
fn premium_transition_lag_promote() {
    let policy = LagPromotePolicy::enabled(10);
    let action = policy.evaluate(&TransitionContext {
        group: 0,
        leader_applied_index: 100,
        standby_id: 4,
        standby_applied_index: 80,
        standby_is_learner: true,
    });
    assert_eq!(action, TransitionAction::PromoteStandby);
}

#[test]
fn premium_auth_bearer_gate() {
    let reg = PluginRegistry::new().register_auth(Arc::new(BearerTokenAuth::new("tok")));
    assert!(reg.authorize_admin(Some("Bearer tok")));
    assert!(!reg.authorize_admin(Some("Bearer nope")));
}

#[test]
fn premium_ops_bundle_registry() {
    let reg = premium_registry(Some("x"), None);
    assert!(!reg.metrics_plugins().is_empty());
    assert!(!reg.transition_plugins().is_empty());
    assert!(reg.authorize_admin(Some("Bearer x")));
}
