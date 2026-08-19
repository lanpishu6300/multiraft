//! Optional runtime plugins (metrics, archive ops, transition policy, auth, …).
//!
//! Core defines traits and [`PluginRegistry`]; concrete plugins live in separate
//! crates (`multiraft-plugin-*`). `multiraft-net` dispatches lifecycle and propose
//! hooks; `multiraft-demo` wires plugins via `--premium-plugins` or Cargo features.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::GroupId;
use crate::NodeId;

/// Immutable node context passed to plugin lifecycle hooks.
#[derive(Clone, Copy, Debug)]
pub struct PluginContext {
    pub node_id: NodeId,
}

/// First-class log position handle for archive / ops (N3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LogPosition {
    pub group: GroupId,
    pub term: u64,
    pub index: u64,
    pub snapshot_id: String,
}

/// Export manifest for one catalog snapshot (no raw bytes — use HTTP fetch).
#[derive(Clone, Debug, Serialize)]
pub struct ArchiveExport {
    pub position: LogPosition,
    pub size: u64,
    pub sha256_hex: String,
    pub data_path_hint: String,
}

/// Propose path stages for segmented latency (N2b).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProposeStage {
    ClientEnqueued,
    ClientWrite,
    LeaderAppend,
    QuorumAck,
    Committed,
    Applied,
}

impl ProposeStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClientEnqueued => "client_enqueued",
            Self::ClientWrite => "client_write",
            Self::LeaderAppend => "leader_append",
            Self::QuorumAck => "quorum_ack",
            Self::Committed => "committed",
            Self::Applied => "applied",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StageSample {
    pub group: GroupId,
    pub stage: ProposeStage,
    pub duration: Duration,
    pub ok: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct StageLatencySummary {
    pub group: GroupId,
    pub stage: ProposeStage,
    pub count: u64,
    pub ok_count: u64,
    pub sum_us: u64,
    pub max_us: u64,
    pub p50_us: u64,
    pub p99_us: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct MetricsSnapshot {
    pub plugin: &'static str,
    pub stages: Vec<StageLatencySummary>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArchiveSnapshot {
    pub plugin: &'static str,
    pub positions: Vec<LogPosition>,
}

/// Inputs for optional auto transition (A9 extension — disabled by default).
#[derive(Clone, Debug)]
pub struct TransitionContext {
    pub group: GroupId,
    pub leader_applied_index: u64,
    pub standby_id: NodeId,
    pub standby_applied_index: u64,
    pub standby_is_learner: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionAction {
    None,
    PromoteStandby,
}

#[derive(Clone, Debug, Serialize)]
pub struct TransitionSnapshot {
    pub plugin: &'static str,
    pub enabled: bool,
    pub lag_threshold: u64,
    pub last_action: TransitionAction,
}

pub trait Plugin: Send + Sync {
    fn name(&self) -> &'static str;

    fn on_node_start(&self, _ctx: &PluginContext) {}

    fn on_node_stop(&self, _ctx: &PluginContext) {}

    fn on_group_ready(&self, _ctx: &PluginContext, _group: GroupId) {}
}

pub trait MetricsPlugin: Plugin {
    fn record_stage(&self, sample: StageSample);

    fn snapshot(&self) -> MetricsSnapshot;
}

/// N3 archive ops over local SnapshotCatalog (plugin impl in `multiraft-plugin-archive`).
pub trait ArchivePlugin: Plugin {
    fn list_positions(&self, group: GroupId) -> Vec<LogPosition>;

    fn export_at(&self, group: GroupId, index: u64, term: u64) -> Option<ArchiveExport>;

    /// Read snapshot bytes for `(group, index, term)` when available locally.
    fn read_bytes_at(&self, group: GroupId, index: u64, term: u64) -> Option<Vec<u8>>;

    fn snapshot(&self) -> ArchiveSnapshot;
}

/// Optional warm-DR automation (operator must enable explicitly).
pub trait TransitionPlugin: Plugin {
    fn evaluate(&self, ctx: &TransitionContext) -> TransitionAction;

    fn snapshot(&self) -> TransitionSnapshot;
}

/// Admin / stale-query gate (A12 extension).
pub trait AuthPlugin: Plugin {
    fn authorize_admin(&self, authorization: Option<&str>) -> bool;
}

#[derive(Default)]
pub struct PluginRegistry {
    plugins: Vec<Arc<dyn Plugin>>,
    metrics: Vec<Arc<dyn MetricsPlugin>>,
    archive: Vec<Arc<dyn ArchivePlugin>>,
    transition: Vec<Arc<dyn TransitionPlugin>>,
    auth: Vec<Arc<dyn AuthPlugin>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_plugin(mut self, plugin: Arc<dyn Plugin>) -> Self {
        self.plugins.push(plugin);
        self
    }

    pub fn register_metrics(mut self, plugin: Arc<dyn MetricsPlugin>) -> Self {
        self.metrics.push(plugin.clone());
        self.plugins.push(plugin);
        self
    }

    pub fn register_archive(mut self, plugin: Arc<dyn ArchivePlugin>) -> Self {
        self.archive.push(plugin.clone());
        self.plugins.push(plugin);
        self
    }

    pub fn register_transition(mut self, plugin: Arc<dyn TransitionPlugin>) -> Self {
        self.transition.push(plugin.clone());
        self.plugins.push(plugin);
        self
    }

    pub fn register_auth(mut self, plugin: Arc<dyn AuthPlugin>) -> Self {
        self.auth.push(plugin.clone());
        self.plugins.push(plugin);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    pub fn metrics_plugins(&self) -> &[Arc<dyn MetricsPlugin>] {
        &self.metrics
    }

    pub fn archive_plugins(&self) -> &[Arc<dyn ArchivePlugin>] {
        &self.archive
    }

    pub fn transition_plugins(&self) -> &[Arc<dyn TransitionPlugin>] {
        &self.transition
    }

    pub fn auth_plugins(&self) -> &[Arc<dyn AuthPlugin>] {
        &self.auth
    }

    pub fn on_node_start(&self, ctx: &PluginContext) {
        for p in &self.plugins {
            p.on_node_start(ctx);
        }
    }

    pub fn on_node_stop(&self, ctx: &PluginContext) {
        for p in &self.plugins {
            p.on_node_stop(ctx);
        }
    }

    pub fn on_group_ready(&self, ctx: &PluginContext, group: GroupId) {
        for p in &self.plugins {
            p.on_group_ready(ctx, group);
        }
    }

    pub fn record_stage(&self, sample: StageSample) {
        for m in &self.metrics {
            m.record_stage(sample.clone());
        }
    }

    pub fn metrics_snapshots(&self) -> Vec<MetricsSnapshot> {
        self.metrics.iter().map(|m| m.snapshot()).collect()
    }

    pub fn archive_snapshots(&self) -> Vec<ArchiveSnapshot> {
        self.archive.iter().map(|a| a.snapshot()).collect()
    }

    pub fn transition_snapshots(&self) -> Vec<TransitionSnapshot> {
        self.transition.iter().map(|t| t.snapshot()).collect()
    }

    /// All auth plugins must pass (AND). Empty registry ⇒ allow.
    pub fn authorize_admin(&self, authorization: Option<&str>) -> bool {
        if self.auth.is_empty() {
            return true;
        }
        self.auth
            .iter()
            .all(|a| a.authorize_admin(authorization))
    }
}

impl Clone for PluginRegistry {
    fn clone(&self) -> Self {
        Self {
            plugins: self.plugins.clone(),
            metrics: self.metrics.clone(),
            archive: self.archive.clone(),
            transition: self.transition.clone(),
            auth: self.auth.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopMetric;

    impl Plugin for NoopMetric {
        fn name(&self) -> &'static str {
            "noop"
        }
    }

    impl MetricsPlugin for NoopMetric {
        fn record_stage(&self, _sample: StageSample) {}

        fn snapshot(&self) -> MetricsSnapshot {
            MetricsSnapshot {
                plugin: self.name(),
                stages: Vec::new(),
            }
        }
    }

    struct DenyAuth;

    impl Plugin for DenyAuth {
        fn name(&self) -> &'static str {
            "deny"
        }
    }

    impl AuthPlugin for DenyAuth {
        fn authorize_admin(&self, _authorization: Option<&str>) -> bool {
            false
        }
    }

    #[test]
    fn registry_dispatches_metrics() {
        let reg = PluginRegistry::new().register_metrics(Arc::new(NoopMetric));
        let ctx = PluginContext { node_id: 1 };
        reg.on_node_start(&ctx);
        reg.record_stage(StageSample {
            group: 0,
            stage: ProposeStage::ClientWrite,
            duration: Duration::from_micros(10),
            ok: true,
        });
        assert_eq!(reg.metrics_snapshots().len(), 1);
    }

    #[test]
    fn auth_plugins_and_gate() {
        let open = PluginRegistry::new();
        assert!(open.authorize_admin(None));

        let closed = PluginRegistry::new().register_auth(Arc::new(DenyAuth));
        assert!(!closed.authorize_admin(None));
    }
}
