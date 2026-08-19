//! Public MultiRaft facade over in-process [`Router`] or cross-process [`GrpcRouter`].
//!
//! Lives in `multiraft-net` (not `multiraft-core`) to avoid a core↔net dependency
//! cycle: net already depends on core for [`TypeConfig`].
//!
//! # In-process cluster
//!
//! - [`MultiRaft::start`] starts **one** node with its own [`Router`].
//! - [`MultiRaft::start_cluster`] starts N nodes sharing one [`Router`] (preferred for tests).
//! - [`SharedFabric`] exposes the shared [`Router`] + glue so chaos tests can
//!   `shutdown` a node and [`SharedFabric::start_node`] it again with the same
//!   `node_id` / `data_dir` / peers.
//!
//! # Cross-process (gRPC)
//!
//! - [`MultiRaft::start_grpc`] binds a tonic server on this node's peer addr and
//!   uses [`GrpcRouter`] for outbound Raft RPCs.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use crate::grpc::GrpcRouter;
use crate::grpc::GrpcServer;
use crate::network::GrpcNetworkFactory;
use crate::network::NetworkFactory;
use crate::node::GroupApp;
use crate::node::GroupMap;
use crate::node::Node;
use crate::router::Router;
use crate::snapshot_fetch::pull_snapshot_chunked;
use crate::standby_throttle::StandbyThrottle;
use multiraft_core::ClusterConfig;
use multiraft_core::GroupId;
use multiraft_core::MultiRaftError;
use multiraft_core::NodeId;
use multiraft_core::NodeRole;
use multiraft_core::ProposeOk;
use multiraft_core::RecoverOutcome;
use multiraft_core::Request;
use multiraft_core::SnapshotAdvertisement;
use multiraft_core::SnapshotMode;
use multiraft_core::StaleRead;
use multiraft_core::TypeConfig;
use multiraft_core::STANDBY_SNAPSHOT_TRIGGER;
use multiraft_fsm::CounterFsm;
use multiraft_fsm::StateMachine;
use multiraft_store::CatalogEntry;
use multiraft_store::FileLogStoreOf;
use multiraft_store::FileLogStreamOptions;
use multiraft_store::MemLogStore;
use multiraft_store::Raft;
use multiraft_store::SmOptions;
use multiraft_store::SnapshotCatalog;
use multiraft_store::StateMachineStore;
use multiraft_store::TriggerCb;
use openraft::async_runtime::WatchReceiver;
use openraft::error::InitializeError;
use openraft::error::RaftError;
use openraft::type_config::TypeConfigExt;
use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::Config;
use openraft::ReadPolicy;

type LeaderCb = Arc<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>;
type SnapshotReadyCb = Arc<dyn Fn(SnapshotAdvertisement) + Send + Sync + 'static>;

/// Shared snapshot catalog / ads for one MultiRaft node.
struct SnapshotRuntime {
    catalog: Option<Arc<SnapshotCatalog>>,
    ads: Mutex<Vec<SnapshotAdvertisement>>,
    serialize_delay: Mutex<Option<Duration>>,
    data_dir: PathBuf,
    admin_advertise_addr: Option<std::net::SocketAddr>,
    on_snapshot_ready: Mutex<Option<SnapshotReadyCb>>,
}

impl SnapshotRuntime {
    fn new(config: &ClusterConfig) -> Arc<Self> {
        let catalog = if config.snapshot_mode == SnapshotMode::StandbyOffload
            && !config.data_dir.as_os_str().is_empty()
        {
            let root = config.data_dir.join("snapshots");
            let _ = fs::create_dir_all(&root);
            Some(Arc::new(SnapshotCatalog::new(root, config.snapshot_keep)))
        } else {
            None
        };
        Arc::new(Self {
            catalog,
            ads: Mutex::new(Self::load_ads(&config.data_dir)),
            serialize_delay: Mutex::new(None),
            data_dir: config.data_dir.clone(),
            admin_advertise_addr: config.admin_advertise_addr,
            on_snapshot_ready: Mutex::new(None),
        })
    }

    fn load_ads(data_dir: &std::path::Path) -> Vec<SnapshotAdvertisement> {
        if data_dir.as_os_str().is_empty() {
            return Vec::new();
        }
        let path = data_dir.join("snapshot_ads.json");
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }

    fn persist_ads(&self) {
        if self.data_dir.as_os_str().is_empty() {
            return;
        }
        let ads = self.ads.lock().unwrap().clone();
        if let Ok(bytes) = serde_json::to_vec_pretty(&ads) {
            let _ = fs::write(self.data_dir.join("snapshot_ads.json"), bytes);
        }
    }

    fn record_ad(&self, ad: SnapshotAdvertisement) {
        {
            let mut ads = self.ads.lock().unwrap();
            ads.retain(|a| !(a.group == ad.group && a.snapshot_id == ad.snapshot_id));
            ads.push(ad.clone());
        }
        self.persist_ads();
        if let Some(cb) = self.on_snapshot_ready.lock().unwrap().clone() {
            cb(ad);
        }
    }
}

/// Coordinates `create_group` so membership is initialized once all peers are local.
#[derive(Clone, Default)]
struct ClusterGlue {
    /// group -> nodes that have created the local raft
    ready: Arc<Mutex<HashMap<GroupId, HashSet<NodeId>>>>,
    /// groups that already claimed initialize
    claimed_init: Arc<Mutex<HashSet<GroupId>>>,
}

impl ClusterGlue {
    fn mark_ready(&self, group: GroupId, node_id: NodeId, members: &[NodeId]) -> bool {
        let mut ready = self.ready.lock().unwrap();
        let set = ready.entry(group).or_default();
        set.insert(node_id);
        members.iter().all(|m| set.contains(m))
    }

    fn try_claim_init(&self, group: GroupId) -> bool {
        self.claimed_init.lock().unwrap().insert(group)
    }
}

/// Shared in-process fabric: one [`Router`] + cluster glue for many nodes.
///
/// Use this when a test needs to restart a node after [`MultiRaft::shutdown`]
/// without losing the peer mesh (unregister on shutdown; re-register on start).
#[derive(Clone, Default)]
pub struct SharedFabric {
    router: Router,
    glue: ClusterGlue,
}

impl SharedFabric {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn router(&self) -> &Router {
        &self.router
    }

    pub async fn start_node(&self, config: ClusterConfig) -> anyhow::Result<MultiRaft> {
        MultiRaft::start_inner(config, self.router.clone(), self.glue.clone(), |_| {
            CounterFsm::new()
        })
        .await
    }
}

enum NetBackend {
    InProcess { router: Router, glue: ClusterGlue },
    Grpc { router: GrpcRouter },
}

/// Multi-Raft handle for one node (many groups).
pub struct MultiRaft<S: StateMachine = CounterFsm> {
    node_id: NodeId,
    config: ClusterConfig,
    net: NetBackend,
    groups: GroupMap<S>,
    make_fsm: Arc<dyn Fn(GroupId) -> S + Send + Sync>,
    leader_cbs: Arc<Mutex<Vec<LeaderCb>>>,
    snapshot_rt: Arc<SnapshotRuntime>,
    /// Standby node ids for replication throttle (shared with Router / GrpcRouter).
    standby_throttle: StandbyThrottle,
}

impl MultiRaft<CounterFsm> {
    /// Start a single in-process node with a private [`Router`].
    pub async fn start(config: ClusterConfig) -> anyhow::Result<Self> {
        Self::start_inner(config, Router::new(), ClusterGlue::default(), |_| {
            CounterFsm::new()
        })
        .await
    }

    /// Start N nodes sharing one [`Router`] (in-process multi-node harness).
    ///
    /// `SocketAddr` peers in each config are unused; nodes are linked via the shared router.
    /// Internally uses [`SharedFabric`]; prefer that type when tests need node restart.
    pub async fn start_cluster(configs: Vec<ClusterConfig>) -> anyhow::Result<Vec<Self>> {
        let fabric = SharedFabric::new();
        let mut nodes = Vec::with_capacity(configs.len());
        for config in configs {
            nodes.push(fabric.start_node(config).await?);
        }
        Ok(nodes)
    }

    /// Start one node with cross-process tonic transport.
    ///
    /// Binds a gRPC server on this node's address from `config.peers` and uses
    /// [`GrpcRouter`] for outbound Raft RPCs to other peers.
    pub async fn start_grpc(config: ClusterConfig) -> anyhow::Result<Self> {
        Self::start_grpc_inner(config, |_| CounterFsm::new()).await
    }
}

impl<S: StateMachine> MultiRaft<S> {
    async fn start_inner<F>(
        config: ClusterConfig,
        router: Router,
        glue: ClusterGlue,
        make_fsm: F,
    ) -> anyhow::Result<Self>
    where
        F: Fn(GroupId) -> S + Send + Sync + 'static,
    {
        let groups: GroupMap<S> = Arc::new(Mutex::new(BTreeMap::new()));
        let (node, _tx) = Node::with_groups(config.node_id, router.clone(), groups.clone());
        TypeConfig::spawn(node.run());
        let snapshot_rt = SnapshotRuntime::new(&config);
        router.throttle().apply_config(&config);
        let standby_throttle = router.throttle().clone();

        Ok(Self {
            node_id: config.node_id,
            config,
            net: NetBackend::InProcess { router, glue },
            groups,
            make_fsm: Arc::new(make_fsm),
            leader_cbs: Arc::new(Mutex::new(Vec::new())),
            snapshot_rt,
            standby_throttle,
        })
    }

    async fn start_grpc_inner<F>(config: ClusterConfig, make_fsm: F) -> anyhow::Result<Self>
    where
        F: Fn(GroupId) -> S + Send + Sync + 'static,
    {
        let self_addr = config
            .peers
            .iter()
            .find(|(id, _)| *id == config.node_id)
            .map(|(_, a)| *a)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "start_grpc: node {} missing from config.peers",
                    config.node_id
                )
            })?;

        let groups: GroupMap<S> = Arc::new(Mutex::new(BTreeMap::new()));
        let grpc_router = GrpcRouter::from_config(&config);
        let standby_throttle = grpc_router.throttle().clone();
        let snapshot_rt = SnapshotRuntime::new(&config);

        let groups_for_server = groups.clone();
        let listener = tokio::net::TcpListener::bind(self_addr).await?;
        tokio::spawn(async move {
            if let Err(e) = GrpcServer::serve_with_listener(listener, groups_for_server).await {
                tracing::error!("grpc server on {self_addr} exited: {e:#}");
            }
        });

        // Give the accept loop a moment to register with the runtime.
        TypeConfig::sleep(Duration::from_millis(20)).await;

        Ok(Self {
            node_id: config.node_id,
            config,
            net: NetBackend::Grpc {
                router: grpc_router,
            },
            groups,
            make_fsm: Arc::new(make_fsm),
            leader_cbs: Arc::new(Mutex::new(Vec::new())),
            snapshot_rt,
            standby_throttle,
        })
    }

    /// Create (or idempotently ensure) a local Raft group peer.
    ///
    /// **In-process:** when every `members` node has created the group, membership
    /// is initialized once (racing callers see `NotAllowed` and ignore).
    ///
    /// **gRPC:** each process spawns the local raft then tries `initialize`;
    /// `NotAllowed` is ignored (no cross-process ClusterGlue).
    ///
    /// **Standby:** local node may be absent from `members` (voters only). Spawns
    /// the local raft without calling `initialize`; join via [`Self::add_standby`].
    pub async fn create_group(&self, group: u64, members: &[u64]) -> Result<(), MultiRaftError> {
        if members.is_empty() {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "create_group requires at least one member"
            )));
        }

        let is_standby = self.config.role == NodeRole::Standby;
        if !is_standby && !members.contains(&self.node_id) {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "local node {} is not in members {:?}",
                self.node_id,
                members
            )));
        }

        let needs_spawn = !self.groups.lock().unwrap().contains_key(&group);
        if needs_spawn {
            self.spawn_local_group(group).await?;
        }

        if is_standby {
            // Learner: wait for leader `add_learner`; do not initialize.
            return Ok(());
        }

        match &self.net {
            NetBackend::InProcess { glue, .. } => {
                let all_ready = glue.mark_ready(group, self.node_id, members);
                if all_ready && glue.try_claim_init(group) {
                    self.try_initialize(group, members).await?;
                }
            }
            NetBackend::Grpc { .. } => {
                // Cross-process: every node attempts initialize; loser gets NotAllowed.
                self.try_initialize(group, members).await?;
            }
        }

        Ok(())
    }

    /// Leader-only: add a Standby as an openraft Learner (`add_learner`, blocking).
    ///
    /// Retries transient "configuration change in progress" errors until membership
    /// from a prior `initialize` / `change_membership` commits (openraft requirement).
    pub async fn add_standby(&self, group: u64, standby_id: u64) -> Result<(), MultiRaftError> {
        retry_on_membership_pending(|| self.add_standby_once(group, standby_id)).await
    }

    async fn add_standby_once(&self, group: u64, standby_id: u64) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let addr = self
            .config
            .peers
            .iter()
            .find(|(n, _)| *n == standby_id)
            .map(|(_, a)| a.to_string())
            .unwrap_or_default();
        let node = BasicNode { addr };
        match raft.add_learner(standby_id, node, true).await {
            Ok(_) => {
                self.standby_throttle.insert(standby_id);
                Ok(())
            }
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                let msg = e.to_string();
                if msg.contains("configuration change") {
                    return Err(transient_membership_err("add_learner", &e));
                }
                Err(MultiRaftError::Other(anyhow::anyhow!("add_learner: {e}")))
            }
        }
    }

    /// Standby ids currently subject to replication throttle.
    pub fn standby_throttle_ids(&self) -> HashSet<NodeId> {
        self.standby_throttle.standby_ids()
    }

    /// Newest local snapshot advertisement for `group` by `(last_term, last_index)`.
    pub fn best_snapshot_ad(&self, group: u64) -> Option<SnapshotAdvertisement> {
        self.snapshot_ads()
            .into_iter()
            .filter(|a| a.group == group && !a.fetch_url.is_empty())
            .max_by_key(|a| (a.last_term, a.last_index))
    }

    /// HTTP GET `fetch_url` (chunked Range when possible), verify sha256, install into FSM.
    pub async fn pull_and_install_snapshot(
        &self,
        group: u64,
        fetch_url: &str,
    ) -> Result<(), MultiRaftError> {
        let fetched = self
            .fetch_snapshot_bytes(fetch_url)
            .await
            .map_err(MultiRaftError::Other)?;
        self.install_durable_snapshot(group, fetched.last_index, fetched.last_term, fetched.data)
            .await
    }

    /// Fetch snapshot bytes via chunked Range download (resume temp under data_dir / temp).
    pub async fn fetch_snapshot_bytes(
        &self,
        fetch_url: &str,
    ) -> Result<crate::snapshot_fetch::FetchedSnapshot, anyhow::Error> {
        let chunk = self.config.snapshot_fetch_chunk_bytes.max(1);
        let temp_dir = if self.config.data_dir.as_os_str().is_empty() {
            std::env::temp_dir().join("multiraft-snap-fetch")
        } else {
            self.config.data_dir.join("snap-fetch-tmp")
        };
        pull_snapshot_chunked(fetch_url, chunk, &temp_dir).await
    }

    /// Pick the newest local snapshot ad for `group` and pull if newer than local applied.
    pub async fn try_recover_from_standby_ads(
        &self,
        group: u64,
    ) -> Result<RecoverOutcome, MultiRaftError> {
        let Some(ad) = self.best_snapshot_ad(group) else {
            return Ok(RecoverOutcome::SkippedNoAd);
        };

        let (local_index, local_term) = self.local_applied(group).await.unwrap_or((0, 0));
        if !log_pos_newer(ad.last_term, ad.last_index, local_term, local_index) {
            return Ok(RecoverOutcome::SkippedNotNewer {
                local_index,
                ad_index: ad.last_index,
            });
        }

        match self.pull_and_install_snapshot(group, &ad.fetch_url).await {
            Ok(()) => Ok(RecoverOutcome::Installed {
                last_index: ad.last_index,
                last_term: ad.last_term,
            }),
            Err(MultiRaftError::UnknownGroup(g)) => Err(MultiRaftError::UnknownGroup(g)),
            Err(e) => Ok(RecoverOutcome::FetchFailed {
                error: e.to_string(),
            }),
        }
    }

    /// Pull latest snapshot from [`ClusterConfig::daisy_upstream_base`] into local
    /// catalog + FSM, then refresh a local [`SnapshotAdvertisement`].
    ///
    /// Skips install (and returns [`RecoverOutcome::SkippedNotNewer`]) when the
    /// upstream snapshot is not strictly newer than the local SM applied watermark.
    pub async fn sync_from_daisy_upstream(
        &self,
        group: u64,
    ) -> Result<RecoverOutcome, MultiRaftError> {
        let base = self.config.daisy_upstream_base.as_ref().ok_or_else(|| {
            MultiRaftError::Other(anyhow::anyhow!(
                "sync_from_daisy_upstream: daisy_upstream_base not set"
            ))
        })?;
        let url = format!("{}/snapshots/{group}/latest", base.trim_end_matches('/'));

        let fetched = match self.fetch_snapshot_bytes(&url).await {
            Ok(f) => f,
            Err(e) => {
                return Ok(RecoverOutcome::FetchFailed {
                    error: e.to_string(),
                })
            }
        };

        let (local_index, local_term) = self.local_applied(group).await.unwrap_or((0, 0));
        if !log_pos_newer(
            fetched.last_term,
            fetched.last_index,
            local_term,
            local_index,
        ) {
            return Ok(RecoverOutcome::SkippedNotNewer {
                local_index,
                ad_index: fetched.last_index,
            });
        }

        let snapshot_id = fetched
            .snapshot_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", fetched.last_index, fetched.last_term));

        if let Some(catalog) = self.snapshot_rt.catalog.as_ref() {
            catalog
                .write(
                    group,
                    fetched.last_index,
                    fetched.last_term,
                    &snapshot_id,
                    &fetched.data,
                )
                .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("daisy catalog write: {e}")))?;
        } else {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "sync_from_daisy_upstream: SnapshotCatalog required (StandbyOffload + data_dir)"
            )));
        }

        if self.raft(group).is_some() {
            self.install_durable_snapshot(
                group,
                fetched.last_index,
                fetched.last_term,
                fetched.data.clone(),
            )
            .await?;
        }

        let fetch_url = self
            .snapshot_rt
            .admin_advertise_addr
            .map(|addr| format!("http://{addr}/snapshots/{group}/latest"))
            .unwrap_or_default();
        let ad = SnapshotAdvertisement {
            group,
            last_index: fetched.last_index,
            last_term: fetched.last_term,
            snapshot_id,
            size: fetched.data.len() as u64,
            sha256_hex: fetched.sha256_hex,
            fetch_url,
        };
        self.record_snapshot_ad(ad);

        Ok(RecoverOutcome::Installed {
            last_index: fetched.last_index,
            last_term: fetched.last_term,
        })
    }

    /// Background loop: when `daisy_upstream_base` is set, periodically
    /// [`Self::sync_from_daisy_upstream`] for each group (interval from config).
    pub fn spawn_daisy_sync_loop(&self, groups: Vec<u64>) {
        let Some(base) = self.config.daisy_upstream_base.clone() else {
            tracing::debug!("spawn_daisy_sync_loop: daisy_upstream_base unset, skip");
            return;
        };
        let interval = Duration::from_millis(self.config.daisy_sync_interval_ms.max(1));
        let chunk = self.config.snapshot_fetch_chunk_bytes.max(1);
        let temp_dir = if self.config.data_dir.as_os_str().is_empty() {
            std::env::temp_dir().join("multiraft-snap-fetch")
        } else {
            self.config.data_dir.join("snap-fetch-tmp")
        };
        let snapshot_rt = self.snapshot_rt.clone();
        let groups_map = self.groups.clone();
        let admin = self.snapshot_rt.admin_advertise_addr;

        TypeConfig::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                for &group in &groups {
                    match daisy_sync_once(
                        &base,
                        group,
                        chunk,
                        &temp_dir,
                        &snapshot_rt,
                        &groups_map,
                        admin,
                    )
                    .await
                    {
                        Ok(RecoverOutcome::Installed { last_index, .. }) => {
                            tracing::info!(
                                group,
                                last_index,
                                "daisy sync installed snapshot from upstream"
                            );
                        }
                        Ok(other) => {
                            tracing::debug!(group, ?other, "daisy sync outcome");
                        }
                        Err(e) => {
                            tracing::warn!(group, error = %e, "daisy sync failed");
                        }
                    }
                }
            }
        });
    }

    /// Leader-only: promote a Standby learner to voter (`change_membership` AddVoterIds).
    pub async fn promote_standby(&self, group: u64, node_id: u64) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let membership = raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .clone();
        let is_learner = membership.learner_ids().any(|id| id == node_id);
        let is_voter = membership.voter_ids().any(|id| id == node_id);
        if is_voter {
            self.standby_throttle.remove(node_id);
            return Ok(());
        }
        if !is_learner {
            return Err(MultiRaftError::Other(anyhow::anyhow!(
                "promote_standby: node {node_id} is not a learner in group {group}"
            )));
        }
        retry_on_membership_pending(|| self.promote_standby_once(group, node_id)).await
    }

    async fn promote_standby_once(&self, group: u64, node_id: u64) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let mut add = BTreeSet::new();
        add.insert(node_id);
        match raft
            .change_membership(ChangeMembers::AddVoterIds(add), true)
            .await
        {
            Ok(_) => {
                self.standby_throttle.remove(node_id);
                Ok(())
            }
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                let msg = e.to_string();
                if msg.contains("configuration change") {
                    return Err(transient_membership_err("promote_standby change_membership", &e));
                }
                Err(MultiRaftError::Other(anyhow::anyhow!(
                    "promote_standby change_membership: {e}"
                )))
            }
        }
    }

    /// Leader-only: demote a voter to Standby learner (`RemoveVoters`, retain=true).
    pub async fn demote_to_standby(&self, group: u64, node_id: u64) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let membership = raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .clone();
        let is_voter = membership.voter_ids().any(|id| id == node_id);
        if !is_voter {
            self.standby_throttle.insert(node_id);
            return Ok(());
        }
        retry_on_membership_pending(|| self.demote_to_standby_once(group, node_id)).await
    }

    async fn demote_to_standby_once(&self, group: u64, node_id: u64) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let mut remove = BTreeSet::new();
        remove.insert(node_id);
        match raft
            .change_membership(ChangeMembers::RemoveVoters(remove), true)
            .await
        {
            Ok(_) => {
                self.standby_throttle.insert(node_id);
                Ok(())
            }
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                let msg = e.to_string();
                if msg.contains("configuration change") {
                    return Err(transient_membership_err(
                        "demote_to_standby change_membership",
                        &e,
                    ));
                }
                Err(MultiRaftError::Other(anyhow::anyhow!(
                    "demote_to_standby change_membership: {e}"
                )))
            }
        }
    }

    /// Current voter ids from raft metrics (committed membership view may lag slightly).
    pub fn voter_ids(&self, group: u64) -> Option<BTreeSet<NodeId>> {
        let raft = self.raft(group)?;
        Some(
            raft.metrics()
                .borrow_watched()
                .membership_config
                .membership()
                .voter_ids()
                .collect(),
        )
    }

    /// Current learner (Standby) ids from raft metrics.
    pub fn learner_ids(&self, group: u64) -> Option<BTreeSet<NodeId>> {
        let raft = self.raft(group)?;
        Some(
            raft.metrics()
                .borrow_watched()
                .membership_config
                .membership()
                .learner_ids()
                .collect(),
        )
    }

    /// Leader proposes the magic standby-snapshot trigger log entry.
    pub async fn trigger_standby_snapshot(&self, group: u64) -> Result<ProposeOk, MultiRaftError> {
        self.propose(group, STANDBY_SNAPSHOT_TRIGGER.to_vec()).await
    }

    /// Record a snapshot advertisement (persisted when `data_dir` is set).
    pub fn record_snapshot_ad(&self, ad: SnapshotAdvertisement) {
        self.snapshot_rt.record_ad(ad);
    }

    /// Return all locally known snapshot advertisements.
    pub fn snapshot_ads(&self) -> Vec<SnapshotAdvertisement> {
        self.snapshot_rt.ads.lock().unwrap().clone()
    }

    /// Optional callback when a Standby finishes an async snapshot.
    pub fn on_snapshot_ready<F>(&self, cb: F)
    where
        F: Fn(SnapshotAdvertisement) + Send + Sync + 'static,
    {
        *self.snapshot_rt.on_snapshot_ready.lock().unwrap() = Some(Arc::new(cb));
    }

    /// Test hook: artificial delay inside `spawn_blocking` serialize/fsync.
    pub fn set_snapshot_serialize_delay(&self, delay: Option<Duration>) {
        *self.snapshot_rt.serialize_delay.lock().unwrap() = delay;
    }

    /// Durable catalog for this node (StandbyOffload + data_dir), if any.
    pub fn snapshot_catalog(&self) -> Option<Arc<SnapshotCatalog>> {
        self.snapshot_rt.catalog.clone()
    }

    /// Latest catalog entry for `group` on this node.
    pub fn latest_catalog_entry(&self, group: GroupId) -> Option<CatalogEntry> {
        self.snapshot_rt
            .catalog
            .as_ref()?
            .latest(group)
            .ok()
            .flatten()
    }

    /// Install durable snapshot bytes into the local state machine (recovery).
    pub async fn install_durable_snapshot(
        &self,
        group: GroupId,
        last_index: u64,
        last_term: u64,
        data: Vec<u8>,
    ) -> Result<(), MultiRaftError> {
        let sm = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.state_machine.clone())
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        sm.install_durable_snapshot(last_index, last_term, true, data)
            .await
            .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("install_durable_snapshot: {e}")))
    }

    /// Pull snapshot bytes from a Standby's catalog (in-process helper) and install.
    pub async fn try_install_from_standby_catalog(
        &self,
        group: GroupId,
        standby_catalog: &SnapshotCatalog,
    ) -> Result<(), MultiRaftError> {
        let entry = standby_catalog
            .latest(group)
            .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("catalog latest: {e}")))?
            .ok_or_else(|| {
                MultiRaftError::Other(anyhow::anyhow!("no snapshot in standby catalog"))
            })?;
        let data = standby_catalog
            .read(group, &entry.snapshot_id)
            .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("catalog read: {e}")))?
            .ok_or_else(|| MultiRaftError::Other(anyhow::anyhow!("snapshot data missing")))?;
        self.install_durable_snapshot(group, entry.last_index, entry.last_term, data)
            .await
    }

    async fn try_initialize(
        &self,
        group: GroupId,
        members: &[NodeId],
    ) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let nodes = self.membership_nodes(members);
        match raft.initialize(nodes).await {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(e) => Err(MultiRaftError::Other(anyhow::anyhow!(
                "initialize group: {e}"
            ))),
        }
    }

    /// Propose application bytes via openraft `client_write`.
    ///
    /// On `Ok`, the write is committed by a quorum and applied (linearizable write
    /// for this group). Non-leader → [`MultiRaftError::NotLeader`].
    /// Timeout / disconnect ⇒ outcome **unknown**; retry with the same idempotency key.
    pub async fn propose(&self, group: u64, data: Vec<u8>) -> Result<ProposeOk, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        Self::client_write_one(&raft, data).await
    }

    /// Pipeline many proposes: **one Raft entry per payload**, concurrent `client_write`.
    ///
    /// Returns `Ok` only if **all** entries succeed. On any failure (including
    /// [`MultiRaftError::NotLeader`]), returns that error — some entries may already
    /// be committed; callers must use idempotency keys.
    ///
    /// Each `client_write` is polled concurrently via `try_join_all` so N quorum
    /// waits overlap (deep pipeline). openraft `api_batch_*` may still merge
    /// consecutive writes into fatter storage appends.
    pub async fn propose_batch(
        &self,
        group: u64,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<ProposeOk>, MultiRaftError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        futures::future::try_join_all(payloads.into_iter().map(|data| {
            let raft = raft.clone();
            async move { Self::client_write_one(&raft, data).await }
        }))
        .await
    }

    /// One Core API message for many payloads (fatter appends; shallower client
    /// pipeline than [`Self::propose_batch`]).
    pub async fn propose_many(
        &self,
        group: u64,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<ProposeOk>, MultiRaftError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let n = payloads.len();
        let mut stream = raft
            .client_write_many(payloads.into_iter().map(Request::new))
            .await
            .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("client_write_many: {e}")))?;
        let mut out = Vec::with_capacity(n);
        while let Some(item) = futures::StreamExt::next(&mut stream).await {
            let result = item.map_err(|e| {
                MultiRaftError::Other(anyhow::anyhow!("client_write_many stream: {e}"))
            })?;
            match result {
                Ok(resp) => out.push(ProposeOk {
                    index: resp.log_id.index(),
                    term: resp.log_id.committed_leader_id().term,
                }),
                Err(fwd) => {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
            }
        }
        Ok(out)
    }

    async fn client_write_one(raft: &Raft<S>, data: Vec<u8>) -> Result<ProposeOk, MultiRaftError> {
        match raft.client_write(Request::new(data)).await {
            Ok(resp) => Ok(ProposeOk {
                index: resp.log_id.index(),
                term: resp.log_id.committed_leader_id().term,
            }),
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                Err(MultiRaftError::Other(anyhow::anyhow!("client_write: {e}")))
            }
        }
    }

    /// Linearizable read: confirm leadership (ReadIndex), then read the local FSM.
    ///
    /// Non-leader → [`MultiRaftError::NotLeader`]. Use this for order-status / truth
    /// reads. For Standby offload / debug local reads, use [`Self::read_stale`] or
    /// [`Self::with_fsm`].
    pub async fn read_linearizable<R>(
        &self,
        group: u64,
        f: impl FnOnce(&S) -> R,
    ) -> Result<R, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;

        match raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
            Ok(_read_log_id) => self.with_fsm(group, f).await.ok_or_else(|| {
                MultiRaftError::Other(anyhow::anyhow!(
                    "read_linearizable: fsm missing for group {group}"
                ))
            }),
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Err(MultiRaftError::NotLeader {
                        hint: fwd.leader_id,
                    });
                }
                Err(MultiRaftError::Other(anyhow::anyhow!(
                    "ensure_linearizable: {e}"
                )))
            }
        }
    }

    pub fn is_leader(&self, group: u64) -> bool {
        self.raft(group).map(|r| r.is_leader()).unwrap_or(false)
    }

    pub fn leader(&self, group: u64) -> Option<u64> {
        self.raft(group)
            .and_then(|r| r.metrics().borrow_watched().current_leader)
    }

    /// Register a leader-change callback `(group_id, current_leader)`.
    ///
    /// Best-effort metrics watcher per group (spawned on create_group and for
    /// groups already present when this is called).
    pub fn on_leader_change<F>(&self, cb: F)
    where
        F: Fn(u64, Option<u64>) + Send + Sync + 'static,
    {
        let cb: LeaderCb = Arc::new(cb);
        self.leader_cbs.lock().unwrap().push(cb);

        let groups: Vec<(GroupId, Raft<S>)> = self
            .groups
            .lock()
            .unwrap()
            .iter()
            .map(|(&gid, app)| (gid, app.raft.clone()))
            .collect();

        for (gid, raft) in groups {
            Self::spawn_leader_watch(gid, raft, self.leader_cbs.clone());
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// In-process shared [`Router`] (panics if this node was started with gRPC).
    pub fn router(&self) -> &Router {
        match &self.net {
            NetBackend::InProcess { router, .. } => router,
            NetBackend::Grpc { .. } => {
                panic!("router() is only available for in-process MultiRaft::start / start_cluster")
            }
        }
    }

    /// Distinct peer links: in-process router channels or gRPC peer channels.
    pub fn unique_peer_links(&self) -> usize {
        match &self.net {
            NetBackend::InProcess { router, .. } => router.unique_peer_links(),
            NetBackend::Grpc { router } => router.unique_peer_links(),
        }
    }

    /// Shut down all local Raft groups cleanly (flush / stop core tasks).
    ///
    /// For in-process mode, also unregisters this node from the shared [`Router`]
    /// so peers observe it as unreachable (used by demo admin leader-loss simulation).
    pub async fn shutdown(&self) -> Result<(), MultiRaftError> {
        let rafts: Vec<Raft<S>> = self
            .groups
            .lock()
            .unwrap()
            .values()
            .map(|g| g.raft.clone())
            .collect();
        for raft in rafts {
            raft.shutdown()
                .await
                .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("shutdown: {e}")))?;
        }
        self.groups.lock().unwrap().clear();
        if let NetBackend::InProcess { router, .. } = &self.net {
            let _ = router.unregister_node(self.node_id);
        }
        Ok(())
    }

    /// Wait until the state machine has recovered at least the persisted commit
    /// point after a restart (no-op when the log was empty).
    pub async fn wait_for_recovery(
        &self,
        group: GroupId,
        timeout: Duration,
    ) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        raft.wait_for_recovery(Some(timeout))
            .await
            .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("wait_for_recovery: {e}")))?;
        Ok(())
    }

    /// Inspect the **local** FSM for `group` without leadership confirmation.
    ///
    /// May be stale relative to the cluster. Prefer [`Self::read_linearizable`] for
    /// application truth reads; keep this for tests / metrics / debug.
    /// For Standby service offload with an applied watermark, use [`Self::read_stale`].
    pub async fn with_fsm<R>(&self, group: GroupId, f: impl FnOnce(&S) -> R) -> Option<R> {
        let sm = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.state_machine.clone())?;
        Some(sm.with_fsm(f).await)
    }

    /// Local FSM read for Standby (or other) service offload.
    ///
    /// Requires [`ClusterConfig::enable_stale_queries`]. Returns the value plus this
    /// node's last applied `(index, term)`. **Not** linearizable — callers must
    /// treat the result as eventually consistent / possibly behind the leader.
    pub async fn read_stale<R>(
        &self,
        group: GroupId,
        f: impl FnOnce(&S) -> R,
    ) -> Result<StaleRead<R>, MultiRaftError> {
        if !self.config.enable_stale_queries {
            return Err(MultiRaftError::StaleQueriesDisabled);
        }
        let sm = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.state_machine.clone())
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let (applied_index, applied_term) = self.local_applied(group).await.unwrap_or((0, 0));
        let value = sm.with_fsm(f).await;
        Ok(StaleRead {
            value,
            applied_index,
            applied_term,
        })
    }

    /// Last applied log id for `group` from the state-machine store.
    ///
    /// Prefer this over Raft metrics so out-of-band
    /// [`Self::install_durable_snapshot`] watermarks stay consistent with FSM data.
    pub async fn local_applied(&self, group: GroupId) -> Option<(u64, u64)> {
        let sm = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.state_machine.clone())?;
        sm.last_applied().await
    }

    /// Whether this node accepts [`Self::read_stale`].
    pub fn stale_queries_enabled(&self) -> bool {
        self.config.enable_stale_queries
    }

    fn raft(&self, group: GroupId) -> Option<Raft<S>> {
        self.groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.raft.clone())
    }

    async fn spawn_local_group(&self, group: GroupId) -> Result<(), MultiRaftError> {
        let snapshot_policy = if self.config.snapshot_mode == SnapshotMode::StandbyOffload {
            // Voters/standby never auto hot-snapshot; Standby builds via trigger log.
            openraft::SnapshotPolicy::Never
        } else {
            openraft::SnapshotPolicy::LogsSinceLast(5000)
        };
        let config = Config {
            heartbeat_interval: self.config.heartbeat_interval_ms,
            election_timeout_min: self.config.election_timeout_min_ms,
            election_timeout_max: self.config.election_timeout_max_ms,
            max_in_snapshot_log_to_keep: 0,
            snapshot_policy,
            // Wipe/restart chaos and follower catch-up can present a shorter log
            // than the leader last matched; without this openraft panics.
            allow_log_reversion: Some(true),
            api_batch_linger_ms: self.config.api_batch_linger_ms,
            api_batch_capacity: if self.config.api_batch_capacity == 0 {
                4096
            } else {
                self.config.api_batch_capacity
            },
            max_append_entries: Some(if self.config.max_append_entries == 0 {
                4096
            } else {
                self.config.max_append_entries
            }),
            max_payload_entries: if self.config.max_payload_entries == 0 {
                300
            } else {
                self.config.max_payload_entries
            },
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| MultiRaftError::Other(anyhow::anyhow!(e.to_string())))?,
        );

        let fsm = (self.make_fsm)(group);
        // StandbyOffload: never hot-dump FSM in openraft build_snapshot (voters or standby).
        let allow_hot_build = self.config.snapshot_mode != SnapshotMode::StandbyOffload;

        let sm_holder: Arc<OnceLock<StateMachineStore<S>>> = Arc::new(OnceLock::new());
        let on_standby_trigger = if self.config.role == NodeRole::Standby
            && self.config.snapshot_mode == SnapshotMode::StandbyOffload
        {
            let catalog = self.snapshot_rt.catalog.clone().ok_or_else(|| {
                MultiRaftError::Other(anyhow::anyhow!(
                    "StandbyOffload Standby requires non-empty data_dir for SnapshotCatalog"
                ))
            })?;
            let rt = self.snapshot_rt.clone();
            let holder = sm_holder.clone();
            let trigger: TriggerCb = Arc::new(move |gid, index, term| {
                let catalog = catalog.clone();
                let rt = rt.clone();
                let holder = holder.clone();
                TypeConfig::spawn(async move {
                    let Some(sm) = holder.get() else {
                        tracing::warn!(group = gid, "standby trigger before SM ready");
                        return;
                    };
                    let delay = *rt.serialize_delay.lock().unwrap();
                    match sm
                        .build_standby_snapshot_async(&catalog, gid, index, term, delay)
                        .await
                    {
                        Ok(entry) => {
                            let fetch_url = rt
                                .admin_advertise_addr
                                .map(|addr| format!("http://{addr}/snapshots/{gid}/latest"))
                                .unwrap_or_default();
                            let ad = SnapshotAdvertisement {
                                group: gid,
                                last_index: entry.last_index,
                                last_term: entry.last_term,
                                snapshot_id: entry.snapshot_id,
                                size: entry.size,
                                sha256_hex: entry.sha256_hex,
                                fetch_url,
                            };
                            rt.record_ad(ad);
                        }
                        Err(e) => {
                            tracing::error!(
                                group = gid,
                                index,
                                term,
                                error = %e,
                                "standby async snapshot failed"
                            );
                        }
                    }
                });
            });
            Some(trigger)
        } else {
            None
        };

        let state_machine_store = StateMachineStore::with_options(
            group,
            fsm,
            SmOptions {
                allow_hot_build,
                catalog: self.snapshot_rt.catalog.clone(),
                on_standby_trigger,
            },
        );
        let _ = sm_holder.set(state_machine_store.clone());

        let raft = match &self.net {
            NetBackend::InProcess { router, .. } => {
                let network = NetworkFactory::new(router.clone(), group);
                if self.config.data_dir.as_os_str().is_empty() {
                    let log_store = MemLogStore::default();
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                } else {
                    let dir = self.config.data_dir.join(format!("group-{group}"));
                    let log_store = FileLogStoreOf::open_with_full_options(
                        &dir,
                        self.config.file_log_coalesce_us,
                        self.config.file_log_sync_level,
                        FileLogStreamOptions {
                            stream_buf_bytes: self.config.file_log_stream_buf_bytes,
                            stream_flush_ms: self.config.file_log_stream_flush_ms,
                            hold_overlap: self.config.file_log_hold_overlap,
                        },
                    )
                    .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("open file log: {e}")))?;
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                }
            }
            NetBackend::Grpc { router } => {
                let network = GrpcNetworkFactory::new(router.clone(), group);
                if self.config.data_dir.as_os_str().is_empty() {
                    let log_store = MemLogStore::default();
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                } else {
                    let dir = self.config.data_dir.join(format!("group-{group}"));
                    let log_store = FileLogStoreOf::open_with_full_options(
                        &dir,
                        self.config.file_log_coalesce_us,
                        self.config.file_log_sync_level,
                        FileLogStreamOptions {
                            stream_buf_bytes: self.config.file_log_stream_buf_bytes,
                            stream_flush_ms: self.config.file_log_stream_flush_ms,
                            hold_overlap: self.config.file_log_hold_overlap,
                        },
                    )
                    .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("open file log: {e}")))?;
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                }
            }
        }
        .map_err(|e| MultiRaftError::Other(anyhow::anyhow!(e.to_string())))?;

        {
            let mut g = self.groups.lock().unwrap();
            g.insert(
                group,
                GroupApp {
                    node_id: self.node_id,
                    group_id: group,
                    raft: raft.clone(),
                    state_machine: state_machine_store,
                },
            );
        }

        Self::spawn_leader_watch(group, raft.clone(), self.leader_cbs.clone());
        Self::spawn_standby_throttle_watch(raft, self.standby_throttle.clone());
        Ok(())
    }

    fn membership_nodes(&self, members: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
        let mut nodes = BTreeMap::new();
        for &id in members {
            let addr = self
                .config
                .peers
                .iter()
                .find(|(n, _)| *n == id)
                .map(|(_, a)| a.to_string())
                .unwrap_or_default();
            nodes.insert(id, BasicNode { addr });
        }
        nodes
    }

    fn spawn_leader_watch(group: GroupId, raft: Raft<S>, cbs: Arc<Mutex<Vec<LeaderCb>>>) {
        TypeConfig::spawn(async move {
            let mut rx = raft.metrics();
            let mut last: Option<Option<NodeId>> = None;
            loop {
                let cur = rx.borrow_watched().current_leader;
                if last.as_ref() != Some(&cur) {
                    last = Some(cur);
                    let callbacks: Vec<LeaderCb> = cbs.lock().unwrap().clone();
                    for cb in callbacks {
                        cb(group, cur);
                    }
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }

    /// Keep standby throttle in sync with committed learner membership so every
    /// potential leader throttles dynamically added Standbys after failover.
    fn spawn_standby_throttle_watch(raft: Raft<S>, throttle: StandbyThrottle) {
        TypeConfig::spawn(async move {
            let mut rx = raft.metrics();
            let mut last_learners: Option<BTreeSet<NodeId>> = None;
            loop {
                let membership = rx.borrow_watched().membership_config.membership().clone();
                let learners: BTreeSet<NodeId> = membership.learner_ids().collect();
                if last_learners.as_ref() != Some(&learners) {
                    let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
                    for &id in &learners {
                        throttle.insert(id);
                    }
                    for id in throttle.standby_ids() {
                        if voters.contains(&id) {
                            throttle.remove(id);
                        }
                    }
                    last_learners = Some(learners);
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }
}

/// Wait until any handle reports a leader for `group` (test helper).
pub async fn wait_for_leader(
    nodes: &[MultiRaft],
    group: GroupId,
    timeout: Duration,
) -> Option<NodeId> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for n in nodes {
            if let Some(leader) = n.leader(group) {
                if nodes.iter().any(|x| x.is_leader(group)) {
                    return Some(leader);
                }
            }
        }
        TypeConfig::sleep(Duration::from_millis(50)).await;
    }
    None
}

async fn daisy_sync_once<S: StateMachine>(
    base: &str,
    group: GroupId,
    chunk: usize,
    temp_dir: &std::path::Path,
    snapshot_rt: &SnapshotRuntime,
    groups: &GroupMap<S>,
    admin: Option<std::net::SocketAddr>,
) -> Result<RecoverOutcome, MultiRaftError> {
    let url = format!("{}/snapshots/{group}/latest", base.trim_end_matches('/'));
    let fetched = match pull_snapshot_chunked(&url, chunk, temp_dir).await {
        Ok(f) => f,
        Err(e) => {
            return Ok(RecoverOutcome::FetchFailed {
                error: e.to_string(),
            })
        }
    };

    let sm = groups
        .lock()
        .unwrap()
        .get(&group)
        .map(|g| g.state_machine.clone());
    let (local_index, local_term) = match &sm {
        Some(sm) => sm.last_applied().await.unwrap_or((0, 0)),
        None => (0, 0),
    };
    if !log_pos_newer(
        fetched.last_term,
        fetched.last_index,
        local_term,
        local_index,
    ) {
        return Ok(RecoverOutcome::SkippedNotNewer {
            local_index,
            ad_index: fetched.last_index,
        });
    }

    let snapshot_id = fetched
        .snapshot_id
        .clone()
        .unwrap_or_else(|| format!("{}-{}", fetched.last_index, fetched.last_term));
    let catalog = snapshot_rt.catalog.as_ref().ok_or_else(|| {
        MultiRaftError::Other(anyhow::anyhow!(
            "daisy sync: SnapshotCatalog required (StandbyOffload + data_dir)"
        ))
    })?;
    catalog
        .write(
            group,
            fetched.last_index,
            fetched.last_term,
            &snapshot_id,
            &fetched.data,
        )
        .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("daisy catalog write: {e}")))?;

    if let Some(sm) = sm {
        sm.install_durable_snapshot(
            fetched.last_index,
            fetched.last_term,
            true,
            fetched.data.clone(),
        )
        .await
        .map_err(|e| MultiRaftError::Other(anyhow::anyhow!("daisy install: {e}")))?;
    }

    let fetch_url = admin
        .map(|addr| format!("http://{addr}/snapshots/{group}/latest"))
        .unwrap_or_default();
    snapshot_rt.record_ad(SnapshotAdvertisement {
        group,
        last_index: fetched.last_index,
        last_term: fetched.last_term,
        snapshot_id,
        size: fetched.data.len() as u64,
        sha256_hex: fetched.sha256_hex,
        fetch_url,
    });
    Ok(RecoverOutcome::Installed {
        last_index: fetched.last_index,
        last_term: fetched.last_term,
    })
}

const MEMBERSHIP_RETRY_TIMEOUT: Duration = Duration::from_secs(15);
const MEMBERSHIP_RETRY_INTERVAL: Duration = Duration::from_millis(50);

fn transient_membership_err(label: &str, e: &impl std::fmt::Display) -> MultiRaftError {
    MultiRaftError::Other(anyhow::anyhow!(
        "{label}: {e} (retry after membership settles)"
    ))
}

fn membership_err_retryable(err: &MultiRaftError) -> bool {
    matches!(err, MultiRaftError::Other(e) if e.to_string().contains("configuration change"))
}

async fn retry_on_membership_pending<F, Fut>(mut op: F) -> Result<(), MultiRaftError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), MultiRaftError>>,
{
    let deadline = std::time::Instant::now() + MEMBERSHIP_RETRY_TIMEOUT;
    loop {
        match op().await {
            Ok(()) => return Ok(()),
            Err(e) if membership_err_retryable(&e) => {
                if std::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tokio::time::sleep(MEMBERSHIP_RETRY_INTERVAL).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// True when `(remote_term, remote_index)` is strictly newer than local.
fn log_pos_newer(remote_term: u64, remote_index: u64, local_term: u64, local_index: u64) -> bool {
    (remote_term, remote_index) > (local_term, local_index)
}
