//! Optional lag-based auto promote loop (A9 extension — off unless configured).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_core::GroupId;
use multiraft_core::MultiRaftError;
use multiraft_core::NodeId;
use multiraft_core::PluginRegistry;
use multiraft_core::TransitionAction;
use multiraft_core::TransitionContext;
use multiraft_core::TypeConfig;
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;
use openraft::async_runtime::WatchReceiver;
use openraft::ChangeMembers;
use openraft::type_config::TypeConfigExt;

use crate::node::GroupMap;
use crate::standby_throttle::StandbyThrottle;

pub(crate) struct TransitionLoopCtx<S: StateMachine> {
    pub plugins: Arc<PluginRegistry>,
    pub groups: GroupMap<S>,
    pub config: ClusterConfig,
    pub standby_throttle: StandbyThrottle,
}

pub(crate) async fn transition_tick_once<S: StateMachine>(
    ctx: &TransitionLoopCtx<S>,
    group: GroupId,
) -> Result<(), MultiRaftError> {
    if ctx.plugins.transition_plugins().is_empty() {
        return Ok(());
    }
    if !ctx
        .plugins
        .transition_snapshots()
        .iter()
        .any(|s| s.enabled)
    {
        return Ok(());
    }

    let raft = {
        let groups = ctx.groups.lock().unwrap();
        groups.get(&group).map(|g| g.raft.clone())
    };
    let Some(raft) = raft else {
        return Ok(());
    };
    if !raft.is_leader() {
        return Ok(());
    }

    let leader_applied = local_applied_index(&ctx.groups, group).await.unwrap_or(0);
    let rx = raft.metrics();
    let metrics = rx.borrow_watched().clone();
    let replication = metrics.replication.clone();
    let membership = metrics.membership_config.membership().clone();
    let learner_ids: Vec<NodeId> = membership.learner_ids().collect();

    let standby_ids: Vec<NodeId> = if ctx.config.standby_node_ids.is_empty() {
        learner_ids
    } else {
        ctx.config
            .standby_node_ids
            .iter()
            .copied()
            .filter(|id| learner_ids.contains(id))
            .collect()
    };

    for standby_id in standby_ids {
        let standby_applied = replication
            .as_ref()
            .and_then(|r| r.get(&standby_id))
            .and_then(|log_id| log_id.as_ref())
            .map(|id| id.index())
            .unwrap_or(0);
        let ctx_eval = TransitionContext {
            group,
            leader_applied_index: leader_applied,
            standby_id,
            standby_applied_index: standby_applied,
            standby_is_learner: true,
        };
        let mut promote = false;
        for plugin in ctx.plugins.transition_plugins() {
            if plugin.evaluate(&ctx_eval) == TransitionAction::PromoteStandby {
                promote = true;
                break;
            }
        }
        if promote {
            promote_standby_once(&raft, &ctx.standby_throttle, standby_id).await?;
        }
    }
    Ok(())
}

async fn local_applied_index<S: StateMachine>(
    groups: &GroupMap<S>,
    group: GroupId,
) -> Option<u64> {
    let sm = {
        let g = groups.lock().unwrap();
        g.get(&group).map(|app| app.state_machine.clone())
    }?;
    sm.last_applied().await.map(|(idx, _)| idx)
}

async fn promote_standby_once<S: StateMachine>(
    raft: &Raft<S>,
    standby_throttle: &StandbyThrottle,
    node_id: NodeId,
) -> Result<(), MultiRaftError> {
    let mut add = BTreeSet::new();
    add.insert(node_id);
    match raft
        .change_membership(ChangeMembers::AddVoterIds(add), true)
        .await
    {
        Ok(_) => {
            standby_throttle.remove(node_id);
            tracing::info!(node_id, "transition loop promoted standby to voter");
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
                tracing::debug!(node_id, "transition promote skipped: membership pending");
                return Ok(());
            }
            Err(MultiRaftError::Other(anyhow::anyhow!(
                "transition promote change_membership: {e}"
            )))
        }
    }
}

pub(crate) fn spawn_transition_loop<S: StateMachine + 'static>(
    ctx: TransitionLoopCtx<S>,
    groups: Vec<GroupId>,
) {
    let interval_ms = ctx.config.transition_poll_interval_ms;
    if interval_ms == 0 {
        return;
    }
    if ctx.plugins.transition_plugins().is_empty() {
        return;
    }
    if !ctx
        .plugins
        .transition_snapshots()
        .iter()
        .any(|s| s.enabled)
    {
        return;
    }
    let interval = Duration::from_millis(interval_ms.max(100));
    TypeConfig::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            for &group in &groups {
                if let Err(e) = transition_tick_once(&ctx, group).await {
                    tracing::debug!(group, error = %e, "transition tick");
                }
            }
        }
    });
}
