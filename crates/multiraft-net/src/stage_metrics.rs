//! Map openraft runtime log-stage timings into [`ProposeStage`] samples (N2b).

use std::time::Duration;

use multiraft_core::GroupId;
use multiraft_core::PluginRegistry;
use multiraft_core::ProposeStage;
use multiraft_core::StageSample;
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;
use openraft::stats::LogStages;
use openraft::Instant;

const PROPOSED: usize = 0;
const RECEIVED: usize = 1;
const SUBMITTED: usize = 2;
const PERSISTED: usize = 3;
const COMMITTED: usize = 4;
const APPLIED: usize = 5;

struct StageDurations {
    enqueued: Duration,
    leader_append: Duration,
    quorum_ack: Duration,
    committed: Duration,
    applied: Duration,
}

fn durations_for_index<I>(log_stage: &LogStages<I>, index: u64) -> Option<StageDurations>
where
    I: Instant,
{
    for seg in log_stage.segments() {
        if seg.range.start <= index && index < seg.range.end {
            let v = &seg.values;
            let enqueued = v[RECEIVED].saturating_duration_since(v[PROPOSED]);
            let leader_append = v[SUBMITTED].saturating_duration_since(v[RECEIVED])
                + v[PERSISTED].saturating_duration_since(v[SUBMITTED]);
            let quorum_ack = v[COMMITTED].saturating_duration_since(v[PERSISTED]);
            let committed = quorum_ack;
            let applied = v[APPLIED].saturating_duration_since(v[COMMITTED]);
            return Some(StageDurations {
                enqueued,
                leader_append,
                quorum_ack,
                committed,
                applied,
            });
        }
    }
    None
}

fn record(plugins: &PluginRegistry, group: GroupId, stage: ProposeStage, duration: Duration) {
    if duration.is_zero() {
        return;
    }
    plugins.record_stage(StageSample {
        group,
        stage,
        duration,
        ok: true,
    });
}

/// After a successful `client_write`, pull openraft runtime stats and record sub-stages.
pub async fn record_openraft_stages<S: StateMachine>(
    plugins: &PluginRegistry,
    group: GroupId,
    index: u64,
    raft: &Raft<S>,
) {
    let Ok(stats) = raft.runtime_stats().await else {
        return;
    };
    let Some(d) = durations_for_index(&stats.log_stage, index) else {
        return;
    };
    record(plugins, group, ProposeStage::ClientEnqueued, d.enqueued);
    record(plugins, group, ProposeStage::LeaderAppend, d.leader_append);
    record(plugins, group, ProposeStage::QuorumAck, d.quorum_ack);
    record(plugins, group, ProposeStage::Committed, d.committed);
    record(plugins, group, ProposeStage::Applied, d.applied);
}
