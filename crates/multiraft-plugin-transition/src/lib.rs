//! Optional lag-based promote policy (A9 automation — off by default).

use std::sync::Arc;
use std::sync::Mutex;

use multiraft_core::Plugin;
use multiraft_core::TransitionAction;
use multiraft_core::TransitionContext;
use multiraft_core::TransitionPlugin;
use multiraft_core::TransitionSnapshot;

#[derive(Clone, Debug)]
pub struct LagPromotePolicy {
    enabled: bool,
    lag_threshold: u64,
    last_action: Arc<Mutex<TransitionAction>>,
}

impl LagPromotePolicy {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            lag_threshold: 0,
            last_action: Arc::new(Mutex::new(TransitionAction::None)),
        }
    }

    pub fn enabled(lag_threshold: u64) -> Self {
        Self {
            enabled: true,
            lag_threshold: lag_threshold.max(1),
            last_action: Arc::new(Mutex::new(TransitionAction::None)),
        }
    }
}

impl Plugin for LagPromotePolicy {
    fn name(&self) -> &'static str {
        "lag-promote"
    }
}

impl TransitionPlugin for LagPromotePolicy {
    fn evaluate(&self, ctx: &TransitionContext) -> TransitionAction {
        let action = if !self.enabled {
            TransitionAction::None
        } else if !ctx.standby_is_learner {
            TransitionAction::None
        } else {
            let lag = ctx.leader_applied_index.saturating_sub(ctx.standby_applied_index);
            if lag >= self.lag_threshold {
                TransitionAction::PromoteStandby
            } else {
                TransitionAction::None
            }
        };
        *self.last_action.lock().unwrap() = action;
        action
    }

    fn snapshot(&self) -> TransitionSnapshot {
        TransitionSnapshot {
            plugin: self.name(),
            enabled: self.enabled,
            lag_threshold: self.lag_threshold,
            last_action: *self.last_action.lock().unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_never_promotes() {
        let p = LagPromotePolicy::disabled();
        let action = p.evaluate(&TransitionContext {
            group: 0,
            leader_applied_index: 100,
            standby_id: 4,
            standby_applied_index: 0,
            standby_is_learner: true,
        });
        assert_eq!(action, TransitionAction::None);
    }

    #[test]
    fn promotes_when_lag_exceeds_threshold() {
        let p = LagPromotePolicy::enabled(50);
        let action = p.evaluate(&TransitionContext {
            group: 0,
            leader_applied_index: 100,
            standby_id: 4,
            standby_applied_index: 40,
            standby_is_learner: true,
        });
        assert_eq!(action, TransitionAction::PromoteStandby);
    }
}
