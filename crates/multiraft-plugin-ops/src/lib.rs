//! PremiumClusterTool-style route catalog + default plugin bundle.

use std::sync::Arc;

use multiraft_core::PluginRegistry;
use multiraft_plugin_auth::BearerTokenAuth;
use multiraft_plugin_metrics::SegmentedLatencyMetrics;
use multiraft_plugin_transition::LagPromotePolicy;

/// Admin routes exposed by `multiraft-demo` (Premium ops subset).
pub const ADMIN_ROUTES: &[&str] = &[
    "GET /metrics/propose-stages",
    "GET /metrics/links",
    "GET /admin/groups/{group}/status",
    "GET /admin/catalog/{group}",
    "GET /admin/archive/{group}/positions",
    "GET /admin/archive/{group}/export/{index}/{term}",
    "GET /admin/archive/{group}/export/{index}/{term}/data",
    "GET /admin/best_snapshot_ad/{group}",
    "POST /admin/replicate_standby_snapshot/{group}",
    "POST /admin/promote_standby/{group}/{id}",
    "POST /admin/demote_standby/{group}/{id}",
    "POST /admin/daisy_sync/{group}",
    "POST /admin/add_standby/{group}/{standby_id}",
    "GET /groups/{id}/stale",
];

/// Build the default Premium plugin set (metrics + transition + auth).
/// Pass `catalog` into [`multiraft_plugin_archive::CatalogArchive`] separately when
/// StandbyOffload is enabled.
pub fn premium_registry(
    auth_token: Option<&str>,
    transition_lag_threshold: Option<u64>,
) -> PluginRegistry {
    let auth = match auth_token {
        Some(t) => BearerTokenAuth::new(t),
        None => BearerTokenAuth::from_env(),
    };
    let transition = match transition_lag_threshold {
        Some(t) if t > 0 => LagPromotePolicy::enabled(t),
        _ => LagPromotePolicy::disabled(),
    };
    PluginRegistry::new()
        .register_metrics(Arc::new(SegmentedLatencyMetrics::new()))
        .register_transition(Arc::new(transition))
        .register_auth(Arc::new(auth))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn premium_registry_has_metrics_and_auth() {
        let reg = premium_registry(Some("lab"), None);
        assert!(!reg.is_empty());
        assert!(reg.authorize_admin(Some("Bearer lab")));
        assert!(!reg.authorize_admin(Some("Bearer wrong")));
    }
}
