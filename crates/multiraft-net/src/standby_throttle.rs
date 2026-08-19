//! Throttle outbound Raft RPCs toward Standby (learner) peers.
//!
//! Does not affect voter↔voter replication: only targets in the standby set
//! are delayed / gated.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use multiraft_core::ClusterConfig;
use multiraft_core::NodeId;

/// Shared standby peer set + soft replication limits.
#[derive(Clone, Debug)]
pub struct StandbyThrottle {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    standby_ids: Mutex<HashSet<NodeId>>,
    /// Fast path for voter-only clusters (no HashSet lock on every RPC).
    standby_count: AtomicUsize,
    delay_ms: AtomicU64,
    max_inflight: AtomicU32,
    semaphores: Mutex<HashMap<NodeId, Arc<Semaphore>>>,
}

impl Default for StandbyThrottle {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                standby_ids: Mutex::new(HashSet::new()),
                standby_count: AtomicUsize::new(0),
                delay_ms: AtomicU64::new(0),
                max_inflight: AtomicU32::new(8),
                semaphores: Mutex::new(HashMap::new()),
            }),
        }
    }
}

impl StandbyThrottle {
    /// Build from [`ClusterConfig`] seed fields.
    pub fn from_config(config: &ClusterConfig) -> Self {
        let t = Self::default();
        t.apply_config(config);
        t
    }

    /// Update delay / max-inflight / seed ids from config (keeps existing runtime ids).
    pub fn apply_config(&self, config: &ClusterConfig) {
        self.inner
            .delay_ms
            .store(config.standby_replicate_delay_ms, Ordering::Relaxed);
        let max = if config.standby_max_inflight == 0 {
            8
        } else {
            config.standby_max_inflight
        };
        self.inner.max_inflight.store(max, Ordering::Relaxed);
        {
            let mut ids = self.inner.standby_ids.lock().unwrap();
            for &id in &config.standby_node_ids {
                ids.insert(id);
            }
            self.inner.standby_count.store(ids.len(), Ordering::Relaxed);
        }
        // Reset per-target semaphores so capacity matches new max.
        self.inner.semaphores.lock().unwrap().clear();
    }

    pub fn insert(&self, id: NodeId) {
        let mut ids = self.inner.standby_ids.lock().unwrap();
        if ids.insert(id) {
            self.inner.standby_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn remove(&self, id: NodeId) {
        let mut ids = self.inner.standby_ids.lock().unwrap();
        if ids.remove(&id) {
            self.inner.standby_count.fetch_sub(1, Ordering::Relaxed);
        }
        self.inner.semaphores.lock().unwrap().remove(&id);
    }

    pub fn contains(&self, id: NodeId) -> bool {
        if self.inner.standby_count.load(Ordering::Relaxed) == 0 {
            return false;
        }
        self.inner.standby_ids.lock().unwrap().contains(&id)
    }

    pub fn standby_ids(&self) -> HashSet<NodeId> {
        self.inner.standby_ids.lock().unwrap().clone()
    }

    /// If `target` is a standby peer: sleep `delay_ms`, then acquire an inflight permit.
    ///
    /// Returns a permit that must be held until the RPC completes. `None` when the
    /// target is not a standby (no throttle).
    pub async fn before_send(&self, target: NodeId) -> Option<OwnedSemaphorePermit> {
        // Hot path: no standbys configured → zero locks / no await.
        if self.inner.standby_count.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let is_standby = {
            let ids = self.inner.standby_ids.lock().unwrap();
            ids.contains(&target)
        };
        if !is_standby {
            return None;
        }
        let delay = self.inner.delay_ms.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let max = self.inner.max_inflight.load(Ordering::Relaxed).max(1) as usize;
        let sem = {
            let mut map = self.inner.semaphores.lock().unwrap();
            map.entry(target)
                .or_insert_with(|| Arc::new(Semaphore::new(max)))
                .clone()
        };
        match sem.acquire_owned().await {
            Ok(p) => Some(p),
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_seeds_ids_and_knobs() {
        let mut cfg = ClusterConfig::for_test(1, &[1, 2, 3, 4]);
        cfg.standby_node_ids = vec![4];
        cfg.standby_replicate_delay_ms = 25;
        cfg.standby_max_inflight = 3;
        let t = StandbyThrottle::from_config(&cfg);
        assert!(t.contains(4));
        assert!(!t.contains(2));
        assert_eq!(t.inner.delay_ms.load(Ordering::Relaxed), 25);
        assert_eq!(t.inner.max_inflight.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn insert_remove_updates_set() {
        let t = StandbyThrottle::default();
        t.insert(9);
        assert!(t.contains(9));
        t.remove(9);
        assert!(!t.contains(9));
        assert!(t.standby_ids().is_empty());
    }

    #[tokio::test]
    async fn before_send_none_for_voter() {
        let t = StandbyThrottle::default();
        t.insert(4);
        assert!(t.before_send(2).await.is_none());
        let permit = t.before_send(4).await;
        assert!(permit.is_some());
    }

    #[tokio::test]
    async fn max_inflight_gates_standby() {
        let mut cfg = ClusterConfig::for_test(1, &[1, 2, 3, 4]);
        cfg.standby_node_ids = vec![4];
        cfg.standby_max_inflight = 1;
        cfg.standby_replicate_delay_ms = 0;
        let t = StandbyThrottle::from_config(&cfg);
        let p1 = t.before_send(4).await.expect("first permit");
        let second = tokio::time::timeout(Duration::from_millis(50), t.before_send(4)).await;
        assert!(
            second.is_err(),
            "second acquire should block while first held"
        );
        drop(p1);
        let p2 = tokio::time::timeout(Duration::from_millis(200), t.before_send(4))
            .await
            .expect("timeout")
            .expect("second permit after release");
        drop(p2);
    }
}
