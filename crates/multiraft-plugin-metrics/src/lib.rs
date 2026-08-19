//! N2b segmented propose latency — first multiraft plugin.
//!
//! Records [`StageSample`]s from `multiraft-net` and exports JSON summaries
//! (`p50` / `p99` over a bounded recent window per group + stage).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use multiraft_core::GroupId;
use multiraft_core::MetricsPlugin;
use multiraft_core::MetricsSnapshot;
use multiraft_core::Plugin;
use multiraft_core::PluginContext;
use multiraft_core::ProposeStage;
use multiraft_core::StageLatencySummary;
use multiraft_core::StageSample;

const RECENT_CAP: usize = 4096;

#[derive(Default)]
struct StageAgg {
    count: u64,
    ok_count: u64,
    sum_us: u64,
    max_us: u64,
    recent_us: Vec<u64>,
}

impl StageAgg {
    fn record(&mut self, duration: Duration, ok: bool) {
        let us = duration.as_micros().min(u64::MAX as u128) as u64;
        self.count += 1;
        if ok {
            self.ok_count += 1;
        }
        self.sum_us = self.sum_us.saturating_add(us);
        self.max_us = self.max_us.max(us);
        if self.recent_us.len() >= RECENT_CAP {
            self.recent_us.remove(0);
        }
        self.recent_us.push(us);
    }

    fn summary(&self, group: GroupId, stage: ProposeStage) -> StageLatencySummary {
        let mut sorted = self.recent_us.clone();
        sorted.sort_unstable();
        let p50_us = percentile(&sorted, 0.50);
        let p99_us = percentile(&sorted, 0.99);
        StageLatencySummary {
            group,
            stage,
            count: self.count,
            ok_count: self.ok_count,
            sum_us: self.sum_us,
            max_us: self.max_us,
            p50_us,
            p99_us,
        }
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// In-process segmented latency aggregator.
#[derive(Default)]
pub struct SegmentedLatencyMetrics {
    stages: Mutex<BTreeMap<(GroupId, ProposeStage), StageAgg>>,
}

impl SegmentedLatencyMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(&self.snapshot())
    }
}

impl Plugin for SegmentedLatencyMetrics {
    fn name(&self) -> &'static str {
        "segmented-latency"
    }

    fn on_node_stop(&self, _ctx: &PluginContext) {
        let mut guard = self.stages.lock().unwrap();
        guard.clear();
    }
}

impl MetricsPlugin for SegmentedLatencyMetrics {
    fn record_stage(&self, sample: StageSample) {
        let mut guard = self.stages.lock().unwrap();
        guard
            .entry((sample.group, sample.stage))
            .or_default()
            .record(sample.duration, sample.ok);
    }

    fn snapshot(&self) -> MetricsSnapshot {
        let guard = self.stages.lock().unwrap();
        let stages = guard
            .iter()
            .map(|((group, stage), agg)| agg.summary(*group, *stage))
            .collect();
        MetricsSnapshot {
            plugin: self.name(),
            stages,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_snapshot_percentiles() {
        let m = SegmentedLatencyMetrics::new();
        for us in [100u64, 200, 300, 400, 500] {
            m.record_stage(StageSample {
                group: 0,
                stage: ProposeStage::ClientWrite,
                duration: Duration::from_micros(us),
                ok: true,
            });
        }
        let snap = m.snapshot();
        assert_eq!(snap.plugin, "segmented-latency");
        assert_eq!(snap.stages.len(), 1);
        let s = &snap.stages[0];
        assert_eq!(s.count, 5);
        assert_eq!(s.ok_count, 5);
        assert_eq!(s.p50_us, 300);
        assert_eq!(s.p99_us, 500);
    }
}
