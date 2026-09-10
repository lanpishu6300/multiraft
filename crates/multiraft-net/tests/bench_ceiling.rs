//! Ceiling / phase breakdown for propose path.
//!
//! ```bash
//! cargo test -p multiraft-net --test bench_ceiling --release -- --nocapture
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use multiraft_core::ClusterConfig;
use multiraft_core::TypeConfig;
use multiraft_fsm::CounterFsm;
use multiraft_fsm::StateMachine;
use multiraft_net::MultiRaft;
use multiraft_net::decode;
use multiraft_net::encode;
use multiraft_net::wait_for_leader;
use multiraft_store::MemLogStore;
use multiraft_store::Request;
use multiraft_store::StateMachineStore;
use multiraft_store::StubNetworkFactory;
use openraft::BasicNode;
use openraft::Config;
use openraft::raft::AppendEntriesRequest;
use openraft::type_config::TypeConfigExt;
use serde_json::json;

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

fn report(name: &str, n: u64, wall: Duration, lats: &mut [u64]) {
    lats.sort_unstable();
    let ms = wall.as_secs_f64() * 1000.0;
    let tps = (n as f64) * 1000.0 / ms.max(0.001);
    println!(
        "{}",
        json!({
            "phase": name,
            "ops": n,
            "wall_ms": ms,
            "tps": tps,
            "latency_us_p50": pct(lats, 0.50),
            "latency_us_p95": pct(lats, 0.95),
            "latency_us_p99": pct(lats, 0.99),
        })
    );
}

#[test]
fn ceiling_phases() {
    TypeConfig::run(async {
        let ops = 3_000u64;

        // --- A: FSM-only (no Raft) ---
        {
            let mut fsm = CounterFsm::new();
            let mut lats = Vec::with_capacity(ops as usize);
            let t0 = Instant::now();
            for i in 1..=ops {
                let t = Instant::now();
                let data = CounterFsm::encode_add(1, i);
                fsm.apply(0, i, &data).unwrap();
                lats.push(t.elapsed().as_micros() as u64);
            }
            report("A_fsm_only", ops, t0.elapsed(), &mut lats);
        }

        // --- B: 1-node Raft memory (no replication) ---
        {
            let config = Arc::new(
                Config {
                    heartbeat_interval: 500,
                    election_timeout_min: 1500,
                    election_timeout_max: 3000,
                    max_in_snapshot_log_to_keep: 0,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            );
            let log = MemLogStore::default();
            let sm = StateMachineStore::new(0, CounterFsm::new());
            let raft = openraft::Raft::new(
                1,
                config,
                StubNetworkFactory,
                log,
                sm.clone(),
            )
            .await
            .unwrap();
            let mut nodes = BTreeMap::new();
            nodes.insert(1u64, BasicNode { addr: String::new() });
            raft.initialize(nodes).await.unwrap();
            TypeConfig::sleep(Duration::from_millis(200)).await;

            let mut lats = Vec::with_capacity(ops as usize);
            let t0 = Instant::now();
            for i in 1..=ops {
                let t = Instant::now();
                let req = Request::new(CounterFsm::encode_add(1, i));
                raft.client_write(req).await.unwrap();
                lats.push(t.elapsed().as_micros() as u64);
            }
            report("B_raft_1node_mem", ops, t0.elapsed(), &mut lats);
            raft.shutdown().await.unwrap();
        }

        // --- C: 3-node in-process memory ---
        {
            let peers = [1u64, 2, 3];
            let configs: Vec<_> = peers
                .iter()
                .map(|&id| {
                    let mut c = ClusterConfig::for_test(id, &peers);
                    c.heartbeat_interval_ms = 50;
                    c.election_timeout_min_ms = 150;
                    c.election_timeout_max_ms = 300;
                    c
                })
                .collect();
            let nodes = MultiRaft::start_cluster(configs).await.unwrap();
            for n in &nodes {
                n.create_group(0, &peers).await.unwrap();
            }
            wait_for_leader(&nodes, 0, Duration::from_secs(5))
                .await
                .expect("leader");

            let mut lats = Vec::with_capacity(ops as usize);
            let t0 = Instant::now();
            for i in 1..=ops {
                let t = Instant::now();
                let data = CounterFsm::encode_add(1, i);
                loop {
                    let mut ok = false;
                    for n in &nodes {
                        if n.is_leader(0) && n.propose(0, data.clone()).await.is_ok() {
                            ok = true;
                            break;
                        }
                    }
                    if ok {
                        break;
                    }
                    TypeConfig::sleep(Duration::from_millis(1)).await;
                }
                lats.push(t.elapsed().as_micros() as u64);
            }
            report("C_raft_3node_mem", ops, t0.elapsed(), &mut lats);
        }

        // --- D: encode/decode roundtrip (production bincode codec) ---
        {
            #[derive(serde::Serialize, serde::Deserialize)]
            struct FakeAe {
                term: u64,
                entries: Vec<(u64, Vec<u8>)>,
            }
            let fake = FakeAe {
                term: 1,
                entries: (1..=8)
                    .map(|i| (i, CounterFsm::encode_add(1, i)))
                    .collect(),
            };
            let mut lats = Vec::with_capacity(ops as usize);
            let t0 = Instant::now();
            for _ in 1..=ops {
                let t = Instant::now();
                let bytes = encode(&fake).expect("encode");
                let _: FakeAe = decode(&bytes).expect("decode");
                lats.push(t.elapsed().as_micros() as u64);
            }
            report("D_codec_roundtrip", ops, t0.elapsed(), &mut lats);
        }

        // --- E: theoretical notes printed for humans ---
        println!(
            "{}",
            json!({
                "note": "ceilings",
                "A": "FSM-only soft ceiling (no consensus)",
                "B": "single-node Raft mem (persist+apply, no quorum RPC)",
                "C": "3-node in-process (quorum + bincode RPC + apply)",
                "gap_C_vs_B": "replication/quorum wait (codec negligible)",
                "gap_B_vs_A": "Raft log + openraft scheduling",
            })
        );
    });
}

// Silence unused import if AppendEntriesRequest unused — keep for future typed AE bench.
#[allow(dead_code)]
fn _ae_type_hint() -> Option<AppendEntriesRequest<multiraft_core::TypeConfig>> {
    None
}
