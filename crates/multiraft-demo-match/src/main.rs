//! match-rust 撮合引擎 × multiraft 整合 demo（multiraft README 二期："可插拔撮合 FSM"）。
//!
//! 验证四件事：
//! 1. **跨节点复制一致性**：每 symbol 一个 Raft Group，leader propose 撮合命令，
//!    全部节点的 MatchFsm（HpEngine）对同一命令序列确定性重放 → 状态逐节点一致。
//! 2. **吞吐 / 延迟**：单 leader 顺序 propose，测 mem（sync=0）/ Os（sync=1）/ All（sync=2）三档。
//! 3. **故障转移**：shutdown 当前 leader，等新 leader，续 propose，状态仍一致。
//! 4. **快照恢复路径**：`MultiRaft::shutdown` 前触发 `with_fsm` 读 summary；恢复由 openraft 负责
//!    （一期 MatchFsm 快照 = 命令累积重放，见 crates/match-raft-fsm）。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use clap::Parser;
use tracing::info;
use match_core_hp::{HpCommand, Side};
use match_raft_fsm::{encode_command, MatchFsm, MatchSummary};
use multiraft_core::{ClusterConfig, FileLogSyncLevel, NodeRole, SnapshotMode};
use multiraft_net::{wait_for_leader, MultiRaft};

#[derive(Parser)]
#[command(name = "multiraft-demo-match", about = "multiraft × HpEngine 撮合 FSM 整合 demo")]
struct Args {
    /// 节点数（voter）。
    #[arg(long, default_value_t = 3)]
    nodes: u64,
    /// Raft group 数（= symbol 数，每交易对一个 group）。
    #[arg(long, default_value_t = 4)]
    groups: u64,
    /// 每 group 订单数。
    #[arg(long, default_value_t = 20000)]
    orders: u64,
    /// 0=内存 log；1=Os(page-cache, Aeron 0)；2=All(fsync, Aeron 2)。
    #[arg(long, default_value_t = 0)]
    sync: u8,
    /// file log group-commit 窗口（µs；sync>0 时生效）。
    #[arg(long, default_value_t = 0)]
    coalesce_us: u64,
    /// peer 起始端口（进程内共享 Router，端口仅作占位）。
    #[arg(long, default_value_t = 21300)]
    port: u16,
    /// 数据目录（sync>0 时必填）。
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// 深流水线宽度：propose_batch 并发 quorum 等待数（1 = 单飞）。
    #[arg(long, default_value_t = 1)]
    pipeline: usize,
    /// openraft max_payload_entries：网络复制批大小（README sync=1 高 TPS 需抬高，如 4096）。
    #[arg(long, default_value_t = 300)]
    max_payload_entries: u64,
    /// openraft api_batch_linger_ms：合并 ClientWrites 的等待窗口（µs→ms；0=不合并）。
    #[arg(long, default_value_t = 0)]
    batch_linger_ms: u64,
}

fn peer_addrs(base_port: u16, nodes: u64) -> Vec<(u64, SocketAddr)> {
    (1..=nodes)
        .map(|pid| {
            let port = base_port.saturating_add((pid as u16).saturating_sub(1));
            (pid, format!("127.0.0.1:{port}").parse().expect("peer addr"))
        })
        .collect()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 生成一组撮合命令（B/S 交替对敲，保证 book 非平凡 + 有成交）。
fn gen_orders(gid: u64, n: u64, client_base: u64, idem_base: u64) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::with_capacity(n as usize);
    for i in 0..n {
        // 每组价格带错开，避免跨 group 混淆
        let base_price = 100_000i64 + (gid as i64) * 10_000;
        let idem = idem_base + i;
        let cmd = if i % 4 == 3 {
            // 每 4 单撤掉一个在途买单（i-3 号）
            HpCommand::Cancel { id: client_base + i - 3 }
        } else {
            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
            let qty = 1 + (i % 7) as i64;
            let price = if side == Side::Buy { base_price - (i % 5) as i64 } else { base_price + (i % 5) as i64 };
            HpCommand::Limit {
                side,
                price_tick: price,
                qty_lot: qty,
                ts: now_ms(),
                client_id: client_base + i,
            }
        };
        out.push((idem, encode_command(&cmd, idem)));
    }
    out
}

/// 等待全部节点 apply 收敛到 `expect_orders`，再核对 summary 完全一致。
/// （RAFT 只保证 quorum 提交 + leader apply；follower apply 异步滞后，必须等收敛。）
async fn verify_all(
    nodes: &[&MultiRaft<MatchFsm>],
    gid: u64,
    expect_orders: u64,
) -> anyhow::Result<MatchSummary> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut last: Option<Vec<MatchSummary>> = None;
    loop {
        let mut sums = Vec::with_capacity(nodes.len());
        for n in nodes {
            let s = n
                .with_fsm(gid, |fsm| fsm.summary(gid).expect("group exists on node"))
                .await
                .ok_or_else(|| anyhow!("node {} no fsm for group {gid}", n.node_id()))?;
            sums.push(s);
        }
        if sums.iter().all(|x| x.applied_orders >= expect_orders) {
            let f = &sums[0];
            for (i, s) in sums.iter().enumerate() {
                if s != f {
                    return Err(anyhow!(
                        "group {gid} MISMATCH node {}: {:?} vs {:?}",
                        nodes[i].node_id(),
                        s,
                        f
                    ));
                }
            }
            return Ok(f.clone());
        }
        last = Some(sums);
        if std::time::Instant::now() > deadline {
            return Err(anyhow!(
                "group {gid} apply not converged: {:?} (expect >= {expect_orders})",
                last.as_ref().map(|v| v.iter().map(|x| x.applied_orders).collect::<Vec<_>>())
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    if args.sync > 0 && args.data_dir.is_none() {
        return Err(anyhow!("--sync > 0 requires --data-dir"));
    }

    let peer_ids: Vec<u64> = (1..=args.nodes).collect();
    let group_ids: Vec<u64> = (0..args.groups).collect();
    let peers = peer_addrs(args.port, args.nodes);

    let configs: Vec<ClusterConfig> = peer_ids
        .iter()
        .map(|&id| {
            let mut cfg = ClusterConfig::for_test(id, &peer_ids);
            cfg.peers = peers.clone();
            cfg.role = NodeRole::Voter;
            cfg.snapshot_mode = SnapshotMode::Disabled;
            if args.sync > 0 {
                cfg.data_dir = args
                    .data_dir
                    .as_ref()
                    .expect("checked")
                    .join(format!("node-{id}"));
                cfg.file_log_sync_level = match args.sync {
                    1 => FileLogSyncLevel::Os,
                    _ => FileLogSyncLevel::All,
                };
                cfg.file_log_coalesce_us = args.coalesce_us;
                cfg.max_payload_entries = args.max_payload_entries;
                cfg.api_batch_linger_ms = args.batch_linger_ms;
            }
            cfg
        })
        .collect();

    info!("starting {} nodes × {} groups (sync={})", args.nodes, args.groups, args.sync);
    let nodes = MultiRaft::<MatchFsm>::start_cluster_with(configs, |_g| MatchFsm::new()).await?;

    for &gid in &group_ids {
        for n in &nodes {
            n.create_group(gid, &peer_ids).await?;
        }
    }
    for &gid in &group_ids {
        let leader = wait_for_leader(&nodes, gid, Duration::from_secs(10))
            .await
            .ok_or_else(|| anyhow!("no leader for group {gid}"))?;
        info!(group = gid, leader, "group ready");
    }

    // ---- 阶段 1：leader 顺序 propose + 吞吐/延迟 ----
    let mut idem = AtomicU64::new(1);
    let client_base = 1_000_000u64;
    let mut lat: Vec<f64> = Vec::new();
    let mut tps_total = 0u64;

    let start = Instant::now();
    for gid in group_ids.clone() {
        let leader = wait_for_leader(&nodes, gid, Duration::from_secs(10)).await.unwrap();
        let lnode = &nodes[(leader - 1) as usize];
        let cmds = gen_orders(gid, args.orders, client_base + gid * 1_000_000, idem.load(Ordering::Relaxed));
        idem.fetch_add(args.orders, Ordering::Relaxed);
        let t0 = Instant::now();
        let pipe = args.pipeline.max(1);
        if pipe == 1 {
            for (idem_k, data) in &cmds {
                let s = Instant::now();
                lnode
                    .propose(gid, data.clone())
                    .await
                    .map_err(|e| anyhow!("group {gid} propose idem {idem_k} failed: {e}"))?;
                lat.push(s.elapsed().as_secs_f64() * 1e6);
            }
        } else {
            for chunk in cmds.chunks(pipe) {
                let payloads: Vec<Vec<u8>> = chunk.iter().map(|(_, d)| d.clone()).collect();
                let s = Instant::now();
                lnode
                    .propose_batch(gid, payloads)
                    .await
                    .map_err(|e| anyhow!("group {gid} propose_batch failed: {e}"))?;
                let dt = s.elapsed().as_secs_f64() * 1e6 / chunk.len() as f64;
                for _ in chunk {
                    lat.push(dt);
                }
            }
        }
        let dt = t0.elapsed().as_secs_f64();
        tps_total += args.orders;
        info!(
            group = gid,
            orders = args.orders,
            secs = format!("{dt:.3}"),
            tps = format!("{:.0}", args.orders as f64 / dt),
            "phase-1 propose done"
        );
    }
    let total = start.elapsed().as_secs_f64();

    let mut lat_sorted = lat.clone();
    lat_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |q: f64| lat_sorted[((lat_sorted.len() as f64 * q) as usize).min(lat_sorted.len() - 1)];
    info!(
        total_tps = format!("{:.0}", tps_total as f64 / total),
        p50_us = format!("{:.2}", p(0.50)),
        p99_us = format!("{:.2}", p(0.99)),
        p999_us = format!("{:.2}", p(0.999)),
        "phase-1 throughput/latency"
    );

    // ---- 阶段 1 验证：全部节点 FSM 状态一致 ----
    for gid in &group_ids {
        let s = verify_all(&nodes.iter().collect::<Vec<_>>(), *gid, args.orders).await?;
        info!(group = gid, ?s, "phase-1 CONSISTENT across all nodes");
    }

    // ---- 阶段 2：故障转移 ----
    if args.nodes >= 3 {
        info!("phase-2: shutting down current leader of group 0…");
        let leader0 = wait_for_leader(&nodes, 0, Duration::from_secs(10)).await.unwrap();
        nodes[(leader0 - 1) as usize].shutdown().await?;
        info!("node {leader0} shutdown; waiting for re-election…");

        let new_leader = wait_for_leader(&nodes, 0, Duration::from_secs(15))
            .await
            .ok_or_else(|| anyhow!("no re-elected leader for group 0"))?;
        info!(new_leader, "re-elected");

        // 新 leader 继续 propose（剩余 group 也顺带续推）
        let mut lat2: Vec<f64> = Vec::new();
        let n2 = args.orders / 10;
        for gid in group_ids.clone() {
            let l = wait_for_leader(&nodes, gid, Duration::from_secs(10)).await.unwrap();
            let lnode = &nodes[(l - 1) as usize];
            let cmds = gen_orders(gid, n2, client_base + 2_000_000 + gid * 1_000_000, idem.load(Ordering::Relaxed));
            idem.fetch_add(n2, Ordering::Relaxed);
            for (_, data) in &cmds {
                let s = Instant::now();
                lnode.propose(gid, data.clone()).await.map_err(|e| anyhow!("post-failover propose failed: {e}"))?;
                lat2.push(s.elapsed().as_secs_f64() * 1e6);
            }
        }
        let mut l2 = lat2.clone();
        l2.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p2 = |q: f64| l2[((l2.len() as f64 * q) as usize).min(l2.len() - 1)];
        info!(
            p50_us = format!("{:.2}", p2(0.50)),
            p99_us = format!("{:.2}", p2(0.99)),
            "phase-2 post-failover latency"
        );

        // 阶段 2 验证：存活节点状态一致（shutdown 节点跳过）
        let alive: Vec<&MultiRaft<MatchFsm>> = nodes
            .iter()
            .filter(|n| n.node_id() != leader0)
            .collect();
        for gid in &group_ids {
            let s = verify_all(&alive, *gid, args.orders + n2).await?;
            info!(group = gid, ?s, "phase-2 CONSISTENT (alive nodes)");
        }
        info!("phase-2 OK");
    }

    println!("ALL CHECKS PASSED");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run().await
}
