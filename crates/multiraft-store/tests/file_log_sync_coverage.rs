//! Exhaustive coverage for [`FileLogSyncLevel`] paths in `FileLogStore`.
//!
//! ```bash
//! cargo test -p multiraft-store --test file_log_sync_coverage --release
//! cargo llvm-cov -p multiraft-store --tests --fail-under-lines 0 \
//!   --ignore-filename-regex 'tests/|sm_bridge|snapshot|mem_log|lib\.rs'
//! ```

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use multiraft_core::typ::Entry;
use multiraft_core::typ::LogId;
use multiraft_core::typ::Vote;
use multiraft_core::FileLogSyncLevel;
use multiraft_core::Request;
use multiraft_core::TypeConfig;
use multiraft_fsm::CounterFsm;
use multiraft_store::FileLogStoreOf;
use multiraft_store::Raft;
use multiraft_store::StateMachineStore;
use multiraft_store::StubNetworkFactory;
use openraft::alias::LeaderIdOf;
use openraft::entry::RaftEntry;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftLogStorageExt;
use openraft::type_config::TypeConfigExt;
use openraft::vote::RaftLeaderIdExt;
use openraft::BasicNode;
use openraft::Config;

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "multiraft-sync-cov-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn blank(index: u64) -> Entry {
    let leader_id = LeaderIdOf::<TypeConfig>::new_committed(1, 1);
    Entry::new_blank(LogId::new(leader_id, index))
}

fn normal(index: u64, data: Vec<u8>) -> Entry {
    let leader_id = LeaderIdOf::<TypeConfig>::new_committed(1, 1);
    let log_id = LogId::new(leader_id, index);
    Entry {
        log_id,
        payload: openraft::EntryPayload::Normal(Request::new(data)),
    }
}

async fn exercise_store(level: FileLogSyncLevel, coalesce_us: u64) {
    let dir = temp_dir(&format!("lvl{}-c{}", level.as_u8(), coalesce_us));
    let mut store = FileLogStoreOf::open_with_options(&dir, coalesce_us, level).unwrap();
    assert_eq!(store.sync_level(), level);
    let _ = format!("{store:?}");

    // Empty append (callback-only path).
    store.blocking_append(Vec::<Entry>::new()).await.unwrap();

    // Steady-state append + sync.
    store
        .blocking_append(vec![blank(1), normal(2, b"a".to_vec())])
        .await
        .unwrap();

    // Hard state (vote / committed) + sync_path via atomic_write_json.
    let vote = Vote::new(1, 1);
    store.save_vote(&vote).await.unwrap();
    assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    let last = LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), 2);
    store.save_committed(Some(last.clone())).await.unwrap();
    assert_eq!(store.read_committed().await.unwrap(), Some(last.clone()));

    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id, Some(last.clone()));

    let entries = store.try_get_log_entries(1..=2).await.unwrap();
    assert_eq!(entries.len(), 2);

    // Truncate → rewrite_log + sync.
    store
        .blocking_append(vec![blank(3), blank(4)])
        .await
        .unwrap();
    store
        .truncate_after(Some(LogId::new(
            LeaderIdOf::<TypeConfig>::new_committed(1, 1),
            2,
        )))
        .await
        .unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id.map(|x| x.index()), Some(2));

    // Purge → persist_hard_state + rewrite_log.
    store
        .purge(LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), 1))
        .await
        .unwrap();

    // Reopen must load bin + hard_state.
    let mut store2 = FileLogStoreOf::open_with_options(&dir, 0, level).unwrap();
    let state2 = store2.get_log_state().await.unwrap();
    assert!(state2.last_purged_log_id.is_some());
    let _ = store2.get_log_reader().await;

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn sync_level_storage_paths_os_data_all() {
    TypeConfig::run(async {
        for level in [
            FileLogSyncLevel::Os,
            FileLogSyncLevel::Data,
            FileLogSyncLevel::All,
        ] {
            exercise_store(level, 0).await;
            exercise_store(level, 50).await;
        }
    });
}

#[test]
fn coalesce_os_deferred_flush_persists() {
    TypeConfig::run(async {
        let dir = temp_dir("coalesce-os-defer");
        let mut store = FileLogStoreOf::open_with_options(&dir, 50, FileLogSyncLevel::Os).unwrap();
        store
            .blocking_append(vec![blank(1), blank(2)])
            .await
            .unwrap();
        TypeConfig::sleep(Duration::from_millis(5)).await;
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.map(|x| x.index()), Some(2));
        assert!(dir.join("log.bin").exists());
        let mut store2 = FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::Os).unwrap();
        assert_eq!(
            store2
                .get_log_state()
                .await
                .unwrap()
                .last_log_id
                .map(|x| x.index()),
            Some(2)
        );
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn coalesce_window_joins_concurrent_appends() {
    TypeConfig::run(async {
        let dir = temp_dir("coalesce-join");
        let store = FileLogStoreOf::open_with_options(&dir, 200, FileLogSyncLevel::Data).unwrap();

        let futs = (1..=4u64).map(|i| {
            let mut s = store.clone();
            async move { s.blocking_append(vec![blank(i)]).await }
        });
        for r in join_all(futs).await {
            r.unwrap();
        }

        let mut s = store.clone();
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.map(|x| x.index()), Some(4));
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn migrate_legacy_json_and_ndjson() {
    TypeConfig::run(async {
        // log.json migration
        {
            let dir = temp_dir("mig-json");
            let entries = vec![blank(1), blank(2)];
            let bytes = serde_json::to_vec(&entries).unwrap();
            fs::write(dir.join("log.json"), bytes).unwrap();
            let mut store =
                FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::Os).unwrap();
            let st = store.get_log_state().await.unwrap();
            assert_eq!(st.last_log_id.map(|x| x.index()), Some(2));
            assert!(dir.join("log.bin").exists());
            let _ = fs::remove_dir_all(&dir);
        }
        // log.ndjson migration
        {
            let dir = temp_dir("mig-ndjson");
            let mut f = fs::File::create(dir.join("log.ndjson")).unwrap();
            for e in [blank(1), blank(2)] {
                writeln!(f, "{}", serde_json::to_string(&e).unwrap()).unwrap();
            }
            let mut store =
                FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::Data).unwrap();
            let st = store.get_log_state().await.unwrap();
            assert_eq!(st.last_log_id.map(|x| x.index()), Some(2));
            let _ = fs::remove_dir_all(&dir);
        }
    });
}

#[test]
fn truncate_after_none_rewrites_empty() {
    TypeConfig::run(async {
        let dir = temp_dir("trunc-none");
        let mut store = FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::All).unwrap();
        store.blocking_append(vec![blank(1)]).await.unwrap();
        store.truncate_after(None).await.unwrap();
        let st = store.get_log_state().await.unwrap();
        assert!(st.last_log_id.is_none() || st.last_log_id == st.last_purged_log_id);
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn open_helpers_and_corrupt_frames() {
    TypeConfig::run(async {
        let dir = temp_dir("open-helpers");
        let s = FileLogStoreOf::open(&dir).unwrap();
        assert_eq!(s.sync_level(), FileLogSyncLevel::Os);
        let s2 = FileLogStoreOf::open_with_coalesce(&dir, 10).unwrap();
        assert_eq!(s2.sync_level(), FileLogSyncLevel::Os);
        let _ = fs::remove_dir_all(&dir);

        // Truncated length-prefixed frame.
        {
            let dir = temp_dir("bin-trunc");
            let mut f = fs::File::create(dir.join("log.bin")).unwrap();
            f.write_all(&10u32.to_le_bytes()).unwrap();
            f.write_all(b"short").unwrap();
            let err = FileLogStoreOf::open(&dir).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            let _ = fs::remove_dir_all(&dir);
        }
        // Trailing garbage after complete frames.
        {
            let dir = temp_dir("bin-trail");
            let e = blank(1);
            let raw = bincode::serialize(&e).unwrap();
            let mut f = fs::File::create(dir.join("log.bin")).unwrap();
            f.write_all(&(raw.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&raw).unwrap();
            f.write_all(&[0xAB]).unwrap();
            let err = FileLogStoreOf::open(&dir).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            let _ = fs::remove_dir_all(&dir);
        }
        // Valid length, invalid bincode payload.
        {
            let dir = temp_dir("bin-bad");
            let mut f = fs::File::create(dir.join("log.bin")).unwrap();
            let junk = [0u8; 8];
            f.write_all(&(junk.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&junk).unwrap();
            let err = FileLogStoreOf::open(&dir).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            let _ = fs::remove_dir_all(&dir);
        }
        // ndjson: blank lines + bad JSON.
        {
            let dir = temp_dir("ndjson-bad");
            let mut f = fs::File::create(dir.join("log.ndjson")).unwrap();
            writeln!(f).unwrap();
            writeln!(f, "not-json").unwrap();
            let err = FileLogStoreOf::open(&dir).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            let _ = fs::remove_dir_all(&dir);
        }
    });
}

#[test]
fn os_stream_deferred_flush_persists_and_callbacks() {
    TypeConfig::run(async {
        let dir = temp_dir("stream-os");
        let stream = multiraft_store::FileLogStreamOptions {
            stream_buf_bytes: 64 * 1024,
            stream_flush_ms: 5,
            hold_overlap: false,
        };
        let mut store =
            FileLogStoreOf::open_with_full_options(&dir, 0, FileLogSyncLevel::Os, stream).unwrap();

        store
            .blocking_append(vec![blank(1), blank(2)])
            .await
            .unwrap();
        TypeConfig::sleep(Duration::from_millis(20)).await;

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.map(|x| x.index()), Some(2));
        assert!(dir.join("log.bin").exists());

        let mut store2 =
            FileLogStoreOf::open_with_full_options(&dir, 0, FileLogSyncLevel::Os, stream).unwrap();
        let state2 = store2.get_log_state().await.unwrap();
        assert_eq!(state2.last_log_id.map(|x| x.index()), Some(2));
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn data_sync_group_commit_batches_one_fdatasync() {
    TypeConfig::run(async {
        let dir = temp_dir("data-group-commit");
        // coalesce window lets overlapping appends share one sync_data.
        let store = FileLogStoreOf::open_with_options(&dir, 2_000, FileLogSyncLevel::Data).unwrap();
        let futs = (1..=16u64).map(|i| {
            let mut s = store.clone();
            async move { s.blocking_append(vec![blank(i)]).await }
        });
        for r in join_all(futs).await {
            r.unwrap();
        }
        TypeConfig::sleep(Duration::from_millis(10)).await;
        let mut s = store.clone();
        let st = s.get_log_state().await.unwrap();
        assert_eq!(st.last_log_id.map(|x| x.index()), Some(16));
        let mut s2 = FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::Data).unwrap();
        assert_eq!(
            s2.get_log_state()
                .await
                .unwrap()
                .last_log_id
                .map(|x| x.index()),
            Some(16)
        );
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn hold_overlap_stress_batch() {
    use std::time::Instant;
    TypeConfig::run(async {
        let dir = temp_dir("hold-stress");
        let stream = multiraft_store::FileLogStreamOptions {
            stream_buf_bytes: 64 * 1024 * 1024,
            stream_flush_ms: 0,
            hold_overlap: true,
        };
        let store =
            FileLogStoreOf::open_with_full_options(&dir, 50_000, FileLogSyncLevel::Data, stream)
                .unwrap();
        let n = 512u64;
        let t0 = Instant::now();
        let futs = (1..=n).map(|i| {
            let mut s = store.clone();
            async move { s.blocking_append(vec![blank(i)]).await }
        });
        for r in join_all(futs).await {
            r.unwrap();
        }
        let elapsed = t0.elapsed();
        println!(
            "hold_stress n={n} elapsed_ms={:.1}",
            elapsed.as_secs_f64() * 1000.0
        );
        // One timer window should cover the burst (plus fdatasync).
        assert!(elapsed >= Duration::from_millis(40), "got {elapsed:?}");
        assert!(elapsed < Duration::from_millis(200), "too slow {elapsed:?}");
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn hold_overlap_waits_for_coalesce_timer() {
    use std::time::Instant;

    TypeConfig::run(async {
        let dir = temp_dir("hold-overlap-timer");
        let stream = multiraft_store::FileLogStreamOptions {
            stream_buf_bytes: 0,
            stream_flush_ms: 0,
            hold_overlap: true,
        };
        let store =
            FileLogStoreOf::open_with_full_options(&dir, 30_000, FileLogSyncLevel::Data, stream)
                .unwrap();
        let t0 = Instant::now();
        let futs = (1..=8u64).map(|i| {
            let mut s = store.clone();
            async move { s.blocking_append(vec![blank(i)]).await }
        });
        for r in join_all(futs).await {
            r.unwrap();
        }
        let elapsed = t0.elapsed();
        // Regression: notifying the flusher on every defer used to flush immediately
        // (~few ms). Timed group-commit must wait for the coalesce window.
        assert!(
            elapsed >= Duration::from_millis(20),
            "hold_overlap must wait for coalesce timer, got {elapsed:?}"
        );
        let mut s2 = FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::Data).unwrap();
        assert_eq!(
            s2.get_log_state()
                .await
                .unwrap()
                .last_log_id
                .map(|x| x.index()),
            Some(8)
        );
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn data_sync_never_defers_stream_flush() {
    TypeConfig::run(async {
        let dir = temp_dir("stream-data");
        let stream = multiraft_store::FileLogStreamOptions {
            stream_buf_bytes: 64 * 1024,
            stream_flush_ms: 50,
            hold_overlap: false,
        };
        let mut store =
            FileLogStoreOf::open_with_full_options(&dir, 0, FileLogSyncLevel::Data, stream)
                .unwrap();
        store.blocking_append(vec![blank(1)]).await.unwrap();
        let len = fs::metadata(dir.join("log.bin")).unwrap().len();
        assert!(len > 0, "Data sync must flush before append returns");
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
#[ignore = "manual microbench: cargo test -p multiraft-store --test file_log_sync_coverage stream_microbench --release -- --ignored --nocapture"]
fn stream_microbench_immediate_vs_deferred() {
    use std::time::Instant;

    TypeConfig::run(async {
        let n = 2_000u64;
        let data = vec![0u8; 128];

        let immediate_dir = temp_dir("bench-immediate");
        let mut immediate =
            FileLogStoreOf::open_with_options(&immediate_dir, 0, FileLogSyncLevel::Os).unwrap();
        let t0 = Instant::now();
        for i in 1..=n {
            immediate
                .blocking_append(vec![normal(i, data.clone())])
                .await
                .unwrap();
        }
        let immediate_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let stream_dir = temp_dir("bench-stream");
        let stream = multiraft_store::FileLogStreamOptions {
            stream_buf_bytes: 256 * 1024,
            stream_flush_ms: 1,
            hold_overlap: false,
        };
        let mut deferred =
            FileLogStoreOf::open_with_full_options(&stream_dir, 0, FileLogSyncLevel::Os, stream)
                .unwrap();
        let t1 = Instant::now();
        for i in 1..=n {
            deferred
                .blocking_append(vec![normal(i, data.clone())])
                .await
                .unwrap();
        }
        TypeConfig::sleep(Duration::from_millis(10)).await;
        let stream_ms = t1.elapsed().as_secs_f64() * 1000.0;

        println!(
            "{}",
            serde_json::json!({
                "entries": n,
                "immediate_os_ms": immediate_ms,
                "stream_os_ms": stream_ms,
                "immediate_tps": (n as f64) * 1000.0 / immediate_ms,
                "stream_tps": (n as f64) * 1000.0 / stream_ms,
                "speedup_x": immediate_ms / stream_ms.max(0.001),
            })
        );

        let _ = fs::remove_dir_all(immediate_dir);
        let _ = fs::remove_dir_all(stream_dir);
    });
}

#[test]
fn stream_options_getter_and_buf_threshold_flush() {
    TypeConfig::run(async {
        let dir = temp_dir("stream-buf");
        let stream = multiraft_store::FileLogStreamOptions {
            stream_buf_bytes: 64,
            stream_flush_ms: 0,
            hold_overlap: false,
        };
        let mut store =
            FileLogStoreOf::open_with_full_options(&dir, 0, FileLogSyncLevel::Os, stream).unwrap();
        assert_eq!(store.stream_options(), stream);

        // Large entry exceeds stream_buf_bytes → defer_os_stream false → sync flush.
        let big = vec![0u8; 128];
        store.blocking_append(vec![normal(1, big)]).await.unwrap();
        assert!(fs::metadata(dir.join("log.bin")).unwrap().len() > 0);

        // Notify-only flusher (flush_ms=0): park on notify, then drop store.
        let dir2 = temp_dir("stream-notify");
        let stream2 = multiraft_store::FileLogStreamOptions {
            stream_buf_bytes: 1024 * 1024,
            stream_flush_ms: 0,
            hold_overlap: false,
        };
        let store2 =
            FileLogStoreOf::open_with_full_options(&dir2, 0, FileLogSyncLevel::Os, stream2)
                .unwrap();
        {
            let mut s = store2.clone();
            s.blocking_append(vec![blank(1)]).await.unwrap();
        }
        TypeConfig::sleep(Duration::from_millis(10)).await;
        drop(store2);
        // Wake flusher so it observes weak upgrade failure and exits.
        TypeConfig::sleep(Duration::from_millis(20)).await;

        // Timer flusher teardown (flush_ms>0).
        let dir3 = temp_dir("stream-teardown");
        let stream3 = multiraft_store::FileLogStreamOptions {
            stream_buf_bytes: 1024 * 1024,
            stream_flush_ms: 5,
            hold_overlap: false,
        };
        let store3 =
            FileLogStoreOf::open_with_full_options(&dir3, 0, FileLogSyncLevel::Os, stream3)
                .unwrap();
        {
            let mut s = store3.clone();
            s.blocking_append(vec![blank(1)]).await.unwrap();
        }
        TypeConfig::sleep(Duration::from_millis(10)).await;
        drop(store3);
        TypeConfig::sleep(Duration::from_millis(40)).await;

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&dir2);
        let _ = fs::remove_dir_all(&dir3);
    });
}

#[test]
fn coalesce_plus_stream_defer_and_join_notify() {
    TypeConfig::run(async {
        let dir = temp_dir("coalesce-stream");
        let stream = multiraft_store::FileLogStreamOptions {
            stream_buf_bytes: 1024 * 1024,
            stream_flush_ms: 5,
            hold_overlap: false,
        };
        let store = FileLogStoreOf::open_with_full_options(&dir, 100, FileLogSyncLevel::Os, stream)
            .unwrap();

        // Concurrent appends: n_callbacks>1 → coalesce_notify + flush path.
        let futs = (1..=4u64).map(|i| {
            let mut s = store.clone();
            async move { s.blocking_append(vec![blank(i)]).await }
        });
        for r in join_all(futs).await {
            r.unwrap();
        }

        // Single append under coalesce + stream_flush_ms → defer path after window.
        let mut s = store.clone();
        s.blocking_append(vec![blank(5)]).await.unwrap();
        TypeConfig::sleep(Duration::from_millis(40)).await;

        let st = s.get_log_state().await.unwrap();
        assert_eq!(st.last_log_id.map(|x| x.index()), Some(5));

        // Burst of deferred appends while flusher is already running
        // (ensure_stream_flusher compare_exchange miss).
        let burst = {
            let store = store.clone();
            async move {
                let futs = (6..=20u64).map(|i| {
                    let mut s = store.clone();
                    async move { s.blocking_append(vec![blank(i)]).await }
                });
                for r in join_all(futs).await {
                    r.unwrap();
                }
            }
        };
        burst.await;
        TypeConfig::sleep(Duration::from_millis(40)).await;
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn save_vote_persists_when_log_is_os() {
    TypeConfig::run(async {
        let dir = temp_dir("vote-os");
        let mut store = FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::Os).unwrap();
        let vote = Vote::new(2, 1);
        store.save_vote(&vote).await.unwrap();

        let mut reopened =
            FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::Os).unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn rewrite_persists_dirty_hard_state() {
    TypeConfig::run(async {
        let dir = temp_dir("hs-dirty");
        let mut store = FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::Os).unwrap();
        store
            .blocking_append(vec![blank(1), blank(2)])
            .await
            .unwrap();
        let last = LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), 2);
        // Debounced commit dirties hard_state without immediate persist.
        store.save_committed(Some(last.clone())).await.unwrap();
        store
            .truncate_after(Some(LogId::new(
                LeaderIdOf::<TypeConfig>::new_committed(1, 1),
                1,
            )))
            .await
            .unwrap();
        let mut store2 = FileLogStoreOf::open_with_options(&dir, 0, FileLogSyncLevel::Os).unwrap();
        assert_eq!(store2.read_committed().await.unwrap(), Some(last));
        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn raft_end_to_end_with_sync_data() {
    TypeConfig::run(async {
        let data_dir = temp_dir("raft-e2e");
        let group_id = 1u64;
        let config = Arc::new(
            Config {
                heartbeat_interval: 200,
                election_timeout_min: 500,
                election_timeout_max: 1000,
                max_in_snapshot_log_to_keep: 0,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let group_dir = data_dir.join(format!("group-{group_id}"));
        let log_store =
            FileLogStoreOf::open_with_options(&group_dir, 0, FileLogSyncLevel::Data).unwrap();
        let sm = StateMachineStore::new(group_id, CounterFsm::new());
        let raft = openraft::Raft::new(1u64, config, StubNetworkFactory, log_store, sm.clone())
            .await
            .unwrap();

        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(1u64, BasicNode { addr: "".into() });
        raft.initialize(nodes).await.unwrap();
        TypeConfig::sleep(Duration::from_millis(150)).await;

        raft.client_write(Request::new(CounterFsm::encode_add(7, 1)))
            .await
            .unwrap();
        TypeConfig::sleep(Duration::from_millis(50)).await;
        assert_eq!(sm.with_fsm(|f| f.value(group_id)).await, 7);

        raft.shutdown().await.unwrap();

        // Recover with All sync.
        let log_store =
            FileLogStoreOf::open_with_options(&group_dir, 0, FileLogSyncLevel::All).unwrap();
        let sm2 = StateMachineStore::new(group_id, CounterFsm::new());
        let raft2: Raft<CounterFsm> = openraft::Raft::new(
            1u64,
            Arc::new(
                Config {
                    heartbeat_interval: 200,
                    election_timeout_min: 500,
                    election_timeout_max: 1000,
                    max_in_snapshot_log_to_keep: 0,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            ),
            StubNetworkFactory,
            log_store,
            sm2.clone(),
        )
        .await
        .unwrap();
        raft2
            .wait_for_recovery(Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(sm2.with_fsm(|f| f.value(group_id)).await, 7);
        raft2.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(&data_dir);
    });
}
