//! FileLogStoreOf open / reopen durability (single-node Raft).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use multiraft_core::FileLogSyncLevel;
use multiraft_fsm::CounterFsm;
use multiraft_store::FileLogStoreOf;
use multiraft_store::Raft;
use multiraft_store::Request;
use multiraft_store::StateMachineStore;
use multiraft_store::StubNetworkFactory;
use multiraft_store::TypeConfig as RaftTypeConfig;
use openraft::type_config::TypeConfigExt;
use openraft::BasicNode;
use openraft::Config;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "multiraft-file-log-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn create_single_node_file(
    node_id: u64,
    group_id: u64,
    data_dir: &std::path::Path,
) -> (Raft<CounterFsm>, StateMachineStore<CounterFsm>) {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    };
    let config = Arc::new(config.validate().unwrap());

    let dir = data_dir.join(format!("group-{group_id}"));
    let log_store = FileLogStoreOf::open(&dir).expect("open file log");
    let state_machine_store = StateMachineStore::new(group_id, CounterFsm::new());
    let network = StubNetworkFactory;

    let raft = openraft::Raft::new(
        node_id,
        config,
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await
    .unwrap();

    (raft, state_machine_store)
}

#[test]
fn open_empty_dir_write_reopen_persists_value() {
    RaftTypeConfig::run(async {
        let group_id = 1u64;
        let data_dir = temp_dir("roundtrip");
        let expected: i64 = 10 + 20 + 30;

        {
            // Fresh empty dir under FileLogStoreOf::open.
            let group_dir = data_dir.join(format!("group-{group_id}"));
            let _ = FileLogStoreOf::open(&group_dir).expect("open empty");

            let (raft, sm) = create_single_node_file(1, group_id, &data_dir).await;

            let mut nodes = BTreeMap::new();
            nodes.insert(
                1u64,
                BasicNode {
                    addr: "".to_string(),
                },
            );
            raft.initialize(nodes).await.unwrap();
            RaftTypeConfig::sleep(Duration::from_millis(200)).await;

            for (i, delta) in [10i64, 20, 30].into_iter().enumerate() {
                let req =
                    Request::new(CounterFsm::encode_add(delta, /*idem=*/ (i as u64) + 1));
                raft.client_write(req).await.unwrap();
            }

            RaftTypeConfig::sleep(Duration::from_millis(100)).await;
            let value = sm.with_fsm(|fsm| fsm.value(group_id)).await;
            assert_eq!(value, expected);

            raft.shutdown().await.unwrap();
        }

        let (raft, sm) = create_single_node_file(1, group_id, &data_dir).await;
        raft.wait_for_recovery(Some(Duration::from_secs(5)))
            .await
            .expect("recovery");

        let value = sm.with_fsm(|fsm| fsm.value(group_id)).await;
        assert_eq!(value, expected, "FSM must rebuild from durable file log");

        let _ = std::fs::remove_dir_all(&data_dir);
    });
}

#[test]
fn corrupt_hard_state_fails_open() {
    let dir = temp_dir("corrupt-hs");
    std::fs::write(dir.join("hard_state.json"), b"{not valid json").unwrap();
    let err = FileLogStoreOf::open(&dir).expect_err("corrupt hard_state must fail");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn open_with_sync_levels() {
    for level in [
        FileLogSyncLevel::Os,
        FileLogSyncLevel::Data,
        FileLogSyncLevel::All,
    ] {
        let dir = temp_dir(&format!("sync-{}", level.as_u8()));
        let store = FileLogStoreOf::open_with_options(&dir, 0, level).expect("open");
        assert_eq!(store.sync_level(), level);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
