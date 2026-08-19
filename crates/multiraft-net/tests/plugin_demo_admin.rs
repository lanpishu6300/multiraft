//! Demo admin HTTP tests (T-D1–T-D3): metrics, archive, auth.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Path;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::get;
use axum::Json;
use axum::Router;
use multiraft_core::ClusterConfig;
use multiraft_core::PluginRegistry;
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_net::wait_for_leader;
use multiraft_plugin_auth::BearerTokenAuth;
use multiraft_plugin_ops::premium_registry;
use serde_json::json;
use tokio::net::TcpListener;

struct AdminState {
    nodes: Vec<MultiRaft>,
}

fn auth_ok(nodes: &[MultiRaft], headers: &HeaderMap) -> bool {
    nodes
        .first()
        .map(|n| {
            n.plugin_registry().authorize_admin(
                headers
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok()),
            )
        })
        .unwrap_or(true)
}

async fn require_auth(
    State(state): State<Arc<AdminState>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if !auth_ok(&state.nodes, &headers) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}

async fn metrics_stages(State(state): State<Arc<AdminState>>) -> Json<serde_json::Value> {
    let node = &state.nodes[0];
    Json(json!({
        "ok": true,
        "metrics": node.plugin_registry().metrics_snapshots(),
    }))
}

async fn archive_positions(
    State(state): State<Arc<AdminState>>,
    Path(group): Path<u64>,
) -> Json<serde_json::Value> {
    let positions: Vec<_> = state.nodes[0]
        .plugin_registry()
        .archive_plugins()
        .iter()
        .flat_map(|a| a.list_positions(group))
        .collect();
    Json(json!({ "ok": true, "group": group, "positions": positions }))
}

async fn spawn_test_admin(
    state: Arc<AdminState>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/metrics/propose-stages", get(metrics_stages))
        .route("/admin/archive/:group/positions", get(archive_positions))
        .route_layer(from_fn_with_state(Arc::clone(&state), require_auth))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, handle)
}

#[tokio::test]
async fn demo_admin_metrics_with_bearer() {
    let reg = premium_registry(Some("tok"), None);
    let cfg = ClusterConfig::for_test(1, &[1]);
    let node = MultiRaft::start_with_plugins(cfg, reg).await.unwrap();
    node.create_group(0, &[1]).await.unwrap();
    wait_for_leader(std::slice::from_ref(&node), 0, Duration::from_secs(5))
        .await
        .expect("leader");
    node.propose(0, CounterFsm::encode_add(1, 1))
        .await
        .unwrap();

    let state = Arc::new(AdminState {
        nodes: vec![node],
    });
    let (addr, _srv) = spawn_test_admin(state).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/metrics/propose-stages");

    let denied = client.get(&url).send().await.unwrap();
    assert_eq!(denied.status(), 401);

    let ok = client
        .get(&url)
        .header("Authorization", "Bearer tok")
        .send()
        .await
        .unwrap();
    assert!(ok.status().is_success());
    let body: serde_json::Value = ok.json().await.unwrap();
    assert_eq!(body["ok"], true);
    let metrics = body["metrics"].as_array().expect("metrics array");
    assert!(!metrics.is_empty());
}

#[tokio::test]
async fn demo_admin_archive_positions_auth() {
    let reg = PluginRegistry::new().register_auth(Arc::new(BearerTokenAuth::new("sekret")));
    let cfg = ClusterConfig::for_test(1, &[1]);
    let node = MultiRaft::start_with_plugins(cfg, reg).await.unwrap();
    let state = Arc::new(AdminState {
        nodes: vec![node],
    });
    let (addr, _srv) = spawn_test_admin(state).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/admin/archive/0/positions");
    let ok = client
        .get(&url)
        .header("Authorization", "Bearer sekret")
        .send()
        .await
        .unwrap();
    assert!(ok.status().is_success());
}
