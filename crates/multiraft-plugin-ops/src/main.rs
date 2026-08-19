//! PremiumClusterTool-style CLI for multiraft demo admin routes.

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use clap::Subcommand;
use multiraft_plugin_ops::ADMIN_ROUTES;

#[derive(Parser)]
#[command(name = "multiraft-ops", about = "Premium ops CLI for multiraft demo admin")]
struct Cli {
    /// Demo admin base URL (e.g. http://127.0.0.1:21100)
    #[arg(long, short = 'u', default_value = "http://127.0.0.1:21100")]
    base: String,

    /// Bearer token (`Authorization: Bearer …`)
    #[arg(long, env = "MULTIRAFT_ADMIN_TOKEN")]
    token: Option<String>,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List documented admin routes.
    Routes,
    /// GET /metrics/propose-stages
    MetricsStages,
    /// GET /admin/archive/{group}/positions
    ArchivePositions {
        group: u64,
    },
    /// GET /admin/archive/{group}/export/{index}/{term}
    ArchiveExport {
        group: u64,
        index: u64,
        term: u64,
        /// Write snapshot bytes to this path (default: stdout JSON manifest only).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// GET /admin/groups/{group}/status
    GroupStatus {
        group: u64,
    },
    /// GET /admin/best_snapshot_ad/{group}
    BestAd {
        group: u64,
    },
    /// POST /admin/promote_standby/{group}/{id}
    Promote {
        group: u64,
        id: u64,
    },
    /// POST /admin/demote_standby/{group}/{id}
    Demote {
        group: u64,
        id: u64,
    },
    /// POST /admin/replicate_standby_snapshot/{group}
    Replicate {
        group: u64,
        #[arg(long)]
        fetch_url: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Command::Routes => {
            for r in ADMIN_ROUTES {
                println!("{r}");
            }
        }
        Command::MetricsStages => {
            let body = get(&cli, "/metrics/propose-stages").await?;
            println!("{body}");
        }
        Command::ArchivePositions { group } => {
            let path = format!("/admin/archive/{group}/positions");
            let body = get(&cli, &path).await?;
            println!("{body}");
        }
        Command::ArchiveExport {
            group,
            index,
            term,
            ref out,
        } => {
            if let Some(path) = out {
                let url = format!(
                    "{}/admin/archive/{group}/export/{index}/{term}/data",
                    cli.base.trim_end_matches('/')
                );
                let bytes = get_bytes(&cli, &url).await?;
                std::fs::write(&path, &bytes)
                    .with_context(|| format!("write {}", path.display()))?;
                println!("wrote {} bytes to {}", bytes.len(), path.display());
            } else {
                let path = format!("/admin/archive/{group}/export/{index}/{term}");
                let body = get(&cli, &path).await?;
                println!("{body}");
            }
        }
        Command::GroupStatus { group } => {
            let path = format!("/admin/groups/{group}/status");
            let body = get(&cli, &path).await?;
            println!("{body}");
        }
        Command::BestAd { group } => {
            let path = format!("/admin/best_snapshot_ad/{group}");
            let body = get(&cli, &path).await?;
            println!("{body}");
        }
        Command::Promote { group, id } => {
            let path = format!("/admin/promote_standby/{group}/{id}");
            let body = post(&cli, &path, "{}").await?;
            println!("{body}");
        }
        Command::Demote { group, id } => {
            let path = format!("/admin/demote_standby/{group}/{id}");
            let body = post(&cli, &path, "{}").await?;
            println!("{body}");
        }
        Command::Replicate { group, ref fetch_url } => {
            let path = format!("/admin/replicate_standby_snapshot/{group}");
            let payload = if let Some(url) = fetch_url {
                serde_json::json!({ "fetch_url": url }).to_string()
            } else {
                "{}".into()
            };
            let body = post(&cli, &path, &payload).await?;
            println!("{body}");
        }
    }
    Ok(())
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

fn auth_header(cli: &Cli) -> Option<String> {
    cli.token
        .as_ref()
        .map(|t| format!("Bearer {t}"))
        .or_else(|| std::env::var("MULTIRAFT_ADMIN_TOKEN").ok().map(|t| format!("Bearer {t}")))
}

async fn get(cli: &Cli, path: &str) -> anyhow::Result<String> {
    let url = format!("{}{}", cli.base.trim_end_matches('/'), path);
    let mut req = client().get(url);
    if let Some(h) = auth_header(cli) {
        req = req.header("Authorization", h);
    }
    let resp = req.send().await?.error_for_status()?;
    Ok(resp.text().await?)
}

async fn get_bytes(cli: &Cli, url: &str) -> anyhow::Result<Vec<u8>> {
    let mut req = client().get(url);
    if let Some(h) = auth_header(cli) {
        req = req.header("Authorization", h);
    }
    let resp = req.send().await?.error_for_status()?;
    Ok(resp.bytes().await?.to_vec())
}

async fn post(cli: &Cli, path: &str, body: &str) -> anyhow::Result<String> {
    let url = format!("{}{}", cli.base.trim_end_matches('/'), path);
    let mut req = client()
        .post(url)
        .header("content-type", "application/json")
        .body(body.to_string());
    if let Some(h) = auth_header(cli) {
        req = req.header("Authorization", h);
    }
    let resp = req.send().await?.error_for_status()?;
    Ok(resp.text().await?)
}
