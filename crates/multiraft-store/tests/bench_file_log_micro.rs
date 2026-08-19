//! Microbench: legacy full-log rewrite vs NDJSON append (no Raft).
//!
//! ```bash
//! cargo test -p multiraft-store --test bench_file_log_micro -- --nocapture
//! ```

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Serialize, Deserialize)]
struct FakeEntry {
    index: u64,
    term: u64,
    data: Vec<u8>,
}

fn temp_dir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "multiraft-flog-micro-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    fs::create_dir_all(&d).unwrap();
    d
}

fn legacy_rewrite_all(dir: &Path, log: &[FakeEntry]) {
    let bytes = serde_json::to_vec(log).unwrap();
    let path = dir.join("log.json");
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, &bytes).unwrap();
    fs::rename(&tmp, &path).unwrap();
}

fn bin_append(dir: &Path, entry: &FakeEntry) {
    let path = dir.join("log.bin");
    let raw = bincode::serialize(entry).unwrap();
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();
    f.write_all(&(raw.len() as u32).to_le_bytes()).unwrap();
    f.write_all(&raw).unwrap();
}

#[test]
#[ignore = "manual microbench: cargo test -p multiraft-store --test bench_file_log_micro -- --ignored --nocapture"]
fn legacy_rewrite_vs_bin_append_scale() {
    let n = 10_000u64;
    let payload = vec![0u8; 128];

    let legacy_dir = temp_dir();
    let mut legacy_log = Vec::new();
    let t0 = Instant::now();
    for i in 1..=n {
        legacy_log.push(FakeEntry {
            index: i,
            term: 1,
            data: payload.clone(),
        });
        legacy_rewrite_all(&legacy_dir, &legacy_log);
    }
    let legacy_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let bin_dir = temp_dir();
    let t1 = Instant::now();
    for i in 1..=n {
        let e = FakeEntry {
            index: i,
            term: 1,
            data: payload.clone(),
        };
        bin_append(&bin_dir, &e);
    }
    let bin_ms = t1.elapsed().as_secs_f64() * 1000.0;

    let speedup = legacy_ms / bin_ms.max(0.001);
    println!(
        "{}",
        serde_json::json!({
            "entries": n,
            "payload_bytes": 128,
            "legacy_full_rewrite_ms": legacy_ms,
            "bin_append_ms": bin_ms,
            "speedup_x": speedup,
            "legacy_tps": (n as f64) * 1000.0 / legacy_ms,
            "bin_tps": (n as f64) * 1000.0 / bin_ms,
        })
    );

    assert!(
        speedup > 3.0,
        "bin append should beat full rewrite: speedup={speedup}"
    );

    let _ = fs::remove_dir_all(legacy_dir);
    let _ = fs::remove_dir_all(bin_dir);
}
