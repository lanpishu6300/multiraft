//! Microbench: fdatasync group-commit ceiling (no Raft).
//!
//! Models "async submit + wait for disk success" as: write N frames, one
//! `sync_data`, repeat. Reports entries/s and syncs/s — the local theoretical
//! ceiling before Raft quorum.
//!
//! ```bash
//! cargo test -p multiraft-store --test bench_sync_group_commit --release -- --nocapture
//! ```

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

#[test]
fn fdatasync_batch_ceiling() {
    let dir = std::env::temp_dir().join(format!(
        "multiraft-sync1-ceiling-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("log.bin");
    let payload = vec![0u8; 128];

    println!("batch\tentries\tsyncs\tentry_tps\tsync_tps\tus_per_sync");
    for batch in [1u64, 8, 16, 32, 64, 128, 256] {
        let rounds = (4096 / batch).max(32);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        // Warmup
        for _ in 0..4 {
            for _ in 0..batch {
                f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
                f.write_all(&payload).unwrap();
            }
            f.flush().unwrap();
            f.sync_data().unwrap();
        }

        let t0 = Instant::now();
        let mut entries = 0u64;
        let mut syncs = 0u64;
        for _ in 0..rounds {
            for _ in 0..batch {
                f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
                f.write_all(&payload).unwrap();
                entries += 1;
            }
            f.flush().unwrap();
            f.sync_data().unwrap();
            syncs += 1;
        }
        let dt = t0.elapsed().as_secs_f64().max(1e-9);
        println!(
            "{batch}\t{entries}\t{syncs}\t{:.0}\t{:.0}\t{:.0}",
            entries as f64 / dt,
            syncs as f64 / dt,
            (dt * 1e6) / syncs as f64
        );
    }
    let _ = fs::remove_dir_all(&dir);
}
