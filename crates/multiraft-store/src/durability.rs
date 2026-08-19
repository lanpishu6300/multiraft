//! Durable file replace helpers (atomic rename + directory fsync).
//!
//! OpenRaft treats successful storage calls as proof data reached stable storage.
//! Vote / hard-state writes use at least [`FileLogSyncLevel::Data`] even when the
//! log append path runs at [`FileLogSyncLevel::Os`] for throughput.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use multiraft_core::FileLogSyncLevel;

/// Hard-state / vote durability: never weaker than `Data` (openraft `save_vote` contract).
pub fn hard_state_sync_level(log_level: FileLogSyncLevel) -> FileLogSyncLevel {
    match log_level {
        FileLogSyncLevel::Os => FileLogSyncLevel::Data,
        other => other,
    }
}

pub fn sync_file(f: &File, level: FileLogSyncLevel) -> io::Result<()> {
    match level {
        FileLogSyncLevel::Os => Ok(()),
        FileLogSyncLevel::Data => f.sync_data(),
        FileLogSyncLevel::All => f.sync_all(),
    }
}

pub fn sync_path(path: &Path, level: FileLogSyncLevel) -> io::Result<()> {
    if matches!(level, FileLogSyncLevel::Os) {
        return Ok(());
    }
    let f = OpenOptions::new().write(true).read(true).open(path)?;
    sync_file(&f, level)
}

/// Flush the directory entry created or replaced by `rename(2)`.
#[cfg(unix)]
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
pub fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

fn temp_swap_path(path: &Path) -> PathBuf {
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("file");
    path.with_file_name(format!("{name}.tmp"))
}

/// Atomically replace `path` with `contents`, durable through `sync_level`.
///
/// Writes a temp file, fsyncs it, renames over `path`, then fsyncs the parent
/// directory so the rename survives power loss.
pub fn atomic_replace(
    path: &Path,
    contents: &[u8],
    sync_level: FileLogSyncLevel,
) -> io::Result<()> {
    let tmp = temp_swap_path(path);
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(contents)?;
        sync_file(&f, sync_level)?;
    }
    fs::rename(&tmp, path)?;
    sync_dir(path.parent().unwrap_or_else(|| Path::new(".")))?;
    if matches!(sync_level, FileLogSyncLevel::All) {
        sync_path(path, sync_level)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_state_sync_level_never_weaker_than_data() {
        assert_eq!(
            hard_state_sync_level(FileLogSyncLevel::Os),
            FileLogSyncLevel::Data
        );
        assert_eq!(
            hard_state_sync_level(FileLogSyncLevel::Data),
            FileLogSyncLevel::Data
        );
        assert_eq!(
            hard_state_sync_level(FileLogSyncLevel::All),
            FileLogSyncLevel::All
        );
    }

    #[test]
    fn atomic_replace_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        atomic_replace(&path, br#"{"v":1}"#, FileLogSyncLevel::Data).unwrap();
        let got = fs::read(&path).unwrap();
        assert_eq!(got, br#"{"v":1}"#);
        atomic_replace(&path, br#"{"v":2}"#, FileLogSyncLevel::All).unwrap();
        assert_eq!(fs::read(&path).unwrap(), br#"{"v":2}"#);
    }
}
