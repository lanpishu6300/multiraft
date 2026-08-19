//! Durable snapshot catalog: `{root}/{group}/{index}-{term}/`.

use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use multiraft_fsm::GroupId;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

/// One durable snapshot entry on disk.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub group: GroupId,
    pub last_index: u64,
    pub last_term: u64,
    pub snapshot_id: String,
    pub size: u64,
    pub sha256_hex: String,
    #[serde(skip)]
    pub dir: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct MetaFile {
    group: GroupId,
    last_index: u64,
    last_term: u64,
    snapshot_id: String,
    size: u64,
    sha256_hex: String,
}

/// Durable snapshot store under `{root}/{group}/{index}-{term}/`.
#[derive(Clone, Debug)]
pub struct SnapshotCatalog {
    root: PathBuf,
    keep: usize,
}

impl SnapshotCatalog {
    pub fn new(root: impl Into<PathBuf>, keep: usize) -> Self {
        Self {
            root: root.into(),
            keep: keep.max(1),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn keep(&self) -> usize {
        self.keep
    }

    /// Write `data.bin` + `meta.json` + `sha256`, fsync, then prune older entries.
    pub fn write(
        &self,
        group: GroupId,
        last_index: u64,
        last_term: u64,
        snapshot_id: impl Into<String>,
        data: &[u8],
    ) -> io::Result<CatalogEntry> {
        let snapshot_id = snapshot_id.into();
        let dir_name = format!("{last_index}-{last_term}");
        let dir = self.root.join(group.to_string()).join(&dir_name);
        fs::create_dir_all(&dir)?;

        let sha256_hex = hex_sha256(data);
        let data_path = dir.join("data.bin");
        write_fsync(&data_path, data)?;

        let meta = MetaFile {
            group,
            last_index,
            last_term,
            snapshot_id: snapshot_id.clone(),
            size: data.len() as u64,
            sha256_hex: sha256_hex.clone(),
        };
        let meta_bytes = serde_json::to_vec_pretty(&meta)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        write_fsync(&dir.join("meta.json"), &meta_bytes)?;
        write_fsync(&dir.join("sha256"), format!("{sha256_hex}\n").as_bytes())?;

        // fsync directory for durability of the new entry name.
        crate::durability::sync_dir(&dir)?;

        self.prune(group)?;

        Ok(CatalogEntry {
            group,
            last_index,
            last_term,
            snapshot_id,
            size: data.len() as u64,
            sha256_hex,
            dir,
        })
    }

    /// Latest snapshot for `group` by `(last_index, last_term)`.
    pub fn latest(&self, group: GroupId) -> io::Result<Option<CatalogEntry>> {
        let mut entries = self.list(group)?;
        entries.sort_by(|a, b| {
            (a.last_index, a.last_term)
                .cmp(&(b.last_index, b.last_term))
                .reverse()
        });
        Ok(entries.into_iter().next())
    }

    /// Read snapshot bytes for `snapshot_id` (or the directory name match).
    pub fn read(&self, group: GroupId, snapshot_id: &str) -> io::Result<Option<Vec<u8>>> {
        let Some(entry) = self.find(group, snapshot_id)? else {
            return Ok(None);
        };
        let data = fs::read(entry.dir.join("data.bin"))?;
        let actual = hex_sha256(&data);
        if actual != entry.sha256_hex {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "snapshot sha256 mismatch: expected {}, got {}",
                    entry.sha256_hex, actual
                ),
            ));
        }
        Ok(Some(data))
    }

    fn find(&self, group: GroupId, snapshot_id: &str) -> io::Result<Option<CatalogEntry>> {
        for e in self.list(group)? {
            if e.snapshot_id == snapshot_id
                || e.dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n == snapshot_id)
            {
                return Ok(Some(e));
            }
        }
        Ok(None)
    }

    /// All durable snapshot entries for `group`.
    pub fn list(&self, group: GroupId) -> io::Result<Vec<CatalogEntry>> {
        let group_dir = self.root.join(group.to_string());
        if !group_dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for ent in fs::read_dir(&group_dir)? {
            let ent = ent?;
            if !ent.file_type()?.is_dir() {
                continue;
            }
            let dir = ent.path();
            let meta_path = dir.join("meta.json");
            if !meta_path.exists() {
                continue;
            }
            let meta_bytes = fs::read(&meta_path)?;
            let meta: MetaFile = serde_json::from_slice(&meta_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            out.push(CatalogEntry {
                group: meta.group,
                last_index: meta.last_index,
                last_term: meta.last_term,
                snapshot_id: meta.snapshot_id,
                size: meta.size,
                sha256_hex: meta.sha256_hex,
                dir,
            });
        }
        Ok(out)
    }

    fn prune(&self, group: GroupId) -> io::Result<()> {
        let mut entries = self.list(group)?;
        if entries.len() <= self.keep {
            return Ok(());
        }
        entries.sort_by(|a, b| (a.last_index, a.last_term).cmp(&(b.last_index, b.last_term)));
        let remove = entries.len() - self.keep;
        for e in entries.into_iter().take(remove) {
            let _ = fs::remove_dir_all(&e.dir);
        }
        Ok(())
    }
}

fn hex_sha256(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn write_fsync(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let mut f = fs::File::create(path)?;
    f.write_all(data)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read_latest_and_prune() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SnapshotCatalog::new(dir.path(), 2);

        let e1 = catalog
            .write(0, 10, 1, "10-1", b"hello-10")
            .expect("write 1");
        assert_eq!(e1.last_index, 10);
        assert_eq!(catalog.read(0, "10-1").unwrap().unwrap(), b"hello-10");

        let e2 = catalog
            .write(0, 20, 2, "20-2", b"hello-20")
            .expect("write 2");
        assert_eq!(
            catalog.latest(0).unwrap().unwrap().snapshot_id,
            e2.snapshot_id
        );

        let _e3 = catalog
            .write(0, 30, 3, "30-3", b"hello-30")
            .expect("write 3");
        // keep=2 → oldest (10) pruned
        assert!(catalog.read(0, "10-1").unwrap().is_none());
        assert!(catalog.read(0, "20-2").unwrap().is_some());
        assert!(catalog.read(0, "30-3").unwrap().is_some());
        assert_eq!(catalog.latest(0).unwrap().unwrap().last_index, 30);
    }
}
