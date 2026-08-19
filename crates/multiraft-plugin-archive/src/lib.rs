//! N3 archive ops over local [`SnapshotCatalog`].

use std::sync::Arc;

use multiraft_core::ArchiveExport;
use multiraft_core::ArchivePlugin;
use multiraft_core::ArchiveSnapshot;
use multiraft_core::GroupId;
use multiraft_core::LogPosition;
use multiraft_core::Plugin;
use multiraft_store::SnapshotCatalog;

#[derive(Clone)]
pub struct CatalogArchive {
    catalog: Arc<SnapshotCatalog>,
}

impl CatalogArchive {
    pub fn new(catalog: Arc<SnapshotCatalog>) -> Self {
        Self { catalog }
    }
}

impl Plugin for CatalogArchive {
    fn name(&self) -> &'static str {
        "catalog-archive"
    }
}

impl ArchivePlugin for CatalogArchive {
    fn list_positions(&self, group: GroupId) -> Vec<LogPosition> {
        let Ok(mut entries) = self.catalog.list(group) else {
            return Vec::new();
        };
        entries.sort_by(|a, b| (a.last_index, a.last_term).cmp(&(b.last_index, b.last_term)));
        entries
            .into_iter()
            .map(|e| LogPosition {
                group: e.group,
                term: e.last_term,
                index: e.last_index,
                snapshot_id: e.snapshot_id,
            })
            .collect()
    }

    fn export_at(&self, group: GroupId, index: u64, term: u64) -> Option<ArchiveExport> {
        let entries = self.catalog.list(group).ok()?;
        let entry = entries.into_iter().find(|e| e.last_index == index && e.last_term == term)?;
        Some(ArchiveExport {
            position: LogPosition {
                group: entry.group,
                term: entry.last_term,
                index: entry.last_index,
                snapshot_id: entry.snapshot_id.clone(),
            },
            size: entry.size,
            sha256_hex: entry.sha256_hex,
            data_path_hint: entry.dir.join("data.bin").display().to_string(),
        })
    }

    fn read_bytes_at(&self, group: GroupId, index: u64, term: u64) -> Option<Vec<u8>> {
        let entries = self.catalog.list(group).ok()?;
        let entry = entries.into_iter().find(|e| e.last_index == index && e.last_term == term)?;
        self.catalog.read(group, &entry.snapshot_id).ok().flatten()
    }

    fn snapshot(&self) -> ArchiveSnapshot {
        ArchiveSnapshot {
            plugin: self.name(),
            positions: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_and_export_positions() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(SnapshotCatalog::new(dir.path(), 3));
        catalog
            .write(0, 10, 1, "10-1", b"snap-a")
            .expect("write");
        catalog
            .write(0, 20, 2, "20-2", b"snap-b")
            .expect("write");

        let archive = CatalogArchive::new(catalog);
        let positions = archive.list_positions(0);
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].index, 10);
        assert_eq!(positions[1].index, 20);

        let export = archive.export_at(0, 20, 2).expect("export");
        assert_eq!(export.size, 6);
        assert!(!export.sha256_hex.is_empty());

        let bytes = archive.read_bytes_at(0, 20, 2).expect("bytes");
        assert_eq!(bytes, b"snap-b");
    }
}
