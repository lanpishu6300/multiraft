//! File-backed `RaftLogStorage` under `{data_dir}/`.
//!
//! Persists log entries + hard state (vote, committed, last_purged) as JSON so a
//! process restart can rebuild Raft state via log replay. Snapshot storage is
//! out of scope for this store (SM rebuilds from the log).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::io;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use futures::lock::Mutex;
use openraft::LogState;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

const HARD_STATE_FILE: &str = "hard_state.json";
const LOG_FILE: &str = "log.json";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(bound = "")]
struct HardState<C: RaftTypeConfig>
where
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    last_purged_log_id: Option<LogIdOf<C>>,
    committed: Option<LogIdOf<C>>,
    vote: Option<VoteOf<C>>,
}

#[derive(Debug)]
struct FileLogInner<C: RaftTypeConfig> {
    dir: PathBuf,
    last_purged_log_id: Option<LogIdOf<C>>,
    log: BTreeMap<u64, C::Entry>,
    committed: Option<LogIdOf<C>>,
    vote: Option<VoteOf<C>>,
}

/// Raft log store that mirrors the memory store and flushes to disk.
#[derive(Debug, Clone)]
pub struct FileLogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<FileLogInner<C>>>,
}

impl<C> FileLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    /// Open (or create) a durable log directory.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let hard = load_hard_state::<C>(&dir)?;
        let log = load_log::<C>(&dir)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(FileLogInner {
                dir,
                last_purged_log_id: hard.last_purged_log_id,
                log,
                committed: hard.committed,
                vote: hard.vote,
            })),
        })
    }
}

impl<C> FileLogInner<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    fn persist_hard_state(&self) -> io::Result<()> {
        let hs = HardState::<C> {
            last_purged_log_id: self.last_purged_log_id.clone(),
            committed: self.committed.clone(),
            vote: self.vote.clone(),
        };
        atomic_write_json(self.dir.join(HARD_STATE_FILE), &hs)
    }

    fn persist_log(&self) -> io::Result<()> {
        let entries: Vec<C::Entry> = self.log.values().cloned().collect();
        atomic_write_json(self.dir.join(LOG_FILE), &entries)
    }

    fn persist_all(&self) -> io::Result<()> {
        self.persist_hard_state()?;
        self.persist_log()?;
        Ok(())
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error> {
        Ok(self
            .log
            .range(range)
            .map(|(_, val)| val.clone())
            .collect())
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let last = self.log.iter().next_back().map(|(_, ent)| ent.log_id());
        let last_purged = self.last_purged_log_id.clone();
        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        self.committed = committed;
        self.persist_hard_state()
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
        self.vote = Some(vote.clone());
        self.persist_hard_state()
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        Ok(self.vote.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        for entry in entries {
            self.log.insert(entry.index(), entry);
        }
        let res = self.persist_all();
        callback.io_completed(res.as_ref().map(|_| ()).map_err(|e| io::Error::new(e.kind(), e.to_string())));
        res
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        let start_index = match last_log_id {
            Some(log_id) => log_id.index() + 1,
            None => 0,
        };
        let keys: Vec<u64> = self.log.range(start_index..).map(|(k, _)| *k).collect();
        for key in keys {
            self.log.remove(&key);
        }
        self.persist_log()
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }
        let keys: Vec<u64> = self.log.range(..=log_id.index()).map(|(k, _)| *k).collect();
        for key in keys {
            self.log.remove(&key);
        }
        self.persist_all()
    }
}

fn load_hard_state<C>(dir: &Path) -> io::Result<HardState<C>>
where
    C: RaftTypeConfig,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    let path = dir.join(HARD_STATE_FILE);
    if !path.exists() {
        return Ok(HardState::default());
    }
    let bytes = fs::read(&path)?;
    serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn load_log<C>(dir: &Path) -> io::Result<BTreeMap<u64, C::Entry>>
where
    C: RaftTypeConfig,
    C::Entry: DeserializeOwned,
{
    let path = dir.join(LOG_FILE);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = fs::read(&path)?;
    let entries: Vec<EntryOf<C>> =
        serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut map = BTreeMap::new();
    for ent in entries {
        map.insert(ent.index(), ent);
    }
    Ok(map)
}

/// Write `value` to `path` so that a crash leaves either the previous file or
/// the new one, and so the new content survives power loss.
///
/// `fs::write` + `fs::rename` gives atomicity but not durability: both return
/// once the data is in the OS page cache, and Linux may hold it there for tens
/// of seconds. Openraft requires the vote to be on disk before `save_vote`
/// returns and the entries to be on disk before `IOFlushed::io_completed` is
/// called, so the temp file is fsynced before the rename and the directory is
/// fsynced after it — a rename is itself a directory update, and without that
/// second fsync the renamed file can be missing entirely after a crash.
fn atomic_write_json<T: Serialize>(path: PathBuf, value: &T) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let tmp = path.with_extension("json.tmp");

    let mut file = fs::File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);

    fs::rename(&tmp, &path)?;

    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    sync_dir(dir)
}

/// Flush the directory entry created by a rename.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

/// Windows has no fsync-a-directory equivalent; `MoveFileEx` is durable enough
/// there once the file itself has been flushed.
#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::io;
    use std::ops::RangeBounds;

    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::RaftTypeConfig;
    use openraft::alias::LogIdOf;
    use openraft::alias::VoteOf;
    use openraft::storage::IOFlushed;
    use openraft::storage::RaftLogStorage;
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use crate::log_file::FileLogStore;

    impl<C> RaftLogReader<C> for FileLogStore<C>
    where
        C: RaftTypeConfig,
        C::Entry: Clone + Serialize + DeserializeOwned,
        LogIdOf<C>: Serialize + DeserializeOwned,
        VoteOf<C>: Serialize + DeserializeOwned,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, io::Error> {
            let mut inner = self.inner.lock().await;
            inner.try_get_log_entries(range).await
        }

        async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
            let mut inner = self.inner.lock().await;
            inner.read_vote().await
        }
    }

    impl<C> RaftLogStorage<C> for FileLogStore<C>
    where
        C: RaftTypeConfig,
        C::Entry: Clone + Serialize + DeserializeOwned,
        LogIdOf<C>: Serialize + DeserializeOwned,
        VoteOf<C>: Serialize + DeserializeOwned,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
            let mut inner = self.inner.lock().await;
            inner.get_log_state().await
        }

        async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            inner.save_committed(committed).await
        }

        async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
            let mut inner = self.inner.lock().await;
            inner.read_committed().await
        }

        async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            inner.save_vote(vote).await
        }

        async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let mut inner = self.inner.lock().await;
            inner.append(entries, callback).await
        }

        async fn truncate_after(
            &mut self,
            last_log_id: Option<LogIdOf<C>>,
        ) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            inner.truncate_after(last_log_id).await
        }

        async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            inner.purge(log_id).await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}
