//! File-backed `RaftLogStorage` under `{data_dir}/`.
//!
//! Log entries use append-only length-prefixed bincode (`log.bin`) so each Raft
//! append is O(new entries) with compact serialization. Optional group-commit
//! batches multiple appends into one `write_all` within a microsecond window.
//! At [`FileLogSyncLevel::Os`], optional stream buffering (`stream_buf_bytes` /
//! `stream_flush_ms`) and/or [`FileLogStore::open_with_coalesce`] defer flush to
//! a background timer — **without sleeping on the append task** (deep pipeline
//! must not pay `1/coalesce_us` per entry). [`FileLogSyncLevel`] controls
//! post-write durability (page cache / data sync / full sync). Truncate / purge
//! force-flush then rewrite the file. Hard state stays in `hard_state.json`.
//! Legacy `log.json` / `log.ndjson` are loaded once and migrated on open.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::lock::Mutex;
use multiraft_core::FileLogSyncLevel;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::LogState;
use openraft::RaftTypeConfig;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Notify;

use crate::durability::{self, atomic_replace, hard_state_sync_level};

const HARD_STATE_FILE: &str = "hard_state.json";
const LOG_FILE_LEGACY: &str = "log.json";
const LOG_NDJSON: &str = "log.ndjson";
const LOG_BIN: &str = "log.bin";
/// Default `BufWriter` capacity when stream batching is disabled.
const DEFAULT_WRITER_CAP: usize = 64 * 1024;

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

struct FileLogInner<C: RaftTypeConfig> {
    dir: PathBuf,
    last_purged_log_id: Option<LogIdOf<C>>,
    log: BTreeMap<u64, C::Entry>,
    committed: Option<LogIdOf<C>>,
    vote: Option<VoteOf<C>>,
    /// Kept open across appends; wraps the append handle in a large `BufWriter`.
    log_writer: Option<BufWriter<File>>,
    writer_cap: usize,
    /// Buffered length-prefixed entries awaiting one batched flush.
    pending_buf: Vec<u8>,
    pending_callbacks: Vec<IOFlushed<C>>,
    sync_level: FileLogSyncLevel,
    /// `committed` changed since last hard_state persist (debounced onto log flush).
    hard_state_dirty: bool,
}

impl<C: RaftTypeConfig> std::fmt::Debug for FileLogInner<C>
where
    C::Entry: std::fmt::Debug,
    LogIdOf<C>: std::fmt::Debug,
    VoteOf<C>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileLogInner")
            .field("dir", &self.dir)
            .field("last_purged_log_id", &self.last_purged_log_id)
            .field("log_len", &self.log.len())
            .field("committed", &self.committed)
            .field("vote", &self.vote)
            .field("pending_bytes", &self.pending_buf.len())
            .field("pending_callbacks", &self.pending_callbacks.len())
            .field("sync_level", &self.sync_level)
            .field("hard_state_dirty", &self.hard_state_dirty)
            .finish()
    }
}

/// Stream / timed group-commit tuning (see [`FileLogStore::open_with_full_options`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileLogStreamOptions {
    /// Pending bytes threshold before a forced flush (`0` = none for Os stream;
    /// with [`Self::hold_overlap`], `0` still applies a 1 MiB safety cap).
    pub stream_buf_bytes: usize,
    /// Max hold time before a background Os flush (`0` = no timer-only defer).
    pub stream_flush_ms: u64,
    /// When true (with `coalesce_us > 0`): overlapping appends do not flush early;
    /// only the coalesce timer / size / count caps flush (timed group-commit).
    pub hold_overlap: bool,
}

/// Shared stream/coalesce control for cloned store handles.
struct StreamControl {
    coalesce_us: u64,
    sync_level: FileLogSyncLevel,
    stream: FileLogStreamOptions,
    coalesce_notify: Arc<Notify>,
    stream_notify: Arc<Notify>,
    flusher_running: AtomicBool,
}

/// Safety valve when [`FileLogStreamOptions::hold_overlap`] is set.
const HOLD_OVERLAP_MAX_CALLBACKS: usize = 8_192;
const HOLD_OVERLAP_DEFAULT_BUF_CAP: usize = 1_048_576;

/// Pending bytes + callbacks taken under the state lock for durable IO.
struct FlushBatch<C: RaftTypeConfig> {
    buf: Vec<u8>,
    callbacks: Vec<IOFlushed<C>>,
    sync_level: FileLogSyncLevel,
}

fn complete_flush_callbacks<C: RaftTypeConfig>(callbacks: Vec<IOFlushed<C>>, res: &io::Result<()>) {
    match res {
        Ok(()) => {
            for cb in callbacks {
                cb.io_completed(Ok(()));
            }
        }
        Err(e) => {
            let kind = e.kind();
            let msg = e.to_string();
            for cb in callbacks {
                cb.io_completed(Err(io::Error::new(kind, msg.clone())));
            }
        }
    }
}

/// Drain pending batches: release state lock during `write`+sync so appends pipeline.
async fn flush_pipeline<C>(
    inner: &Arc<Mutex<FileLogInner<C>>>,
    io_gate: &Arc<Mutex<()>>,
) -> io::Result<()>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    loop {
        let batch = {
            let mut guard = inner.lock().await;
            guard.take_flush_batch()
        };
        let Some(batch) = batch else {
            return Ok(());
        };

        let _io = io_gate.lock().await;
        let res = {
            let mut guard = inner.lock().await;
            guard.ensure_log_writer()?;
            let mut writer = guard
                .log_writer
                .take()
                .expect("ensure_log_writer left a writer");
            drop(guard);

            let write_res = (|| {
                writer.write_all(&batch.buf)?;
                writer.flush()?;
                durability::sync_file(writer.get_mut(), batch.sync_level)
            })();

            let mut guard = inner.lock().await;
            guard.log_writer = Some(writer);
            if write_res.is_ok() && guard.hard_state_dirty {
                if let Err(e) = guard.persist_hard_state() {
                    complete_flush_callbacks(
                        batch.callbacks,
                        &Err(io::Error::new(e.kind(), e.to_string())),
                    );
                    return Err(e);
                }
            }
            write_res
        };

        complete_flush_callbacks(batch.callbacks, &res);
        res?;
        // Loop: drain work that arrived while we held only io_gate / were syncing.
    }
}

impl StreamControl {
    fn new(coalesce_us: u64, sync_level: FileLogSyncLevel, stream: FileLogStreamOptions) -> Self {
        Self {
            coalesce_us,
            sync_level,
            stream,
            coalesce_notify: Arc::new(Notify::new()),
            stream_notify: Arc::new(Notify::new()),
            flusher_running: AtomicBool::new(false),
        }
    }

    /// Deferred flush enabled: Os stream/coalesce, or Data/All group-commit (`coalesce_us`).
    fn defer_enabled(&self) -> bool {
        if self.coalesce_us > 0 {
            return true;
        }
        self.sync_level == FileLogSyncLevel::Os
            && (self.stream.stream_buf_bytes > 0 || self.stream.stream_flush_ms > 0)
    }

    fn hit_force_flush_cap(&self, pending_len: usize, pending_callbacks: usize) -> bool {
        if self.stream.stream_buf_bytes > 0 && pending_len >= self.stream.stream_buf_bytes {
            return true;
        }
        if self.stream.hold_overlap {
            if pending_callbacks >= HOLD_OVERLAP_MAX_CALLBACKS {
                return true;
            }
            let buf_cap = if self.stream.stream_buf_bytes > 0 {
                self.stream.stream_buf_bytes
            } else {
                HOLD_OVERLAP_DEFAULT_BUF_CAP
            };
            if pending_len >= buf_cap {
                return true;
            }
        }
        false
    }

    /// Whether this append should defer to the background flusher.
    #[cfg_attr(not(test), allow(dead_code))]
    fn should_defer(&self, pending_len: usize, pending_callbacks: usize) -> bool {
        if !self.defer_enabled() || self.hit_force_flush_cap(pending_len, pending_callbacks) {
            return false;
        }
        if self.stream.hold_overlap && self.coalesce_us > 0 {
            // Timed group-commit: hold across overlapping appends until timer/cap.
            return true;
        }
        // Default: only solo appends defer; overlap flushes immediately (one sync).
        pending_callbacks <= 1
            && !(self.stream.stream_buf_bytes > 0 && pending_len >= self.stream.stream_buf_bytes)
    }

    /// Timer for the background flusher (`0` = notify-only).
    fn flush_wait_us(&self) -> u64 {
        let from_coalesce = self.coalesce_us;
        let from_stream = self.stream.stream_flush_ms.saturating_mul(1000);
        match (from_coalesce > 0, from_stream > 0) {
            (true, true) => from_coalesce.min(from_stream),
            (true, false) => from_coalesce,
            (false, true) => from_stream,
            (false, false) => 0,
        }
    }

    fn ensure_flusher<C: RaftTypeConfig>(
        self: &Arc<Self>,
        inner: Arc<Mutex<FileLogInner<C>>>,
        io_gate: Arc<Mutex<()>>,
    ) where
        C::Entry: Clone + Serialize + DeserializeOwned,
        LogIdOf<C>: Serialize + DeserializeOwned,
        VoteOf<C>: Serialize + DeserializeOwned,
    {
        if !self.defer_enabled() {
            return;
        }
        if self
            .flusher_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let ctrl = Arc::clone(self);
        tokio::spawn(async move {
            ctrl.run_flusher(inner, io_gate).await;
        });
    }

    async fn run_flusher<C: RaftTypeConfig>(
        self: Arc<Self>,
        inner: Arc<Mutex<FileLogInner<C>>>,
        io_gate: Arc<Mutex<()>>,
    ) where
        C::Entry: Clone + Serialize + DeserializeOwned,
        LogIdOf<C>: Serialize + DeserializeOwned,
        VoteOf<C>: Serialize + DeserializeOwned,
    {
        // Hold only a weak ref so dropping the last FileLogStore stops the loop.
        let inner_weak = Arc::downgrade(&inner);
        drop(inner);
        loop {
            let wait_us = self.flush_wait_us();
            if self.stream.hold_overlap && wait_us > 0 {
                // Timed group-commit must not wake on Notify: openraft clones the
                // log store (e.g. get_log_reader) and Drop would otherwise punch
                // through the coalesce window. Cap flushes stay in-line on append;
                // teardown may wait up to one window for the flusher to exit.
                tokio::time::sleep(std::time::Duration::from_micros(wait_us)).await;
            } else if wait_us > 0 {
                tokio::select! {
                    _ = self.stream_notify.notified() => {}
                    _ = self.coalesce_notify.notified() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_micros(wait_us)) => {}
                }
            } else {
                tokio::select! {
                    _ = self.stream_notify.notified() => {}
                    _ = self.coalesce_notify.notified() => {}
                }
            }
            let Some(inner) = inner_weak.upgrade() else {
                break;
            };
            let _ = flush_pipeline(&inner, &io_gate).await;
        }
        self.flusher_running.store(false, Ordering::Release);
    }
}

/// Raft log store that mirrors the memory store and flushes to disk.
#[derive(Clone)]
pub struct FileLogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<FileLogInner<C>>>,
    control: Arc<StreamControl>,
    /// Serializes durable `write`+sync. The state mutex is **released** during that
    /// IO so overlapping `append`s can accumulate the next group-commit batch
    /// (disk pipeline / deeper outstanding `IOFlushed`).
    io_gate: Arc<Mutex<()>>,
}

impl<C: RaftTypeConfig> Drop for FileLogStore<C> {
    fn drop(&mut self) {
        // Only the last handle should wake the flusher. Transient `clone()`/`Drop`
        // pairs (common in openraft storage plumbing) must not interrupt a timed
        // group-commit window (`hold_overlap`).
        if Arc::strong_count(&self.inner) == 1 {
            self.control.stream_notify.notify_waiters();
            self.control.coalesce_notify.notify_waiters();
        }
    }
}

impl<C: RaftTypeConfig> std::fmt::Debug for FileLogStore<C>
where
    C::Entry: std::fmt::Debug,
    LogIdOf<C>: std::fmt::Debug,
    VoteOf<C>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileLogStore")
            .field("coalesce_us", &self.control.coalesce_us)
            .field("sync_level", &self.control.sync_level)
            .field("stream", &self.control.stream)
            .finish()
    }
}

impl<C> FileLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    /// Open (or create) a durable log directory (immediate flush, OS page-cache sync).
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_options(dir, 0, FileLogSyncLevel::Os)
    }

    /// Open with a group-commit coalesce window in microseconds (OS sync level).
    pub fn open_with_coalesce(dir: impl AsRef<Path>, coalesce_us: u64) -> io::Result<Self> {
        Self::open_with_options(dir, coalesce_us, FileLogSyncLevel::Os)
    }

    /// Open with group-commit window and local disk sync strength.
    pub fn open_with_options(
        dir: impl AsRef<Path>,
        coalesce_us: u64,
        sync_level: FileLogSyncLevel,
    ) -> io::Result<Self> {
        Self::open_with_full_options(
            dir,
            coalesce_us,
            sync_level,
            FileLogStreamOptions::default(),
        )
    }

    /// Open with group-commit, sync level, and optional Os-level stream batching.
    pub fn open_with_full_options(
        dir: impl AsRef<Path>,
        coalesce_us: u64,
        sync_level: FileLogSyncLevel,
        stream: FileLogStreamOptions,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let hard = load_hard_state::<C>(&dir)?;
        let log = load_log::<C>(&dir)?;
        let writer_cap = stream.stream_buf_bytes.max(DEFAULT_WRITER_CAP);

        Ok(Self {
            inner: Arc::new(Mutex::new(FileLogInner {
                dir,
                last_purged_log_id: hard.last_purged_log_id,
                log,
                committed: hard.committed,
                vote: hard.vote,
                log_writer: None,
                writer_cap,
                pending_buf: Vec::with_capacity(4096),
                pending_callbacks: Vec::new(),
                sync_level,
                hard_state_dirty: false,
            })),
            control: Arc::new(StreamControl::new(coalesce_us, sync_level, stream)),
            io_gate: Arc::new(Mutex::new(())),
        })
    }

    pub fn sync_level(&self) -> FileLogSyncLevel {
        self.control.sync_level
    }

    pub fn stream_options(&self) -> FileLogStreamOptions {
        self.control.stream
    }
}

impl<C> FileLogInner<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    fn persist_hard_state(&mut self) -> io::Result<()> {
        let hs = HardState::<C> {
            last_purged_log_id: self.last_purged_log_id.clone(),
            committed: self.committed.clone(),
            vote: self.vote.clone(),
        };
        let bytes =
            serde_json::to_vec(&hs).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        atomic_replace(
            &self.dir.join(HARD_STATE_FILE),
            &bytes,
            hard_state_sync_level(self.sync_level),
        )?;
        self.hard_state_dirty = false;
        Ok(())
    }

    /// Encode entries directly into `pending_buf` (no intermediate Vec).
    fn encode_entries_into(buf: &mut Vec<u8>, entries: &[C::Entry]) -> io::Result<()> {
        for ent in entries {
            let raw = bincode::serialize(ent)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let len = u32::try_from(raw.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "entry too large"))?;
            buf.reserve(4 + raw.len());
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&raw);
        }
        Ok(())
    }

    fn ensure_log_writer(&mut self) -> io::Result<&mut BufWriter<File>> {
        if self.log_writer.is_none() {
            let path = self.dir.join(LOG_BIN);
            let f = OpenOptions::new().create(true).append(true).open(&path)?;
            self.log_writer = Some(BufWriter::with_capacity(self.writer_cap, f));
        }
        Ok(self.log_writer.as_mut().unwrap())
    }

    /// Flush coalesced pending bytes and complete all pending [`IOFlushed`] callbacks.
    ///
    /// Holds the state lock for the entire durable IO — used by truncate/rewrite.
    /// Hot path uses [`flush_pipeline`] so appends can queue during sync.
    fn flush_pending(&mut self) -> io::Result<()> {
        let Some(batch) = self.take_flush_batch() else {
            return Ok(());
        };
        let res = (|| {
            let writer = self.ensure_log_writer()?;
            writer.write_all(&batch.buf)?;
            writer.flush()?;
            durability::sync_file(writer.get_mut(), batch.sync_level)
        })();
        complete_flush_callbacks(batch.callbacks, &res);
        if res.is_ok() && self.hard_state_dirty {
            self.persist_hard_state()?;
        }
        res
    }

    fn take_flush_batch(&mut self) -> Option<FlushBatch<C>> {
        if self.pending_buf.is_empty() {
            debug_assert!(self.pending_callbacks.is_empty());
            return None;
        }
        Some(FlushBatch {
            buf: std::mem::take(&mut self.pending_buf),
            callbacks: std::mem::take(&mut self.pending_callbacks),
            sync_level: self.sync_level,
        })
    }

    /// Full rewrite used after truncate / purge (and legacy migration).
    fn rewrite_log(&mut self) -> io::Result<()> {
        self.flush_pending()?;
        if self.hard_state_dirty {
            self.persist_hard_state()?;
        }
        self.log_writer = None;
        let path = self.dir.join(LOG_BIN);
        let tmp = path.with_extension("bin.tmp");
        {
            let mut buf = Vec::new();
            for ent in self.log.values() {
                let raw = bincode::serialize(ent)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let len = u32::try_from(raw.len())
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "entry too large"))?;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(&raw);
            }
            fs::write(&tmp, &buf)?;
            durability::sync_path(&tmp, self.sync_level)?;
        }
        fs::rename(&tmp, &path)?;
        if !matches!(self.sync_level, FileLogSyncLevel::Os) {
            durability::sync_dir(self.dir.as_path())?;
            durability::sync_path(&path, self.sync_level)?;
        }
        let _ = fs::remove_file(self.dir.join(LOG_FILE_LEGACY));
        let _ = fs::remove_file(self.dir.join(LOG_NDJSON));
        Ok(())
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error> {
        Ok(self.log.range(range).map(|(_, val)| val.clone()).collect())
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
        // Debounce: persist with the next log flush / vote / rewrite (avoids a
        // hard_state.json rewrite on every commit under propose load).
        self.committed = committed;
        self.hard_state_dirty = true;
        Ok(())
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
        let newly: Vec<_> = entries.into_iter().collect();
        if newly.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }
        Self::encode_entries_into(&mut self.pending_buf, &newly)?;
        for entry in newly {
            self.log.insert(entry.index(), entry);
        }
        self.pending_callbacks.push(callback);
        Ok(())
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
        self.rewrite_log()
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
        self.persist_hard_state()?;
        self.rewrite_log()
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
    C::Entry: Clone + DeserializeOwned + Serialize,
{
    let bin = dir.join(LOG_BIN);
    if bin.exists() {
        return load_bin::<C>(&bin);
    }

    // Migrate older formats once into log.bin.
    let mut map = BTreeMap::new();
    let ndjson = dir.join(LOG_NDJSON);
    if ndjson.exists() {
        map = load_ndjson::<C>(&ndjson)?;
    } else {
        let legacy = dir.join(LOG_FILE_LEGACY);
        if legacy.exists() {
            let bytes = fs::read(&legacy)?;
            let entries: Vec<EntryOf<C>> = serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            for ent in entries {
                map.insert(ent.index(), ent);
            }
        }
    }
    if !map.is_empty() {
        let mut tmp_store = FileLogInner::<C> {
            dir: dir.to_path_buf(),
            last_purged_log_id: None,
            log: map.clone(),
            committed: None,
            vote: None,
            log_writer: None,
            writer_cap: DEFAULT_WRITER_CAP,
            pending_buf: Vec::new(),
            pending_callbacks: Vec::new(),
            sync_level: FileLogSyncLevel::Os,
            hard_state_dirty: false,
        };
        tmp_store.rewrite_log()?;
    }
    Ok(map)
}

fn load_bin<C>(path: &Path) -> io::Result<BTreeMap<u64, C::Entry>>
where
    C: RaftTypeConfig,
    C::Entry: DeserializeOwned,
{
    let bytes = fs::read(path)?;
    let mut map = BTreeMap::new();
    let mut off = 0usize;
    while off + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + len > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: truncated frame at {}", path.display(), off),
            ));
        }
        let ent: EntryOf<C> = bincode::deserialize(&bytes[off..off + len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        off += len;
        map.insert(ent.index(), ent);
    }
    if off != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: trailing {} bytes", path.display(), bytes.len() - off),
        ));
    }
    Ok(map)
}

fn load_ndjson<C>(path: &Path) -> io::Result<BTreeMap<u64, C::Entry>>
where
    C: RaftTypeConfig,
    C::Entry: DeserializeOwned,
{
    let f = fs::File::open(path)?;
    let reader = BufReader::new(f);
    let mut map = BTreeMap::new();
    for (lineno, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let ent: EntryOf<C> = serde_json::from_str(&line).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}:{}: {e}", path.display(), lineno + 1),
            )
        })?;
        map.insert(ent.index(), ent);
    }
    Ok(map)
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::io;
    use std::ops::RangeBounds;
    use std::sync::Arc;

    use openraft::alias::LogIdOf;
    use openraft::alias::VoteOf;
    use openraft::storage::IOFlushed;
    use openraft::storage::RaftLogStorage;
    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::RaftTypeConfig;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    use super::flush_pipeline;
    use super::FileLogStore;

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
            let control = Arc::clone(&self.control);
            let inner = Arc::clone(&self.inner);
            let io_gate = Arc::clone(&self.io_gate);
            let mut guard = inner.lock().await;
            guard.append(entries, callback).await?;

            let pending_len = guard.pending_buf.len();
            let n_callbacks = guard.pending_callbacks.len();

            // Critical: when deferred flush is enabled, **never await durable IO on
            // this path**. openraft's command loop waits for `append()` to return;
            // awaiting fdatasync here caps outstanding log IO at ~1 and kills
            // group-commit. Caps still wake the flusher immediately.
            if control.defer_enabled() {
                let at_cap = control.hit_force_flush_cap(pending_len, n_callbacks);
                drop(guard);
                control.ensure_flusher(Arc::clone(&inner), Arc::clone(&io_gate));
                // `notify_one` stores a permit if the flusher has not parked yet
                // (`notify_waiters` would be lost). hold_overlap uses timer-only
                // wakes except at size/count caps.
                if at_cap || !control.stream.hold_overlap {
                    control.stream_notify.notify_one();
                    control.coalesce_notify.notify_one();
                }
                return Ok(());
            }

            drop(guard);
            flush_pipeline(&inner, &io_gate).await
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

#[cfg(test)]
mod debug_tests {
    use super::*;
    use multiraft_core::TypeConfig;

    #[test]
    fn file_log_inner_debug_fmt() {
        let inner = FileLogInner::<TypeConfig> {
            dir: PathBuf::from("/tmp/x"),
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
            log_writer: None,
            writer_cap: DEFAULT_WRITER_CAP,
            pending_buf: Vec::new(),
            pending_callbacks: Vec::new(),
            sync_level: FileLogSyncLevel::Data,
            hard_state_dirty: false,
        };
        let s = format!("{inner:?}");
        assert!(s.contains("FileLogInner"));
        assert!(s.contains("sync_level"));
    }

    #[test]
    fn stream_control_defer_and_disabled_flusher() {
        let disabled = StreamControl::new(
            0,
            FileLogSyncLevel::Data,
            FileLogStreamOptions {
                stream_buf_bytes: 1024,
                stream_flush_ms: 1,
                hold_overlap: false,
            },
        );
        // Data without coalesce: no defer (must fdatasync per append by default).
        assert!(!disabled.defer_enabled());
        assert!(!disabled.should_defer(10, 1));

        let os = StreamControl::new(
            0,
            FileLogSyncLevel::Os,
            FileLogStreamOptions {
                stream_buf_bytes: 100,
                stream_flush_ms: 1,
                hold_overlap: false,
            },
        );
        assert!(os.defer_enabled());
        assert!(!os.should_defer(10, 2)); // pipelined callbacks
        assert!(!os.should_defer(100, 1)); // at/over buf threshold
        assert!(os.should_defer(50, 1));

        let coal = StreamControl::new(50, FileLogSyncLevel::Os, FileLogStreamOptions::default());
        assert!(coal.defer_enabled());
        assert_eq!(coal.flush_wait_us(), 50);
        assert!(coal.should_defer(10, 1));

        let data_coal = StreamControl::new(
            1000,
            FileLogSyncLevel::Data,
            FileLogStreamOptions::default(),
        );
        assert!(data_coal.defer_enabled());
        assert!(data_coal.should_defer(10, 1));
        assert!(!data_coal.should_defer(10, 2));

        let hold = StreamControl::new(
            10_000,
            FileLogSyncLevel::Data,
            FileLogStreamOptions {
                stream_buf_bytes: 0,
                stream_flush_ms: 0,
                hold_overlap: true,
            },
        );
        assert!(hold.should_defer(10, 1));
        assert!(hold.should_defer(10, 64)); // hold across overlap
        assert!(hold.hit_force_flush_cap(HOLD_OVERLAP_DEFAULT_BUF_CAP, 1));

        let ctrl = Arc::new(disabled);
        let inner = Arc::new(Mutex::new(FileLogInner::<TypeConfig> {
            dir: PathBuf::from("/tmp/x"),
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
            log_writer: None,
            writer_cap: DEFAULT_WRITER_CAP,
            pending_buf: Vec::new(),
            pending_callbacks: Vec::new(),
            sync_level: FileLogSyncLevel::Data,
            hard_state_dirty: false,
        }));
        ctrl.ensure_flusher(inner, Arc::new(Mutex::new(())));
        assert!(!ctrl.flusher_running.load(Ordering::Acquire));
    }

    #[test]
    fn flusher_already_running_short_circuits() {
        let os = Arc::new(StreamControl::new(
            0,
            FileLogSyncLevel::Os,
            FileLogStreamOptions {
                stream_buf_bytes: 1024,
                stream_flush_ms: 50,
                hold_overlap: false,
            },
        ));
        os.flusher_running.store(true, Ordering::Release);
        let inner = Arc::new(Mutex::new(FileLogInner::<TypeConfig> {
            dir: PathBuf::from("/tmp/x"),
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
            log_writer: None,
            writer_cap: DEFAULT_WRITER_CAP,
            pending_buf: Vec::new(),
            pending_callbacks: Vec::new(),
            sync_level: FileLogSyncLevel::Os,
            hard_state_dirty: false,
        }));
        os.ensure_flusher(inner, Arc::new(Mutex::new(())));
        assert!(os.flusher_running.load(Ordering::Acquire));
    }
}
