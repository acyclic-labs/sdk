//! Durable local medium: one append-only record log per root.
//!
//! Every write appends one checksummed frame, a put or a removal, to `log`,
//! so a frame is whole or absent after any crash. An in-memory index, rebuilt
//! by one replay at open, maps each key to its value in the log. One process
//! owns the root, and one lock orders every write, so compare-and-replace and
//! compare-and-remove are atomic; create never replaces an existing key.
//!
//! A write is visible at once, and [`Medium::flush`] makes every write so far
//! durable with one data flush of the log. The owner flushes on close, once
//! its last accepted write is done; a root opened after an unclean close cuts
//! a torn tail, flushes what survived and reports [`Medium::recovered`] until
//! settled, so the store repairs a closure the crash left partial.
//!
//! When the log holds more dead bytes than live ones, its live frames are
//! rewritten to a fresh log that replaces it atomically, so space is
//! reclaimed in amortised constant work per dead byte.
use crate::{Medium, MediumError, Store};
use acyclic_native_runtime::{LogFrame, spawn_persistence};
use acyclic_stream::OwnedBytes;
use std::collections::BTreeMap;
use std::io::Write as _;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// How file contents are made durable before they become observable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalDurability {
    /// Flushes file contents and the containing directory.
    #[default]
    FullFlush,
    /// Uses Apple's `F_BARRIERFSYNC`; opening elsewhere fails.
    Barrier,
}

/// Local medium options.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalObjectsLimits {
    /// Synchronization policy.
    pub durability: LocalDurability,
}

/// Opening failure.
#[derive(Debug, thiserror::Error)]
pub enum LocalOpenError {
    /// The options are unsupported on this platform.
    #[error("invalid local Objects options")]
    Invalid,
    /// The root holds something other than a v1 store.
    #[error("local Objects root is not a v1 store")]
    Corrupt,
    /// The root holds the earlier one-file-per-key layout, which this version
    /// does not open; it has no migration.
    #[error("local Objects root holds the earlier one-file-per-key layout")]
    UnsupportedLayout,
    /// Another process owns the root.
    #[error("local Objects root is already owned")]
    AlreadyOwned,
    /// The blocking executor is unavailable.
    #[error("local Objects executor unavailable")]
    Unavailable,
    /// Filesystem failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

const FORMAT: &[u8] = b"acyclic-objects-log-v1\n";
/// The one-file-per-key layout's format marker.
const FILE_PER_KEY_FORMAT: &[u8] = b"acyclic-objects-local-v1\n";
/// Present while an owner may hold unflushed writes.
const OPEN: &str = "OPEN";
const LOG: &str = "log";
/// A compaction's replacement log, until it is renamed over [`LOG`].
const NEXT: &str = "log.next";
/// Dead bytes below this are never compacted: a chunk of work, not a bound.
const COMPACTION_CHUNK: u64 = 1 << 20;

/// The log's frame: an operation, the key and, for a put, the value.
const LOG_FRAME: LogFrame = LogFrame {
    domain: b"acyclic-objects-log-frame-v1\0",
    wide: true,
    minimum_body: 1 + 4,
    maximum_body: u64::MAX - 64,
};
const PUT: u8 = 1;
const REMOVE: u8 = 2;
/// Operation and key length, before the key.
const BODY_HEADER: u64 = 1 + 4;

/// The durable local medium.
pub struct LocalMedium {
    shared: Arc<Shared>,
}

/// One durable local account over a [`LocalMedium`].
pub type LocalObjects = crate::Account<LocalMedium>;

impl crate::Account<LocalMedium> {
    /// Opens (creating if empty) the single-account store at `root`.
    pub async fn open(
        root: impl AsRef<Path>,
        limits: LocalObjectsLimits,
        anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
    ) -> Result<Self, LocalOpenError> {
        let medium = LocalMedium::open(root, limits, anchor).await?;
        Store::new(medium)
            .account(b"local")
            .map_err(|_| LocalOpenError::Invalid)
    }
}

/// Submits `work` at once, so callers collecting several submit them
/// together and await them after. The key is an opaque address in the original
/// invocation, never the caller's object name or prefix. The runtime job identity
/// distinguishes concurrent invocations even when an address is reused.
fn blocking<T: Send + 'static, F: FnOnce() -> std::io::Result<T> + Send + 'static>(
    medium: &LocalMedium,
    operation: &'static str,
    key: usize,
    work: F,
) -> impl Future<Output = std::io::Result<T>> + use<T, F> {
    let submitted = spawn_persistence(
        "objects",
        operation,
        &format_args!("{:p}/{key:x}", Arc::as_ptr(&medium.shared)),
        work,
    );
    async move { submitted.await? }
}

fn native(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}

/// Where a live key's value lies in the log.
#[derive(Clone, Copy, Debug)]
struct Slot {
    /// The frame's first byte.
    frame: u64,
    /// The frame's bytes.
    bytes: u64,
    /// The value's first byte.
    value: u64,
    /// The value's bytes.
    length: u64,
}

/// The log and its index, under the one lock that orders every write.
struct Log {
    file: Arc<std::fs::File>,
    /// Where the next frame goes.
    end: u64,
    index: BTreeMap<String, Slot>,
    /// Frame bytes of live puts; the rest of the log is dead.
    live: u64,
    /// Writes appended so far, compared against [`Shared::synced`].
    written: u64,
}

/// What every accepted write holds: the root closes, everything flushed, the
/// marker removed and the lock released, only once the last lets go.
struct Shared {
    root: PathBuf,
    durability: LocalDurability,
    log: Mutex<Log>,
    /// Writes made durable so far.
    synced: AtomicU64,
    /// Whether the root opened unclean and its repair has not settled.
    recovered: AtomicBool,
    /// Whether a write or flush failed after touching the log. The log may
    /// then end in a partial frame, or a flush may have lost writes, so the
    /// root refuses every later write and flush and never closes clean: the
    /// next open recovers it.
    stopped: AtomicBool,
    #[cfg(test)]
    counts: Counts,
    /// The root's exclusive lock. It unlocks before its file closes, so a
    /// child forked meanwhile, which shares the open file description until
    /// it execs, never keeps the root owned after this store drops.
    lock: acyclic_native_runtime::FileLock,
    _anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
}

/// Files created and flushes issued, so a test counts a workload's cost.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Counts {
    pub(crate) created: std::sync::atomic::AtomicUsize,
    pub(crate) flushed: std::sync::atomic::AtomicUsize,
}

impl Drop for Shared {
    /// A clean close of a settled root leaves nothing unflushed, so the next
    /// open trusts it; an unsettled or stopped one stays marked unclean.
    fn drop(&mut self) {
        let settled = !*self.recovered.get_mut();
        let closed = self.sync().and_then(|()| {
            if !settled {
                return Ok(());
            }
            std::fs::remove_file(self.root.join(OPEN))?;
            self.sync_directory()
        });
        // A failed close leaves the marker, and the next open recovers.
        drop(closed);
    }
}

impl Shared {
    fn log(&self) -> MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn sync_file(&self, file: &std::fs::File) -> std::io::Result<()> {
        #[cfg(test)]
        self.counts.flushed.fetch_add(1, Ordering::Relaxed);
        if cfg!(target_vendor = "apple") {
            acyclic_native_runtime::sync_file(file, native(self.durability), None)
        } else {
            acyclic_native_runtime::sync_data(file, native(self.durability))
        }
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        #[cfg(test)]
        self.counts.flushed.fetch_add(1, Ordering::Relaxed);
        acyclic_native_runtime::sync_parent(&self.root, native(self.durability))
    }

    /// Fails once the root has stopped.
    fn usable(&self) -> std::io::Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(std::io::Error::other(
                "local Objects root stopped after a failed write; reopen it to recover",
            ));
        }
        Ok(())
    }

    /// Stops the root if `result`, a write or flush of the log, failed.
    fn stop_on_error<T>(&self, result: std::io::Result<T>) -> std::io::Result<T> {
        if result.is_err() {
            self.stopped.store(true, Ordering::Release);
        }
        result
    }

    fn created(&self) {
        #[cfg(test)]
        self.counts.created.fetch_add(1, Ordering::Relaxed);
    }

    /// Makes every write appended before it durable with one data flush; a
    /// flush with nothing new is free.
    fn sync(&self) -> std::io::Result<()> {
        self.usable()?;
        let (file, written) = {
            let log = self.log();
            (Arc::clone(&log.file), log.written)
        };
        if self.synced.load(Ordering::Acquire) >= written {
            return Ok(());
        }
        self.stop_on_error(self.sync_file(&file))?;
        self.synced.fetch_max(written, Ordering::AcqRel);
        Ok(())
    }

    /// Appends one frame, returning where it lies.
    fn append(&self, log: &mut Log, frame: &[u8]) -> std::io::Result<u64> {
        self.usable()?;
        let start = log.end;
        self.stop_on_error(acyclic_native_runtime::persistence_syscall(|| {
            acyclic_native_runtime::write_all_at(&log.file, start, frame, None)
        }))?;
        log.end += frame.len() as u64;
        log.written += 1;
        Ok(start)
    }

    fn put(&self, log: &mut Log, key: &str, value: &[u8]) -> std::io::Result<()> {
        let frame = encode(PUT, key, value)?;
        let start = self.append(log, &frame)?;
        let slot = Slot {
            frame: start,
            bytes: frame.len() as u64,
            value: start + value_offset(key),
            length: value.len() as u64,
        };
        log.live += slot.bytes;
        if let Some(old) = log.index.insert(key.to_owned(), slot) {
            log.live -= old.bytes;
            self.compact_if_due(log)?;
        }
        Ok(())
    }

    fn remove(&self, log: &mut Log, key: &str) -> std::io::Result<bool> {
        if !log.index.contains_key(key) {
            return Ok(false);
        }
        self.append(log, &encode(REMOVE, key, &[])?)?;
        if let Some(old) = log.index.remove(key) {
            log.live -= old.bytes;
        }
        self.compact_if_due(log)?;
        Ok(true)
    }

    fn compact_if_due(&self, log: &mut Log) -> std::io::Result<()> {
        let dead = log.end - log.live;
        if dead < COMPACTION_CHUNK || dead <= log.live {
            return Ok(());
        }
        self.stop_on_error(self.compact(log))
    }

    /// Rewrites the live frames to a fresh log and renames it over the old
    /// one: a crash leaves either whole, and every write is durable after.
    fn compact(&self, log: &mut Log) -> std::io::Result<()> {
        let next = self.root.join(NEXT);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&next)?;
        self.created();
        let mut end = 0;
        let mut index = BTreeMap::new();
        let mut frame = Vec::new();
        for (key, slot) in &log.index {
            frame.resize(
                usize::try_from(slot.bytes).map_err(std::io::Error::other)?,
                0,
            );
            read_exact_at(&log.file, slot.frame, &mut frame)?;
            acyclic_native_runtime::write_all_at(&file, end, &frame, None)?;
            let moved = Slot {
                frame: end,
                value: end + (slot.value - slot.frame),
                ..*slot
            };
            index.insert(key.clone(), moved);
            end += slot.bytes;
        }
        self.sync_file(&file)?;
        acyclic_native_runtime::durable_rename(
            &next,
            &self.root.join(LOG),
            acyclic_native_runtime::RenameMode::Replace,
            None,
        )?;
        #[cfg(test)]
        self.counts.flushed.fetch_add(1, Ordering::Relaxed);
        log.file = Arc::new(file);
        log.end = end;
        log.index = index;
        self.synced.fetch_max(log.written, Ordering::AcqRel);
        Ok(())
    }
}

/// The offset of a put's value within its frame.
fn value_offset(key: &str) -> u64 {
    LOG_FRAME.prefix_bytes() as u64 + BODY_HEADER + key.len() as u64
}

fn encode(operation: u8, key: &str, value: &[u8]) -> std::io::Result<Vec<u8>> {
    let length = u32::try_from(key.len()).map_err(|_| std::io::ErrorKind::InvalidInput)?;
    let mut frame = Vec::new();
    LOG_FRAME.encode(
        &[&[operation], &length.to_le_bytes(), key.as_bytes(), value],
        &mut frame,
    )?;
    Ok(frame)
}

/// One replayed frame: its operation, key and value range within the body.
fn decode(body: &[u8]) -> Option<(u8, &str, Range<usize>)> {
    let (&operation, rest) = body.split_first()?;
    let length = usize::try_from(u32::from_le_bytes(*rest.first_chunk::<4>()?)).ok()?;
    let (key, value) = rest.get(4..)?.split_at_checked(length)?;
    let key = std::str::from_utf8(key).ok()?;
    let value = body.len() - value.len()..body.len();
    match operation {
        PUT => Some((operation, key, value)),
        REMOVE if value.is_empty() => Some((operation, key, value)),
        _ => None,
    }
}

fn read_exact_at(file: &std::fs::File, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
    let mut done = 0;
    while let Some(rest) = out.get_mut(done..).filter(|rest| !rest.is_empty()) {
        let read = acyclic_native_runtime::read_at(file, offset + done as u64, rest)?;
        if read == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        done += read;
    }
    Ok(())
}

/// Replays `file`, `length` bytes long, into its index: the end of its last
/// whole frame, the index and its live bytes. The first invalid frame ends
/// the replay; `None` if it is not at the end.
fn replay(
    file: &std::fs::File,
    length: u64,
) -> std::io::Result<(u64, BTreeMap<String, Slot>, u64, bool)> {
    let mut reader = std::io::BufReader::new(file);
    let (mut end, mut index, mut live) = (0, BTreeMap::new(), 0);
    while end < length {
        let Some(body) = LOG_FRAME.read(&mut reader, length - end)? else {
            return Ok((end, index, live, false));
        };
        let Some((operation, key, value)) = decode(&body) else {
            return Err(std::io::ErrorKind::InvalidData.into());
        };
        let bytes = LOG_FRAME.frame_bytes(body.len() as u64);
        let previous = if operation == PUT {
            let slot = Slot {
                frame: end,
                bytes,
                value: end + LOG_FRAME.prefix_bytes() as u64 + value.start as u64,
                length: value.len() as u64,
            };
            live += bytes;
            index.insert(key.to_owned(), slot)
        } else {
            index.remove(key)
        };
        if let Some(previous) = previous {
            live -= previous.bytes;
        }
        end += bytes;
    }
    Ok((end, index, live, true))
}

impl LocalMedium {
    /// Opens `root`, claiming exclusive ownership for this process.
    pub async fn open(
        root: impl AsRef<Path>,
        limits: LocalObjectsLimits,
        anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
    ) -> Result<Self, LocalOpenError> {
        if limits.durability == LocalDurability::Barrier && !cfg!(target_vendor = "apple") {
            return Err(LocalOpenError::Invalid);
        }
        let root = root.as_ref().to_path_buf();
        let durability = limits.durability;
        let shared = spawn_persistence("objects", "open", &"root", move || {
            open_root(root, durability, anchor)
        })
        .await
        .map_err(|_| LocalOpenError::Unavailable)??;
        Ok(Self {
            shared: Arc::new(shared),
        })
    }

    /// Another copy of the root lock's open file description, as a child
    /// forked during a spawn holds one until it execs.
    #[cfg(test)]
    pub(crate) fn lock_description(&self) -> std::io::Result<std::fs::File> {
        self.shared.lock.try_clone()
    }

    /// The files created and flushes issued since open.
    #[cfg(test)]
    pub(crate) fn counts(&self) -> (usize, usize) {
        (
            self.shared.counts.created.load(Ordering::Relaxed),
            self.shared.counts.flushed.load(Ordering::Relaxed),
        )
    }
}

fn open_root(
    root: PathBuf,
    durability: LocalDurability,
    anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
) -> Result<Shared, LocalOpenError> {
    let directory = |path: &Path| {
        acyclic_native_runtime::sync_parent(path, native(durability)).map_err(LocalOpenError::Io)
    };
    let file = |file: &std::fs::File| {
        acyclic_native_runtime::sync_file(file, native(durability), None)
            .map_err(LocalOpenError::Io)
    };
    std::fs::create_dir_all(&root)?;
    let format = root.join("FORMAT");
    match std::fs::read(&format) {
        Ok(bytes) if bytes == FORMAT => {}
        Ok(bytes) if bytes == FILE_PER_KEY_FORMAT => return Err(LocalOpenError::UnsupportedLayout),
        Ok(_) => return Err(LocalOpenError::Corrupt),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut created = std::fs::File::create(&format)?;
            created.write_all(FORMAT)?;
            file(&created)?;
            directory(&root)?;
        }
        Err(error) => return Err(error.into()),
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join("LOCK"))?;
    let lock = match acyclic_native_runtime::FileLock::try_acquire(
        lock,
        acyclic_native_runtime::FileLockMode::Exclusive,
    ) {
        Ok(lock) => lock,
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            return Err(LocalOpenError::AlreadyOwned);
        }
        Err(error) => return Err(error.into()),
    };
    // A compaction that never renamed its log left only garbage.
    match std::fs::remove_file(root.join(NEXT)) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        _ => {}
    }
    let marker = root.join(OPEN);
    let recovered = marker.exists();
    let log = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(LOG))?;
    let length = log.metadata()?.len();
    let (end, index, live, whole) = replay(&log, length).map_err(|error| {
        if error.kind() == std::io::ErrorKind::InvalidData {
            LocalOpenError::Corrupt
        } else {
            LocalOpenError::Io(error)
        }
    })?;
    if !whole {
        // Only an owner that never closed can have torn the log.
        if !recovered {
            return Err(LocalOpenError::Corrupt);
        }
        let tail = usize::try_from(length - end).map_err(std::io::Error::other)?;
        match acyclic_native_runtime::recover_log_tail(
            &log,
            end,
            tail,
            native(durability),
            |bytes| LOG_FRAME.is_valid(bytes),
        )? {
            acyclic_native_runtime::LogTail::Torn => {}
            acyclic_native_runtime::LogTail::Corrupt => return Err(LocalOpenError::Corrupt),
        }
    }
    if recovered {
        // The last owner may have left writes unflushed: everything that
        // survived is made durable before anything observes it.
        file(&log)?;
    } else {
        file(&std::fs::File::create(&marker)?)?;
    }
    directory(&root)?;
    Ok(Shared {
        root,
        durability,
        log: Mutex::new(Log {
            file: Arc::new(log),
            end,
            index,
            live,
            written: 0,
        }),
        synced: AtomicU64::new(0),
        recovered: AtomicBool::new(recovered),
        stopped: AtomicBool::new(false),
        #[cfg(test)]
        counts: Counts::default(),
        lock,
        _anchor: anchor,
    })
}

fn failed(error: std::io::Error) -> MediumError {
    MediumError::Failed(error)
}

#[async_trait::async_trait]
impl Medium for LocalMedium {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let (shared, key) = (Arc::clone(&self.shared), key.to_owned());
        blocking(self, "create", key.as_ptr() as usize, move || {
            let mut log = shared.log();
            if log.index.contains_key(&key) {
                return Ok(false);
            }
            shared.put(&mut log, &key, &bytes)?;
            Ok(true)
        })
        .await
        .map_err(failed)
    }

    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        let (shared, key) = (Arc::clone(&self.shared), key.to_owned());
        blocking(self, "read", key.as_ptr() as usize, move || {
            let (file, slot) = {
                let log = shared.log();
                let Some(&slot) = log.index.get(&key) else {
                    return Ok(None);
                };
                (Arc::clone(&log.file), slot)
            };
            let range = range.unwrap_or(0..slot.length);
            if range.start > range.end || range.end > slot.length {
                return Err(std::io::ErrorKind::InvalidInput.into());
            }
            let count = usize::try_from(range.end - range.start)
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
            let mut bytes = vec![0; count];
            // A log a compaction replaced stays readable through its handle.
            acyclic_native_runtime::persistence_syscall(|| {
                read_exact_at(&file, slot.value + range.start, &mut bytes)
            })?;
            Ok(Some(OwnedBytes::from(bytes)))
        })
        .await
        .map_err(failed)
    }

    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        let (shared, key) = (Arc::clone(&self.shared), key.to_owned());
        blocking(self, "remove", key.as_ptr() as usize, move || {
            shared.remove(&mut shared.log(), &key)
        })
        .await
        .map_err(failed)
    }

    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        let (shared, key, expected) = (Arc::clone(&self.shared), key.to_owned(), expected.to_vec());
        blocking(self, "replace", key.as_ptr() as usize, move || {
            let mut log = shared.log();
            if !holds(&log, &key, &expected)? {
                return Ok(false);
            }
            shared.put(&mut log, &key, &bytes)?;
            Ok(true)
        })
        .await
        .map_err(failed)
    }

    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        let (shared, key, expected) = (Arc::clone(&self.shared), key.to_owned(), expected.to_vec());
        blocking(self, "remove-if", key.as_ptr() as usize, move || {
            let mut log = shared.log();
            if !holds(&log, &key, &expected)? {
                return Ok(false);
            }
            shared.remove(&mut log, &key)
        })
        .await
        .map_err(failed)
    }

    /// One data flush covers every write so far, whichever keys are named.
    async fn flush(&self, _keys: &[String]) -> Result<(), MediumError> {
        let shared = Arc::clone(&self.shared);
        blocking(self, "flush", 0, move || shared.sync())
            .await
            .map_err(failed)
    }

    fn volatile(&self) -> bool {
        true
    }

    fn recovered(&self) -> bool {
        self.shared.recovered.load(Ordering::Acquire)
    }

    fn settle(&self) {
        self.shared.recovered.store(false, Ordering::Release);
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        if !prefix.is_empty() && !prefix.ends_with('/') {
            return Err(failed(std::io::ErrorKind::InvalidInput.into()));
        }
        let (shared, prefix, after) = (
            Arc::clone(&self.shared),
            prefix.to_owned(),
            after.map(str::to_owned),
        );
        // Writers hold the lock across disk work, so it is taken off the
        // async thread.
        blocking(self, "list", prefix.as_ptr() as usize, move || {
            let log = shared.log();
            let start = match after.as_deref() {
                Some(after) if after >= prefix.as_str() => std::ops::Bound::Excluded(after),
                _ => std::ops::Bound::Included(prefix.as_str()),
            };
            Ok(log
                .index
                .range::<str, _>((start, std::ops::Bound::Unbounded))
                .take_while(|(key, _)| key.starts_with(prefix.as_str()))
                .take(limit)
                .map(|(key, slot)| (key.clone(), slot.length))
                .collect())
        })
        .await
        .map_err(failed)
    }
}

/// Whether `key` currently holds exactly `expected`.
fn holds(log: &Log, key: &str, expected: &[u8]) -> std::io::Result<bool> {
    let Some(slot) = log.index.get(key) else {
        return Ok(false);
    };
    if slot.length != expected.len() as u64 {
        return Ok(false);
    }
    let mut current = vec![0; expected.len()];
    read_exact_at(&log.file, slot.value, &mut current)?;
    Ok(current == expected)
}

impl std::fmt::Debug for LocalMedium {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalMedium")
            .field("root", &self.shared.root)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::*;
    use futures::FutureExt as _;

    fn bytes(value: &[u8]) -> OwnedBytes {
        OwnedBytes::from(value.to_vec())
    }

    async fn contents(medium: &LocalMedium) -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        for (key, _) in medium.list("", None, usize::MAX).await.unwrap() {
            let value = medium.read(&key, None).await.unwrap().unwrap();
            out.insert(key, value.as_ref().to_vec());
        }
        out
    }

    #[tokio::test]
    async fn a_write_creates_no_file_and_a_flush_syncs_the_log_once() {
        let root = tempfile::tempdir().unwrap();
        let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None)
            .await
            .unwrap();
        for key in 0..64 {
            let key = format!("k/{key}");
            assert!(medium.create(&key, bytes(b"value")).await.unwrap());
            assert!(
                medium
                    .replace(&key, b"value", bytes(b"next"))
                    .await
                    .unwrap()
            );
        }
        assert!(medium.remove("k/0").await.unwrap());
        // Every write is visible at once, and none created a file or synced.
        assert_eq!(
            medium.read("k/1", None).await.unwrap().unwrap().as_ref(),
            b"next"
        );
        assert_eq!(medium.counts(), (0, 0));
        // One flush makes all of them durable; with nothing new, one is free.
        medium.flush(&["k/1".to_owned()]).await.unwrap();
        assert_eq!(medium.counts(), (0, 1));
        medium.flush(&[]).await.unwrap();
        assert_eq!(medium.counts(), (0, 1));
    }

    /// A crash keeps every whole frame and cuts the torn one, at every byte.
    #[tokio::test]
    async fn a_crash_keeps_exactly_the_whole_frames_at_every_cut() {
        let root = tempfile::tempdir().unwrap();
        let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None)
            .await
            .unwrap();
        let log = root.path().join(LOG);
        // The log's length after each write, and what the store held then.
        let mut states = vec![(0, BTreeMap::new())];
        assert!(medium.create("a/x", bytes(b"one")).await.unwrap());
        states.push((
            std::fs::metadata(&log).unwrap().len(),
            contents(&medium).await,
        ));
        assert!(medium.create("b/y", bytes(b"two")).await.unwrap());
        states.push((
            std::fs::metadata(&log).unwrap().len(),
            contents(&medium).await,
        ));
        assert!(
            medium
                .replace("a/x", b"one", bytes(b"three"))
                .await
                .unwrap()
        );
        states.push((
            std::fs::metadata(&log).unwrap().len(),
            contents(&medium).await,
        ));
        assert!(medium.remove_if("b/y", b"two").await.unwrap());
        states.push((
            std::fs::metadata(&log).unwrap().len(),
            contents(&medium).await,
        ));
        let written = std::fs::read(&log).unwrap();
        assert_eq!(written.len() as u64, states.last().unwrap().0);
        for cut in 0..=written.len() {
            let copy = tempfile::tempdir().unwrap();
            std::fs::write(copy.path().join("FORMAT"), FORMAT).unwrap();
            // The owner never closed.
            std::fs::write(copy.path().join(OPEN), b"").unwrap();
            std::fs::write(copy.path().join(LOG), written.get(..cut).unwrap()).unwrap();
            let reopened = LocalMedium::open(copy.path(), LocalObjectsLimits::default(), None)
                .await
                .unwrap();
            assert!(reopened.recovered());
            let (end, expected) = states
                .iter()
                .rev()
                .find(|(end, _)| *end <= cut as u64)
                .unwrap();
            assert_eq!(contents(&reopened).await, *expected, "cut at {cut}");
            assert_eq!(
                std::fs::metadata(copy.path().join(LOG)).unwrap().len(),
                *end
            );
        }
    }

    #[tokio::test]
    async fn damage_is_a_torn_tail_only_at_the_end() {
        let root = tempfile::tempdir().unwrap();
        let open =
            async || LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await;
        let medium = open().await.unwrap();
        assert!(medium.create("a/x", bytes(b"first")).await.unwrap());
        let first = std::fs::metadata(root.path().join(LOG)).unwrap().len() as usize;
        assert!(medium.create("a/y", bytes(b"second")).await.unwrap());
        drop(medium);
        let written = std::fs::read(root.path().join(LOG)).unwrap();
        let damaged = |at: usize| {
            let mut damaged = written.clone();
            *damaged.get_mut(at).unwrap() ^= 1;
            std::fs::write(root.path().join(LOG), damaged).unwrap();
        };
        // A clean close leaves no torn frame, so damage at the end is corrupt.
        damaged(written.len() - 1);
        assert!(matches!(open().await, Err(LocalOpenError::Corrupt)));
        // After an unclean close the last frame may be torn: it is cut.
        std::fs::write(root.path().join(OPEN), b"").unwrap();
        let reopened = open().await.unwrap();
        assert_eq!(
            contents(&reopened).await.into_keys().collect::<Vec<_>>(),
            ["a/x"]
        );
        drop(reopened);
        // A damaged frame followed by a whole one is never a torn append.
        std::fs::write(root.path().join(OPEN), b"").unwrap();
        damaged(first - 1);
        assert!(matches!(open().await, Err(LocalOpenError::Corrupt)));
    }

    /// A failed flush may have lost writes, so the root accepts nothing more
    /// and stays marked unclean; the next open recovers what reached the log.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn a_failed_flush_stops_the_root_until_it_recovers() {
        use acyclic_native_runtime::testing::{DurableFault, Schedule};
        acyclic_native_runtime::initialize_native_io_services().unwrap();
        let root = tempfile::tempdir().unwrap();
        let open =
            async || LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await;
        let medium = open().await.unwrap();
        assert!(medium.create("k/a", bytes(b"a")).await.unwrap());
        let (schedule, mut events) = Schedule::new();
        let driver = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                boundary.release();
            }
        });
        schedule.cut_at(0, DurableFault::Failed);
        assert!(schedule.scope(medium.flush(&[])).await.is_err());
        schedule.drained().await;
        driver.abort();
        let _ = driver.await;
        assert!(medium.create("k/b", bytes(b"b")).await.is_err());
        assert!(medium.flush(&[]).await.is_err());
        drop(medium);
        assert!(root.path().join(OPEN).exists());
        let reopened = open().await.unwrap();
        assert!(reopened.recovered());
        assert_eq!(
            contents(&reopened).await.into_keys().collect::<Vec<_>>(),
            ["k/a"]
        );
    }

    #[tokio::test]
    async fn a_root_of_the_file_per_key_layout_is_refused() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("FORMAT"), FILE_PER_KEY_FORMAT).unwrap();
        assert!(matches!(
            LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await,
            Err(LocalOpenError::UnsupportedLayout)
        ));
    }

    #[tokio::test]
    async fn dead_bytes_are_reclaimed_into_one_fresh_log() {
        let root = tempfile::tempdir().unwrap();
        let open =
            async || LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await;
        let medium = open().await.unwrap();
        let value = |round: u8| vec![round; 1 << 16];
        assert!(medium.create("k/key", bytes(&value(0))).await.unwrap());
        assert!(medium.create("k/kept", bytes(b"kept")).await.unwrap());
        let rounds = 20;
        for round in 1..=rounds {
            assert!(
                medium
                    .replace("k/key", &value(round - 1), bytes(&value(round)))
                    .await
                    .unwrap()
            );
        }
        // One compaction replaced the log: it holds the live frames and what
        // followed, never every dead one.
        assert_eq!(medium.counts().0, 1);
        let length = std::fs::metadata(root.path().join(LOG)).unwrap().len();
        assert!(length < 2 * COMPACTION_CHUNK, "{length}");
        let expected = contents(&medium).await;
        assert_eq!(expected["k/key"], value(rounds));
        assert_eq!(expected["k/kept"], b"kept");
        drop(medium);
        assert_eq!(contents(&open().await.unwrap()).await, expected);
    }

    #[test]
    fn local_storage_progresses_with_an_occupied_input_worker() {
        let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
        let (started, starting) = std::sync::mpsc::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let input = runtime.spawn_blocking(move || {
            started.send(()).unwrap();
            held.recv()
        });
        starting.recv().unwrap();
        runtime.block_on(async {
            // The old route cannot start until the input producer closes.
            let mut old_route = Box::pin(tokio::task::spawn_blocking(|| ()));
            assert!(old_route.as_mut().now_or_never().is_none());
            let root = tempfile::tempdir().unwrap();
            let store = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None)
                .await
                .unwrap();
            assert!(
                store
                    .create("values/a", OwnedBytes::from(b"before".to_vec()))
                    .await
                    .unwrap()
            );
            assert_eq!(
                store
                    .read("values/a", None)
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"before"
            );
            assert!(
                store
                    .replace("values/a", b"before", OwnedBytes::from(b"after".to_vec()))
                    .await
                    .unwrap()
            );
            assert_eq!(
                store
                    .read("values/a", None)
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"after"
            );
            assert!(
                !store
                    .replace("values/a", b"before", OwnedBytes::from(b"wrong".to_vec()))
                    .await
                    .unwrap()
            );
            assert!(store.remove_if("values/a", b"after").await.unwrap());
            // Dropping the sole input producer terminates the blocked read.
            drop(release);
            assert!(input.await.unwrap().is_err());
            old_route.await.unwrap();
        });
    }
}
