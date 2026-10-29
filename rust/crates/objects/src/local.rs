//! Durable local-directory medium: one immutable file per key.
//!
//! A key becomes visible only by hard-linking a complete temporary file into
//! place, so create-if-absent is atomic on every supported filesystem; until
//! flushed, a crash can lose it or leave it short or damaged. One process
//! owns the root, so one in-process lock makes compare-and-replace and
//! compare-and-remove atomic; only create competes with them, and it never
//! replaces an existing key.
//!
//! A write only publishes its name; [`Medium::flush`] makes named keys
//! durable in one batch, where a commit needs them. One owning process makes
//! the set of unflushed writes exact: it flushes them all on close, once its
//! last accepted write is done, and a root opened after an unclean close is
//! flushed whole before any write and reported [`Medium::recovered`] until
//! settled: a close before then leaves the root marked unclean.
use crate::{Medium, MediumError, Store};
use acyclic_stream::OwnedBytes;
use std::collections::{BTreeSet, HashMap};
use std::io::{Read as _, Seek as _, Write as _};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

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

const FORMAT: &[u8] = b"acyclic-objects-local-v1\n";
/// Present while an owner may hold unflushed writes.
const OPEN: &str = "OPEN";

/// The durable local-directory medium.
pub struct LocalMedium {
    root: PathBuf,
    durability: LocalDurability,
    conditional: Arc<Mutex<()>>,
    unflushed: Arc<Unflushed>,
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
/// together and await them after.
fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> impl Future<Output = std::io::Result<T>> {
    let submitted = acyclic_native_runtime::spawn_persistence(work);
    async move { submitted.await? }
}

fn native(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}

/// The shared native content flush; a barrier outside Apple refuses.
fn sync(file: &std::fs::File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native(durability), None)
}

/// The flush of one member of a batch. Apple orders each member with a
/// barrier and pays for one full flush per batch, in [`finish_batch`];
/// elsewhere each member flushes its data.
fn sync_member(path: &Path, durability: LocalDurability) -> std::io::Result<()> {
    // Windows flushes a file only through a handle that may write it.
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(cfg!(windows))
        .open(path)
    {
        Ok(file) => file,
        // A removal is durable once its directory is.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    sync_contents(&file, durability)
}

/// Orders `file`'s bytes before any later metadata change of it.
fn sync_contents(file: &std::fs::File, durability: LocalDurability) -> std::io::Result<()> {
    if cfg!(target_vendor = "apple") {
        acyclic_native_runtime::sync_file(file, acyclic_native_runtime::Durability::Barrier, None)
    } else {
        acyclic_native_runtime::sync_data(file, native(durability))
    }
}

fn sync_member_directory(path: &Path, durability: LocalDurability) -> std::io::Result<()> {
    if cfg!(target_vendor = "apple") {
        acyclic_native_runtime::sync_parent(path, acyclic_native_runtime::Durability::Barrier)
    } else {
        sync_directory(path, durability)
    }
}

/// Apple's one full flush, covering every barrier before it.
fn finish_batch(root: &Path, durability: LocalDurability) -> std::io::Result<()> {
    if cfg!(target_vendor = "apple") && durability == LocalDurability::FullFlush {
        sync_directory(root, durability)?;
    }
    Ok(())
}

/// Flushes every file and directory below `directory`, for a root whose
/// last owner may have closed without flushing.
fn flush_tree(directory: &Path, durability: LocalDurability) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            flush_tree(&entry.path(), durability)?;
        } else {
            sync_member(&entry.path(), durability)?;
        }
    }
    sync_member_directory(directory, durability)
}

/// Makes everything below `root` durable: one filesystem flush where the
/// platform has one, else each file and directory.
fn recover(root: &Path, durability: LocalDurability) -> std::io::Result<()> {
    match acyclic_native_runtime::sync_filesystem(root) {
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
            flush_tree(&root.join("keys"), durability)?;
            finish_batch(root, durability)
        }
        synced => synced,
    }
}

/// The shared native namespace flush. Every platform flushes the directory
/// itself (Windows through a backup-semantics handle), so a successful
/// FullFlush never rests on a skipped directory.
fn sync_directory(path: &Path, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_parent(path, native(durability))
}

/// Paths written since they were last flushed: files, removals and created
/// directories. Each is tracked before it becomes visible, so a flush of
/// anything below it finds it, and again after, so a flush whose snapshot
/// preceded the change forgets none of it; each tracking carries a fresh
/// sequence, and a flush forgets only the sequences it snapshot.
///
/// It also holds the root's ownership. The medium and every accepted write
/// hold it, so the root closes, everything flushed, the marker removed and
/// the lock released, only once the last of them lets go.
struct Unflushed {
    paths: Mutex<(u64, HashMap<PathBuf, u64>)>,
    root: PathBuf,
    durability: LocalDurability,
    /// Whether the root opened unclean and its repair has not settled.
    recovered: AtomicBool,
    _lock: std::fs::File,
    _anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
}

impl Drop for Unflushed {
    /// A clean close of a settled root leaves nothing unflushed, so the next
    /// open trusts it; an unsettled one stays marked unclean.
    fn drop(&mut self) {
        let unflushed = self.drain();
        let settled = !*self.recovered.get_mut();
        let flushed = flush_batch(&self.root, &unflushed, self.durability).and_then(|()| {
            if !settled {
                return Ok(());
            }
            std::fs::remove_file(self.root.join(OPEN))?;
            sync_directory(&self.root, self.durability)
        });
        // A failed close leaves the marker, and the next open recovers.
        drop(flushed);
    }
}

impl Unflushed {
    /// Applies `change` to `path`, tracked on both sides of it.
    fn changed<T>(
        &self,
        path: &Path,
        change: impl FnOnce() -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        self.track(path);
        let changed = change();
        self.track(path);
        changed
    }

    fn track(&self, path: &Path) {
        let mut paths = self.paths.lock().unwrap_or_else(PoisonError::into_inner);
        paths.0 += 1;
        let sequence = paths.0;
        paths.1.insert(path.to_path_buf(), sequence);
    }

    /// The directories a flush of `paths` syncs: each path's own, and the
    /// parent of each unflushed directory above it, with the writes covered.
    fn plan(&self, keys: &Path, paths: &[PathBuf]) -> (BTreeSet<PathBuf>, Vec<(PathBuf, u64)>) {
        let tracked = self.paths.lock().unwrap_or_else(PoisonError::into_inner);
        let mut directories = BTreeSet::new();
        let mut covered = Vec::new();
        for path in paths {
            if let Some(&sequence) = tracked.1.get(path) {
                covered.push((path.clone(), sequence));
            }
            let mut below = path.as_path();
            while let Some(directory) = below.parent() {
                if !directories.insert(directory.to_path_buf()) || directory == keys {
                    break;
                }
                match tracked.1.get(directory) {
                    Some(&sequence) => covered.push((directory.to_path_buf(), sequence)),
                    None => break,
                }
                below = directory;
            }
        }
        (directories, covered)
    }

    fn forget(&self, covered: Vec<(PathBuf, u64)>) {
        let mut tracked = self.paths.lock().unwrap_or_else(PoisonError::into_inner);
        for (path, sequence) in covered {
            if tracked.1.get(&path) == Some(&sequence) {
                tracked.1.remove(&path);
            }
        }
    }

    fn drain(&mut self) -> Vec<PathBuf> {
        let tracked = self.paths.get_mut().unwrap_or_else(PoisonError::into_inner);
        tracked.1.drain().map(|(path, _)| path).collect()
    }
}

/// Creates `directory` and its missing ancestors, tracking each new one so
/// the flush of a name below it also makes its entry durable.
fn create_directories(directory: &Path, unflushed: &Unflushed) -> std::io::Result<()> {
    if directory.is_dir() {
        return Ok(());
    }
    let parent = directory.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
    create_directories(parent, unflushed)?;
    match unflushed.changed(directory, || std::fs::create_dir(directory)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
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
        let (root, lock, recovered) = acyclic_native_runtime::spawn_persistence(move || {
            // Every key lives below `keys`, created here and made durable
            // with the root below, so no flush need reach above it.
            std::fs::create_dir_all(root.join("tmp"))?;
            std::fs::create_dir_all(root.join("keys"))?;
            let format = root.join("FORMAT");
            match std::fs::read(&format) {
                Ok(bytes) if bytes == FORMAT => {}
                Ok(_) => return Err(LocalOpenError::Corrupt),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let mut file = std::fs::File::create(&format)?;
                    file.write_all(FORMAT)?;
                    sync(&file, durability)?;
                    sync_directory(&root, durability)?;
                }
                Err(error) => return Err(error.into()),
            }
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(root.join("LOCK"))?;
            match lock.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => return Err(LocalOpenError::AlreadyOwned),
                Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
            }
            // Crash-left temporaries never became visible.
            for entry in std::fs::read_dir(root.join("tmp"))? {
                std::fs::remove_file(entry?.path())?;
            }
            let marker = root.join(OPEN);
            let recovered = marker.exists();
            if recovered {
                // The last owner may have left writes unflushed.
                recover(&root, durability)?;
            } else {
                sync(&std::fs::File::create(&marker)?, durability)?;
            }
            sync_directory(&root, durability)?;
            Ok((root, lock, recovered))
        })
        .await
        .map_err(|_| LocalOpenError::Unavailable)??;
        Ok(Self {
            durability,
            conditional: Default::default(),
            unflushed: Arc::new(Unflushed {
                paths: Mutex::default(),
                root: root.clone(),
                durability,
                recovered: AtomicBool::new(recovered),
                _lock: lock,
                _anchor: anchor,
            }),
            root,
        })
    }

    fn path(&self, key: &str) -> PathBuf {
        let mut path = self.root.join("keys");
        path.extend(key.split('/'));
        path
    }
}

/// Flushes every unflushed path and its directory as one batch.
fn flush_batch(root: &Path, paths: &[PathBuf], durability: LocalDurability) -> std::io::Result<()> {
    let directories: BTreeSet<&Path> = paths.iter().filter_map(|path| path.parent()).collect();
    for path in paths.iter().filter(|path| !path.is_dir()) {
        sync_member(path, durability)?;
    }
    for directory in directories {
        sync_member_directory(directory, durability)?;
    }
    finish_batch(root, durability)
}

fn failed(error: std::io::Error) -> MediumError {
    MediumError::Failed(error)
}

fn walk(
    directory: &Path,
    key: &str,
    after: Option<&str>,
    limit: usize,
    out: &mut Vec<(String, u64)>,
) -> std::io::Result<()> {
    let mut entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    entries.sort();
    for name in entries {
        if out.len() >= limit {
            return Ok(());
        }
        let Some(name) = name.to_str() else {
            return Err(std::io::ErrorKind::InvalidData.into());
        };
        let child = format!("{key}{name}");
        let path = directory.join(name);
        let metadata = std::fs::metadata(&path)?;
        if metadata.is_dir() {
            let prefix = format!("{child}/");
            // Every key below sorts before `after` unless `after` lies inside.
            if after.is_some_and(|after| prefix.as_str() < after && !after.starts_with(&prefix)) {
                continue;
            }
            walk(&path, &prefix, after, limit, out)?;
        } else if after.is_none_or(|after| child.as_str() > after) {
            out.push((child, metadata.len()));
        }
    }
    Ok(())
}

/// Whether `path` currently holds exactly `expected`.
fn holds(path: &Path, expected: &[u8]) -> std::io::Result<bool> {
    match std::fs::read(path) {
        Ok(current) => Ok(current == expected),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

#[async_trait::async_trait]
impl Medium for LocalMedium {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let target = self.path(key);
        let mut name = [0u8; 16];
        getrandom::fill(&mut name).map_err(|error| failed(std::io::Error::other(error)))?;
        let temporary = self.root.join("tmp").join(hex::encode(name));
        let unflushed = self.unflushed.clone();
        blocking(move || {
            if target.exists() {
                return Ok(false);
            }
            let parent = target.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
            create_directories(parent, &unflushed)?;
            let mut file = std::fs::File::create_new(&temporary)?;
            file.write_all(&bytes)?;
            drop(file);
            let published = unflushed.changed(&target, || {
                acyclic_native_runtime::publish_new(&temporary, &target, None)
            });
            let _ = std::fs::remove_file(&temporary);
            match published {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
                Err(error) => Err(error),
            }
        })
        .await
        .map_err(failed)
    }

    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        let path = self.path(key);
        blocking(move || {
            let mut file = match std::fs::File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            };
            let length = file.metadata()?.len();
            let range = range.unwrap_or(0..length);
            if range.start > range.end || range.end > length {
                return Err(std::io::ErrorKind::InvalidInput.into());
            }
            let count = usize::try_from(range.end - range.start)
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
            let mut bytes = vec![0; count];
            file.seek(std::io::SeekFrom::Start(range.start))?;
            file.read_exact(&mut bytes)?;
            Ok(Some(OwnedBytes::from(bytes)))
        })
        .await
        .map_err(failed)
    }

    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        let path = self.path(key);
        let unflushed = self.unflushed.clone();
        blocking(
            move || match unflushed.changed(&path, || std::fs::remove_file(&path)) {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(error),
            },
        )
        .await
        .map_err(failed)
    }

    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        let (target, expected) = (self.path(key), expected.to_vec());
        let mut name = [0u8; 16];
        getrandom::fill(&mut name).map_err(|error| failed(std::io::Error::other(error)))?;
        let temporary = self.root.join("tmp").join(hex::encode(name));
        let (conditional, unflushed) = (self.conditional.clone(), self.unflushed.clone());
        let durability = self.durability;
        blocking(move || {
            let _held = conditional.lock().unwrap_or_else(PoisonError::into_inner);
            if !holds(&target, &expected)? {
                return Ok(false);
            }
            let mut file = std::fs::File::create_new(&temporary)?;
            file.write_all(&bytes)?;
            // The bytes land before the rename can: a crash keeps the old
            // value or the new one, never a torn rewrite of a committed key.
            sync_contents(&file, durability)?;
            drop(file);
            if let Err(error) = unflushed.changed(&target, || std::fs::rename(&temporary, &target))
            {
                let _ = std::fs::remove_file(&temporary);
                return Err(error);
            }
            Ok(true)
        })
        .await
        .map_err(failed)
    }

    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        let (path, expected) = (self.path(key), expected.to_vec());
        let (conditional, unflushed) = (self.conditional.clone(), self.unflushed.clone());
        blocking(move || {
            let _held = conditional.lock().unwrap_or_else(PoisonError::into_inner);
            if !holds(&path, &expected)? {
                return Ok(false);
            }
            unflushed.changed(&path, || std::fs::remove_file(&path))?;
            Ok(true)
        })
        .await
        .map_err(failed)
    }

    async fn flush(&self, keys: &[String]) -> Result<(), MediumError> {
        let paths: Vec<PathBuf> = keys.iter().map(|key| self.path(key)).collect();
        let durability = self.durability;
        // What this flush covers is snapshot first: a change finishing during
        // it is tracked again, and stays unflushed. Members are submitted
        // together, so the device can retire them as one batch; directories
        // follow their entries, and Apple's one full flush follows every
        // barrier.
        let (directories, covered) = self.unflushed.plan(&self.root.join("keys"), &paths);
        let files: Vec<_> = paths
            .iter()
            .cloned()
            .map(|path| blocking(move || sync_member(&path, durability)))
            .collect();
        for file in files {
            file.await.map_err(failed)?;
        }
        let directories: Vec<_> = directories
            .into_iter()
            .map(|directory| blocking(move || sync_member_directory(&directory, durability)))
            .collect();
        for directory in directories {
            directory.await.map_err(failed)?;
        }
        let root = self.root.clone();
        blocking(move || finish_batch(&root, durability))
            .await
            .map_err(failed)?;
        self.unflushed.forget(covered);
        Ok(())
    }

    fn volatile(&self) -> bool {
        true
    }

    fn recovered(&self) -> bool {
        self.unflushed.recovered.load(Ordering::Acquire)
    }

    fn settle(&self) {
        self.unflushed.recovered.store(false, Ordering::Release);
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
        let directory = self.path(prefix.trim_end_matches('/'));
        let prefix = prefix.to_owned();
        let after = after.map(str::to_owned);
        blocking(move || {
            let mut out = Vec::new();
            walk(&directory, &prefix, after.as_deref(), limit, &mut out)?;
            Ok(out)
        })
        .await
        .map_err(failed)
    }
}

impl std::fmt::Debug for LocalMedium {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalMedium")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::*;
    use futures::FutureExt as _;

    #[tokio::test]
    async fn a_change_finishing_during_a_flush_stays_unflushed() {
        let root = tempfile::tempdir().unwrap();
        let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None)
            .await
            .unwrap();
        let (keys, path) = (medium.root.join("keys"), medium.path("k/key"));
        let tracked = |unflushed: &Unflushed| {
            let paths = unflushed.paths.lock().unwrap();
            paths.1.contains_key(&path)
        };
        // A flush snapshots while the change is in flight.
        let mut covered = None;
        medium
            .unflushed
            .changed(&path, || {
                covered = Some(medium.unflushed.plan(&keys, std::slice::from_ref(&path)).1);
                Ok(())
            })
            .unwrap();
        medium.unflushed.forget(covered.unwrap());
        assert!(tracked(&medium.unflushed));
        // Control: a change tracked only before it lands is forgotten.
        medium
            .unflushed
            .forget(medium.unflushed.plan(&keys, std::slice::from_ref(&path)).1);
        medium.unflushed.track(&path);
        let (_, covered) = medium.unflushed.plan(&keys, std::slice::from_ref(&path));
        medium.unflushed.forget(covered);
        assert!(!tracked(&medium.unflushed));
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
