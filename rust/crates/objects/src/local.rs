//! Durable local-directory medium: one immutable file per key.
//!
//! A key becomes visible only by hard-linking a complete, synchronized
//! temporary file into place, so create-if-absent is atomic on every
//! supported filesystem and a crash never exposes partial bytes.
use crate::{Medium, MediumError, Store};
use acyclic_stream::OwnedBytes;
use std::io::{Read as _, Seek as _, Write as _};
use std::ops::Range;
use std::path::{Path, PathBuf};

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

/// The durable local-directory medium.
pub struct LocalMedium {
    root: PathBuf,
    durability: LocalDurability,
    _lock: std::fs::File,
    _anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
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

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(std::io::Error::other)?
}

fn sync(file: &std::fs::File, durability: LocalDurability) -> std::io::Result<()> {
    match durability {
        LocalDurability::FullFlush => file.sync_all(),
        #[cfg(target_vendor = "apple")]
        LocalDurability::Barrier => {
            use std::os::fd::AsRawFd as _;
            // SAFETY: the descriptor is open for the call's duration.
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) } == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        }
        #[cfg(not(target_vendor = "apple"))]
        LocalDurability::Barrier => Err(std::io::ErrorKind::Unsupported.into()),
    }
}

fn sync_directory(path: &Path, durability: LocalDurability) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        sync(&std::fs::File::open(path)?, durability)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, durability);
        Ok(())
    }
}

/// Creates `directory` and its missing ancestors, syncing the parent of each
/// directory this call created, or raced another creator to, so that no new
/// name below survives a crash its directory entry does not.
fn create_directories(directory: &Path, durability: LocalDurability) -> std::io::Result<()> {
    if directory.is_dir() {
        return Ok(());
    }
    let parent = directory.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
    create_directories(parent, durability)?;
    match std::fs::create_dir(directory) {
        Ok(()) => sync_directory(parent, durability),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            sync_directory(parent, durability)
        }
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
        let (root, lock) = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(root.join("tmp"))?;
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
            Ok((root, lock))
        })
        .await
        .map_err(|_| LocalOpenError::Unavailable)??;
        Ok(Self {
            root,
            durability,
            _lock: lock,
            _anchor: anchor,
        })
    }

    fn path(&self, key: &str) -> PathBuf {
        let mut path = self.root.join("keys");
        path.extend(key.split('/'));
        path
    }
}

enum Created {
    Exists,
    Linked(std::io::Result<()>),
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

#[async_trait::async_trait]
impl Medium for LocalMedium {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let target = self.path(key);
        let mut name = [0u8; 16];
        getrandom::fill(&mut name).map_err(|error| failed(std::io::Error::other(error)))?;
        let temporary = self.root.join("tmp").join(hex::encode(name));
        let durability = self.durability;
        let created = blocking(move || {
            if target.exists() {
                return Ok(Created::Exists);
            }
            let parent = target.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
            create_directories(parent, durability)?;
            let mut file = std::fs::File::create_new(&temporary)?;
            file.write_all(&bytes)?;
            sync(&file, durability)?;
            drop(file);
            let linked = std::fs::hard_link(&temporary, &target);
            let _ = std::fs::remove_file(&temporary);
            match linked {
                Ok(()) => Ok(Created::Linked(sync_directory(parent, durability))),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    Ok(Created::Exists)
                }
                Err(error) => Err(error),
            }
        })
        .await
        .map_err(failed)?;
        match created {
            Created::Exists => Ok(false),
            Created::Linked(Ok(())) => Ok(true),
            // Visible, but the name may not survive a crash.
            Created::Linked(Err(error)) => Err(MediumError::Ambiguous(error)),
        }
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
        let durability = self.durability;
        blocking(move || match std::fs::remove_file(&path) {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    sync_directory(parent, durability)?;
                }
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        })
        .await
        .map_err(failed)
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        if !prefix.ends_with('/') {
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
