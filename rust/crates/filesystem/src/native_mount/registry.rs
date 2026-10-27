use super::*;

// Process configuration is initialized before bounded invocations, just like
// the callback runtime. SDK retirement never unlinks this directory or file.
static DIRECTORY: std::sync::OnceLock<Result<PathBuf, Arc<std::io::Error>>> =
    std::sync::OnceLock::new();

pub(super) fn initialize() -> Result<(), NativeMountError> {
    DIRECTORY.get_or_init(|| {
        let directory = persistent_base()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "native mount registry requires the user's persistent data directory",
                )
            })
            .and_then(|base| {
                let directory = base.join("acyclic").join("native-mount");
                std::fs::create_dir_all(&directory)?;
                let metadata = std::fs::symlink_metadata(&directory)?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "native mount registry is not an ordinary directory",
                    ));
                }
                directory.canonicalize()
            });
        directory.map_err(Arc::new)
    });
    directory().map(|_| ())
}

fn directory() -> Result<&'static Path, NativeMountError> {
    match DIRECTORY
        .get()
        .ok_or(NativeMountError::ServicesNotInitialized)?
    {
        Ok(directory) => Ok(directory),
        Err(error) => Err(NativeMountError::ServiceUnavailable(Arc::clone(error))),
    }
}

// Ordinary control-path queries run before any bounded native invocation.
pub(super) fn path() -> Result<PathBuf, NativeMountError> {
    initialize()?;
    with_directory(|directory| Ok(directory.join(NATIVE_REGISTRY_NAME)))
}

pub(super) fn with_directory<T>(
    operation: impl FnOnce(&Path) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    #[cfg(all(test, loom, target_os = "linux"))]
    if let Some(kernel) = super::disconnected::kernel::current()
        && let Some(directory) = kernel.registry_directory()
    {
        return operation(directory);
    }
    operation(directory()?)
}

#[cfg(test)]
pub(super) fn open<W: crate::native_host::NativeHostWork>(
    work: &mut W,
) -> Result<fence_io::MountLockFile, NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    open_with_directory(work).map(|(_, file)| file)
}

pub(super) fn open_with_directory<W: crate::native_host::NativeHostWork>(
    work: &mut W,
) -> Result<(cap_std::fs::Dir, fence_io::MountLockFile), NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    with_directory(|directory| open_in_directory(directory, work))
}

/// The registry directory and lock, opened once by the first mount to need
/// them and shared by every later one, so mounts retain no registry
/// descriptors of their own. The file's lock belongs to the open
/// descriptor, which every thread here shares; `admission` therefore admits
/// one thread at a time, and only that thread contends with other processes
/// for the file's lock.
pub(super) struct Registry {
    pub(super) file: fence_io::MountLockFile,
    pub(super) directory: cap_std::fs::Dir,
    admission: crate::sync::native::Mutex<()>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Registry")
            .field("file", &self.file)
            .finish_non_exhaustive()
    }
}

/// The registry's lock, held by one thread of this process. Dropping it
/// unlocks the file uncharged, as closing a mount's own descriptor once did
/// on every path that left before its charged unlock.
pub(super) struct RegistryHold<'a> {
    file: &'a fence_io::MountLockFile,
    locked: bool,
    _admitted: Option<crate::sync::native::MutexGuard<'a, ()>>,
}

impl Registry {
    pub(super) fn new(directory: cap_std::fs::Dir, file: fence_io::MountLockFile) -> Self {
        Self {
            file,
            directory,
            admission: crate::sync::native::Mutex::new(()),
        }
    }

    /// Admits this thread, then takes the file's lock with one blocking,
    /// charged syscall.
    pub(super) fn lock<W: crate::native_host::NativeHostWork>(
        &self,
        work: &mut W,
    ) -> Result<RegistryHold<'_>, NativeMountError>
    where
        NativeMountError: From<W::Error>,
    {
        #[cfg(all(test, loom, target_os = "linux"))]
        let admitted = (!super::disconnected::kernel::current()
            .is_some_and(|kernel| kernel.registry_admission_disabled()))
        .then(|| {
            self.admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        #[cfg(not(all(test, loom, target_os = "linux")))]
        let admitted = Some(
            self.admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        // Armed before the syscall: a charge refused after it locked must
        // still unlock.
        let held = RegistryHold {
            file: &self.file,
            locked: true,
            _admitted: admitted,
        };
        super::fence::lock_mount_file_with_work(&self.file, true, work)?;
        Ok(held)
    }
}

impl RegistryHold<'_> {
    /// Unlocks the file as one charged syscall.
    pub(super) fn unlock<W: crate::native_host::NativeHostWork>(
        mut self,
        work: &mut W,
    ) -> Result<(), NativeMountError>
    where
        NativeMountError: From<W::Error>,
    {
        let file = self.file;
        work.attempt_io(
            crate::materializer::NativeResourceCredit::WriteOperations.delta(1),
            || fence_io::unlock(file),
        )?;
        self.locked = false;
        Ok(())
    }
}

impl Drop for RegistryHold<'_> {
    fn drop(&mut self) {
        if self.locked {
            let _ = fence_io::unlock(self.file);
        }
    }
}

/// This process's registry, opened by the first caller and charged to it.
/// A failed open is not kept: the next caller opens it again. Models give
/// each caller its own descriptor, as other processes have, and share one
/// registry between their threads explicitly.
pub(super) fn shared<W: crate::native_host::NativeHostWork>(
    work: &mut W,
) -> Result<crate::sync::native::Arc<Registry>, NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    #[cfg(loom)]
    {
        let (directory, file) = open_with_directory(work)?;
        return Ok(crate::sync::native::Arc::new(Registry::new(
            directory, file,
        )));
    }
    #[cfg(not(loom))]
    {
        static SHARED: std::sync::Mutex<Option<std::sync::Arc<Registry>>> =
            std::sync::Mutex::new(None);
        let mut shared = SHARED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(registry) = shared.as_ref() {
            return Ok(std::sync::Arc::clone(registry));
        }
        let (directory, file) = open_with_directory(work)?;
        let registry = std::sync::Arc::new(Registry::new(directory, file));
        *shared = Some(std::sync::Arc::clone(&registry));
        Ok(registry)
    }
}

fn open_in_directory<W: crate::native_host::NativeHostWork>(
    directory: &Path,
    work: &mut W,
) -> Result<(cap_std::fs::Dir, fence_io::MountLockFile), NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    let (directory, _) = crate::native_host::open_root_directory(directory, work)?;
    let file = crate::native_host::open_leaf(
        &directory,
        Path::new(NATIVE_REGISTRY_NAME),
        &crate::native_host::NativeOpenOptions::ReadWriteCreate,
        work,
    )?;
    Ok((directory, file.into()))
}

// Match the service's persistent state-root selection before consulting OS
// defaults. Known Folder lookup ignores an explicitly supplied LOCALAPPDATA.
fn persistent_base() -> Option<PathBuf> {
    #[cfg(windows)]
    if let Some(directory) =
        std::env::var_os("LOCALAPPDATA").filter(|directory| !directory.is_empty())
    {
        return Some(directory.into());
    }
    #[cfg(not(windows))]
    {
        if let Some(directory) =
            std::env::var_os("XDG_STATE_HOME").filter(|directory| !directory.is_empty())
        {
            return Some(directory.into());
        }
        if let Some(directory) = std::env::var_os("HOME").filter(|directory| !directory.is_empty())
        {
            return Some(PathBuf::from(directory).join(".local").join("state"));
        }
    }
    dirs::data_local_dir()
}

#[cfg(test)]
mod tests;
