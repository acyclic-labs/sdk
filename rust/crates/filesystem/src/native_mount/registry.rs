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
    let directory = directory()?;
    // Recovery below initializes again; only the first caller reaps.
    if !REAPED.swap(true, std::sync::atomic::Ordering::AcqRel) {
        reap(directory);
    }
    Ok(())
}

static REAPED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

const FENCE_RECORD_V1: &[u8] = b"acyclic-fs/native-mount-fence/v1\0";

/// The persisted destination of one mount fence, written by its owner.
pub(super) fn fence_record(
    #[cfg(unix)] canonical: &Path,
    #[cfg(windows)] canonical: &[u16],
) -> Vec<u8> {
    let mut record = FENCE_RECORD_V1.to_vec();
    #[cfg(unix)]
    record.extend_from_slice(std::os::unix::ffi::OsStrExt::as_bytes(
        canonical.as_os_str(),
    ));
    #[cfg(windows)]
    record.extend(canonical.iter().flat_map(|unit| unit.to_le_bytes()));
    record
}

pub(super) fn write_fence_record(file: &std::fs::File, record: &[u8]) -> std::io::Result<()> {
    use std::io::{Seek as _, Write as _};
    let mut file = file;
    file.set_len(0)?;
    file.seek(std::io::SeekFrom::Start(0))?;
    file.write_all(record)
}

fn fence_destination(record: &[u8]) -> Option<PathBuf> {
    let path = record.strip_prefix(FENCE_RECORD_V1)?;
    #[cfg(unix)]
    return Some(<std::ffi::OsStr as std::os::unix::ffi::OsStrExt>::from_bytes(path).into());
    #[cfg(windows)]
    {
        let (units, []) = path.as_chunks::<2>() else {
            return None;
        };
        let units: Vec<u16> = units.iter().map(|unit| u16::from_le_bytes(*unit)).collect();
        Some(<std::ffi::OsString as std::os::windows::ffi::OsStringExt>::from_wide(&units).into())
    }
}

/// A mount whose owner and exit action both die is detached at the next
/// registry open on the host. Process-group kills and terminal hangups do not
/// reach the exit action, which leads its own group; a session or cgroup kill
/// does, and this reap is then the guarantee. Liveness is the
/// fence's kernel lock, held by the owner and by its exit action, and released
/// only once both have exited: recovery of a live fence is refused as busy and
/// the fence is left alone. Identity is that lock, never a name or path.
fn reap(directory: &Path) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.as_encoded_bytes();
        if !name.starts_with(NATIVE_FENCE_PREFIX.as_bytes())
            || !name.ends_with(NATIVE_FENCE_SUFFIX.as_bytes())
        {
            continue;
        }
        let result = (|| {
            let Some((destination, guard)) = locked_destination(&entry.path())? else {
                return Ok(());
            };
            let original = crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .map_err(|(error, _)| NativeMountError::from(error))?;
            let mut owner =
                NativeMountRecoveryOwner::from_locked_existing(&destination, guard, &original)?;
            #[cfg(target_os = "macos")]
            acyclic_native_runtime::initialize_native_io_services()?;
            owner.detach(&original).map(|_| ())
        })();
        match result {
            Ok(()) | Err(NativeMountError::DestinationBusy) => {}
            Err(error) => {
                tracing::warn!(%error, fence = %entry.path().display(), "dead native mount was not reaped")
            }
        }
    }
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
        static SHARED: RegistrySlot = RegistrySlot::new();
        with_directory(|directory| SHARED.open(directory, work))
    }
}

/// One registry descriptor per owner, lazily charged to its first user.
#[cfg(not(loom))]
pub(super) struct RegistrySlot(std::sync::Mutex<Option<std::sync::Arc<Registry>>>);

#[cfg(not(loom))]
impl RegistrySlot {
    pub(super) const fn new() -> Self {
        Self(std::sync::Mutex::new(None))
    }

    pub(super) fn open<W: crate::native_host::NativeHostWork>(
        &self,
        directory: &Path,
        work: &mut W,
    ) -> Result<std::sync::Arc<Registry>, NativeMountError>
    where
        NativeMountError: From<W::Error>,
    {
        let mut shared = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(registry) = shared.as_ref() {
            return Ok(std::sync::Arc::clone(registry));
        }
        let (directory, file) = open_in_directory(directory, work)?;
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
// A persistent root must be absolute before any directory is created. The
// root is configuration: without an explicit one, a test build mounts below
// its own executable's temporary root and never in the user's profile, so no
// test can leave a mount in state that builds or the user's session share.
fn persistent_base() -> Option<PathBuf> {
    #[cfg(windows)]
    if let Some(directory) =
        std::env::var_os("LOCALAPPDATA").filter(|directory| Path::new(directory).is_absolute())
    {
        return Some(directory.into());
    }
    #[cfg(not(windows))]
    if let Some(directory) =
        std::env::var_os("XDG_STATE_HOME").filter(|directory| Path::new(directory).is_absolute())
    {
        return Some(directory.into());
    }
    #[cfg(any(test, feature = "test-support"))]
    return process_base();
    #[cfg(not(any(test, feature = "test-support")))]
    profile_base()
}

/// One private state root per test executable. Every process the test binary
/// spawns from itself derives the same root, so a parent and its children see
/// one registry and one set of fences, exactly as cooperating product
/// processes share a configured root. It is never the user's profile.
#[cfg(any(test, feature = "test-support"))]
fn process_base() -> Option<PathBuf> {
    static BASE: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();
    BASE.get_or_init(|| {
        let executable = std::env::current_exe().ok()?;
        let mut identity = blake3::Hasher::new();
        identity.update(b"acyclic-fs/test-state-root/v1\0");
        identity.update(executable.as_os_str().as_encoded_bytes());
        // Linux shares one temporary directory between users; macOS and
        // Windows temporary directories are already per user.
        #[cfg(target_os = "linux")]
        let owner = nix::unistd::geteuid().as_raw();
        #[cfg(target_os = "linux")]
        identity.update(&owner.to_le_bytes());
        let identity = identity.finalize().to_hex();
        let base =
            std::env::temp_dir().join(format!("acyclic-native-mount-{}", identity.get(..32)?));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        match builder.create(&base) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return None,
        }
        // A shared temporary directory must not hand this user's root to a
        // name another user planted first.
        let metadata = std::fs::symlink_metadata(&base).ok()?;
        #[cfg(target_os = "linux")]
        if std::os::unix::fs::MetadataExt::uid(&metadata) != owner {
            return None;
        }
        (metadata.is_dir() && !metadata.file_type().is_symlink()).then_some(base)
    })
    .clone()
}

#[cfg(not(any(test, feature = "test-support")))]
fn profile_base() -> Option<PathBuf> {
    #[cfg(not(windows))]
    {
        if let Some(directory) =
            std::env::var_os("HOME").filter(|directory| Path::new(directory).is_absolute())
        {
            return Some(PathBuf::from(directory).join(".local").join("state"));
        }
    }
    dirs::data_local_dir().filter(|directory| directory.is_absolute())
}

#[cfg(test)]
mod tests;

// The lock precedes the read and is transferred, never reacquired by name.
// An interrupted write cannot authorize another destination's fence.
fn locked_destination(
    path: &Path,
) -> Result<Option<(PathBuf, MountDestinationGuard)>, NativeMountError> {
    use std::io::Read as _;
    let file = super::fence::acquire_existing_mount_lock(path)?;
    let mut record = Vec::new();
    (&*file).read_to_end(&mut record)?;
    let Some(destination) = fence_destination(&record) else {
        return Ok(None);
    };
    if !destination.is_absolute() || destination.parent().is_none() {
        return Ok(None);
    }
    let (expected, _) = MountDestinationGuard::paths_for_canonical(&destination)?;
    if expected != path {
        return Ok(None);
    }
    Ok(Some((
        destination,
        MountDestinationGuard::from_locked_existing(file, expected),
    )))
}

#[cfg(test)]
mod recovery_record_tests {
    use super::*;

    #[test]
    fn recovery_requires_original_locked_fence_and_complete_destination()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let destination = root.path().join("complete-destination");
        std::fs::create_dir(&destination)?;
        let canonical = destination.canonicalize()?;
        let mut held = MountDestinationGuard::acquire_canonical(&canonical)?;
        let path = held.lock_path.clone();
        #[cfg(unix)]
        let record = fence_record(&canonical);
        #[cfg(windows)]
        let record = {
            use std::os::windows::ffi::OsStrExt as _;
            fence_record(&canonical.as_os_str().encode_wide().collect::<Vec<_>>())
        };
        write_fence_record(held.file.as_ref().unwrap(), &record)?;
        assert!(matches!(
            locked_destination(&path),
            Err(NativeMountError::DestinationBusy)
        ));
        drop(held.file.take());
        // A syntactically valid prefix still cannot name this original fence.
        std::fs::write(
            &path,
            &record[..record.len() - if cfg!(windows) { 2 } else { 1 }],
        )?;
        assert!(locked_destination(&path)?.is_none());
        std::fs::write(&path, &record)?;
        let (selected, guard) = locked_destination(&path)?.expect("complete record is recoverable");
        assert_eq!(selected, canonical);
        assert!(matches!(
            locked_destination(&path),
            Err(NativeMountError::DestinationBusy)
        ));
        drop(guard);
        drop(held);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }
}
