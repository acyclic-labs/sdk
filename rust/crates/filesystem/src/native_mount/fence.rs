use super::fence_io::MountLockFile;
use super::*;
mod destination;
#[cfg(windows)]
mod ownership;

pub(super) const NATIVE_FENCE_PREFIX: &str = ".acyclic-fs-mount-";
pub(super) const NATIVE_FENCE_SUFFIX: &str = ".lock";
pub(super) const NATIVE_FENCE_NAME_BYTES: usize =
    NATIVE_FENCE_PREFIX.len() + 64 + NATIVE_FENCE_SUFFIX.len();
pub(super) const NATIVE_REGISTRY_NAME: &str = ".acyclic-fs-mount-registry.lock";

#[derive(Debug)]
pub(super) struct MountDestinationGuard {
    pub(super) file: Option<MountLockFile>,
    pub(super) lock_path: PathBuf,
    // Retain the physical parent and fixed leaf so replacing an alias cannot redirect cleanup.
    pub(super) parent_directory: Option<cap_std::fs::Dir>,
    pub(super) fence_name: Option<[u8; NATIVE_FENCE_NAME_BYTES]>,
    // This process shares one registry descriptor.
    pub(super) registry_file: Option<crate::sync::native::Arc<registry::Registry>>,
    pub(super) cleanup_name:
        Option<crate::native_host::PreparedAsciiLeaf<{ NATIVE_FENCE_NAME_BYTES + 1 }>>,
    pub(super) birth_cleanup: Option<crate::NativeWorkReservation>,
    // Allocated and funded before shutdown; retained on a failed exact cleanup.
    destination_cleanup: Option<(
        Vec<crate::native_host::NativeUnit>,
        crate::NativeAllocation,
        crate::NativeWorkReservation,
    )>,
}

/// The accepted fence path can spend only its supplied, original-ledger credit.
pub(super) struct NativeFenceReleaseWork<'a> {
    pub(super) credit: &'a crate::NativeWorkReservation,
    pub(super) wait: Option<&'a crate::CancellationToken>,
    pub(super) pending: Option<crate::NativeWorkReservation>,
}

impl crate::native_host::NativeHostWork for NativeFenceReleaseWork<'_> {
    type Error = crate::NativeTreeMaterializationError;
    type Allocation = std::convert::Infallible;
    type CleanupWrite = std::convert::Infallible;

    fn io_error_kind(error: &Self::Error) -> Option<std::io::ErrorKind> {
        match error {
            Self::Error::Io(error) => Some(error.kind()),
            _ => None,
        }
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        Self::Error::AllocationFailed(error)
    }
    fn invalid_request() -> Self::Error {
        Self::Error::InvalidLayout
    }
    fn check(&self) -> Result<(), Self::Error> {
        self.credit.check_wait(self.wait)
    }
    fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        if self.pending.is_some() {
            return Err(Self::Error::InvalidLayout);
        }
        self.pending = Some(self.credit.partition_work(delta, self.wait)?);
        Ok(())
    }
    fn record(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.pending
            .take()
            .ok_or(Self::Error::InvalidLayout)?
            .record_completed(delta)
    }
    fn started_io(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        let io = crate::WorkCounters {
            backend_read_operations: delta.backend_read_operations,
            backend_write_operations: delta.backend_write_operations,
            durability_operations: delta.durability_operations,
            ..crate::WorkCounters::UNCHARGED
        };
        let validation = io.remaining(delta)?;
        let credit = self.credit.partition_work(delta, self.wait)?;
        credit.record_completed(validation)?;
        credit.started_io(io, self.wait)
    }
    fn claim(&mut self, _bytes: u64) -> Result<Self::Allocation, Self::Error> {
        Err(Self::Error::InvalidLayout)
    }
    #[cfg(windows)]
    fn reserve_opaque_decode(&mut self, _bytes: u64) -> Result<Self::Allocation, Self::Error> {
        Err(Self::Error::InvalidLayout)
    }
    fn reconcile_allocation(
        &mut self,
        allocation: &mut Self::Allocation,
        _actual: u64,
    ) -> Result<(), Self::Error> {
        match *allocation {}
    }
    fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error> {
        match allocation {}
    }
    #[cfg(any(windows, target_os = "linux", target_os = "macos", test))]
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        Err(Self::Error::InvalidLayout)
    }
    #[cfg(any(target_os = "linux", all(test, unix)))]
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        match reservation {}
    }
    #[cfg(any(windows, target_os = "linux", target_os = "macos", test))]
    fn attempt_cleanup_write<T>(
        &mut self,
        reservation: Self::CleanupWrite,
        _operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) {
        match reservation {}
    }
}

impl MountDestinationGuard {
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    pub(super) fn acquire_with_work(
        destination: &Path,
        allow_nonempty: bool,
        native: &NativeMaterializationWork,
        guard: &mut Option<Self>,
        recovery: bool,
    ) -> Result<(), NativeMountError> {
        if guard.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        let (directory, parent) =
            Self::destination_directory(destination, allow_nonempty, recovery, native)?;
        Self::acquire_held(destination, directory, parent, native, guard, recovery)
    }

    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    pub(super) fn acquire_recovery_in_directory(
        destination: &Path,
        directory: cap_std::fs::Dir,
        native: &NativeMaterializationWork,
        guard: &mut Option<Self>,
    ) -> Result<(), NativeMountError> {
        Self::acquire_held(destination, directory, None, native, guard, true)
    }

    fn acquire_held(
        destination: &Path,
        directory: cap_std::fs::Dir,
        held_parent: Option<cap_std::fs::Dir>,
        native: &NativeMaterializationWork,
        guard: &mut Option<Self>,
        recovery: bool,
    ) -> Result<(), NativeMountError> {
        use crate::native_host::NativeHostWork as _;
        let work = &mut &*native;
        if guard.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        #[cfg(unix)]
        let (canonical_owner, held_recovery_directory) = if recovery {
            let copy = work.attempt_io(
                crate::materializer::NativeResourceCredit::ReadOperations.delta(1),
                || directory.try_clone(),
            )?;
            (
                crate::native_host::canonical_directory_path_held(copy, work)?,
                Some(directory),
            )
        } else {
            (
                crate::native_host::canonical_directory_path_held(directory, work)?,
                None,
            )
        };
        #[cfg(windows)]
        let canonical_owner = crate::native_host::canonical_directory_wide(&directory, work)?;
        // The actual native allocation guard stays after the path in this
        // tuple, including early-error and panic destruction.
        let canonical = &canonical_owner.0;
        #[cfg(unix)]
        let canonical_bytes = canonical.as_os_str().len() as u64;
        #[cfg(windows)]
        let canonical_bytes = canonical
            .len()
            .checked_mul(std::mem::size_of::<u16>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        let leaf = if recovery {
            Some(
                destination
                    .file_name()
                    .ok_or(NativeMountError::InvalidDestination)?,
            )
        } else {
            None
        };
        #[cfg(unix)]
        let separator = recovery && !canonical.as_os_str().as_encoded_bytes().ends_with(b"/");
        #[cfg(windows)]
        let separator = recovery
            && !canonical
                .last()
                .is_some_and(|unit| *unit == u16::from(b'\\') || *unit == u16::from(b'/'));
        #[cfg(unix)]
        let suffix_bytes = leaf.map_or(0, |leaf| leaf.len() as u64) + u64::from(separator);
        #[cfg(windows)]
        let suffix_bytes = {
            use std::os::windows::ffi::OsStrExt;
            leaf.map_or(0, |leaf| leaf.encode_wide().count() as u64 * 2) + u64::from(separator) * 2
        };
        let hashed = crate::WorkCounters {
            bytes_hashed: canonical_bytes
                .checked_add(suffix_bytes)
                .and_then(|bytes| {
                    bytes.checked_add(b"acyclic-fs/native-mount-destination/v1\0".len() as u64)
                })
                .ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        };
        let result = Self::fence_name(canonical, leaf, separator, hashed, native);
        let recovery_directory: Result<Option<cap_std::fs::Dir>, NativeMountError> =
            if recovery && result.is_ok() {
                #[cfg(unix)]
                {
                    Ok(held_recovery_directory)
                }
                #[cfg(windows)]
                {
                    Ok(Some(directory))
                }
            } else {
                Ok(None)
            };
        let (canonical, canonical_allocation) = canonical_owner;
        drop(canonical);
        work.release(canonical_allocation)?;
        let name = result?;
        // Prepare all retirement names and accepted controls before either
        // registry or fence creation can mutate the destination namespace.
        let (cleanup_name, birth_cleanup) = Self::prepare_cleanup(&name, native)?;
        // The namespace parent is the capability used to open this child.
        // Never resolve the lexical parent again after opening the root: an
        // alias replacement could otherwise place its fence in another tree.
        let parent_directory = held_parent
            .or(recovery_directory?)
            .ok_or(NativeMountError::ControlPending)?;
        Self::acquire_files(
            cleanup_name,
            birth_cleanup,
            parent_directory,
            name,
            recovery,
            guard,
            native,
        )
    }

    fn destination_directory(
        destination: &Path,
        allow_nonempty: bool,
        recovery: bool,
        native: &NativeMaterializationWork,
    ) -> Result<(cap_std::fs::Dir, Option<cap_std::fs::Dir>), NativeMountError> {
        use crate::native_host::NativeHostWork as _;
        let work = &mut &*native;
        let recovery_parent = if recovery {
            let parent = destination
                .parent()
                .ok_or(NativeMountError::InvalidDestination)?;
            if parent.as_os_str().is_empty() {
                return Err(NativeMountError::InvalidDestination);
            }
            Some(parent)
        } else {
            None
        };
        let (directory, parent) = if let Some(parent) = recovery_parent {
            (
                crate::native_host::HostRoot::open(parent, work)?.directory,
                None,
            )
        } else {
            let parent = destination
                .parent()
                .ok_or(NativeMountError::InvalidDestination)?;
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            let leaf = destination
                .file_name()
                .ok_or(NativeMountError::InvalidDestination)?;
            let parent = crate::native_host::HostRoot::open(parent, work)?;
            let child = parent.open_directory(Path::new(leaf), work)?;
            (child.directory, Some(parent.directory))
        };
        if !allow_nonempty {
            let (mut reader, allocation) = crate::native_host::read_dir_held(&directory, work)?;
            #[cfg(unix)]
            let result = reader.next(work);
            #[cfg(windows)]
            let result = reader.next(work);
            drop(reader);
            work.release(allocation)?;
            match result? {
                None => {}
                Some((entry, allocation)) => {
                    drop(entry);
                    work.release(allocation)?;
                    return Err(NativeMountError::InvalidDestination);
                }
            }
        }
        Ok((directory, parent))
    }

    fn fence_name(
        #[cfg(unix)] canonical: &Path,
        #[cfg(windows)] canonical: &[u16],
        leaf: Option<&std::ffi::OsStr>,
        separator: bool,
        hashed: crate::WorkCounters,
        native: &NativeMaterializationWork,
    ) -> Result<[u8; NATIVE_FENCE_NAME_BYTES], NativeMountError> {
        use crate::native_host::NativeHostWork as _;
        let work = &mut &*native;
        work.check()?;
        work.admit(hashed)?;
        let mut identity = blake3::Hasher::new();
        identity.update(b"acyclic-fs/native-mount-destination/v1\0");
        #[cfg(unix)]
        update_path_identity(&mut identity, canonical);
        #[cfg(windows)]
        for unit in canonical {
            identity.update(&unit.to_le_bytes());
        }
        if let Some(leaf) = leaf {
            #[cfg(unix)]
            if separator {
                identity.update(b"/");
            }
            #[cfg(windows)]
            if separator {
                identity.update(&u16::from(b'\\').to_le_bytes());
            }
            update_path_identity(&mut identity, Path::new(leaf));
        }
        work.record(hashed)?;
        let hex = identity.finalize().to_hex();
        let mut name = [0u8; NATIVE_FENCE_NAME_BYTES];
        for (destination, byte) in name.iter_mut().zip(
            NATIVE_FENCE_PREFIX
                .bytes()
                .chain(hex.bytes())
                .chain(NATIVE_FENCE_SUFFIX.bytes()),
        ) {
            *destination = byte;
        }
        Ok::<_, NativeMountError>(name)
    }

    fn prepare_cleanup(
        name: &[u8; NATIVE_FENCE_NAME_BYTES],
        native: &NativeMaterializationWork,
    ) -> Result<
        (
            Option<crate::native_host::PreparedAsciiLeaf<{ NATIVE_FENCE_NAME_BYTES + 1 }>>,
            Option<crate::NativeWorkReservation>,
        ),
        NativeMountError,
    > {
        let mut terminated = [0u8; NATIVE_FENCE_NAME_BYTES + 1];
        let copied = native.reserve_work(
            crate::materializer::NativeResourceCredit::Copy.delta(NATIVE_FENCE_NAME_BYTES as u64),
        )?;
        terminated[..NATIVE_FENCE_NAME_BYTES].copy_from_slice(name);
        copied.record_completed(
            crate::materializer::NativeResourceCredit::Copy.delta(NATIVE_FENCE_NAME_BYTES as u64),
        )?;
        let prepared = crate::native_host::prepare_ascii_leaf(terminated, native)?;
        let credit = native.reserve_work(Self::cleanup_work())?;
        Ok((Some(prepared), Some(credit)))
    }

    fn cleanup_work() -> crate::WorkCounters {
        crate::WorkCounters {
            // Registry lock, exact fence unlink, and registry unlock. The
            // accepted unlock remains funded even if caller waiting stops.
            backend_write_operations: 3,
            #[cfg(windows)]
            backend_read_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        }
    }

    fn acquire_files(
        cleanup_name: Option<
            crate::native_host::PreparedAsciiLeaf<{ NATIVE_FENCE_NAME_BYTES + 1 }>,
        >,
        birth_cleanup: Option<crate::NativeWorkReservation>,
        parent_directory: cap_std::fs::Dir,
        name: [u8; NATIVE_FENCE_NAME_BYTES],
        recovery: bool,
        guard: &mut Option<Self>,
        native: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let work = &mut &*native;
        let registry = registry::shared(work)?;
        let held = registry.lock(work)?;
        let file = crate::native_host::open_ascii_leaf(
            &parent_directory,
            cleanup_name
                .as_ref()
                .ok_or(NativeMountError::ControlPending)?,
            &crate::native_host::NativeOpenOptions::ReadWriteCreate,
            work,
        )?;
        let admitted = Self {
            file: Some(file.into()),
            lock_path: PathBuf::new(),
            parent_directory: Some(parent_directory),
            fence_name: Some(name),
            registry_file: Some(crate::sync::native::Arc::clone(&registry)),
            cleanup_name,
            birth_cleanup,
            destination_cleanup: None,
        };
        if recovery {
            // Even a post-open lock failure belongs to the preborn admin
            // worker. It closes the actual file using its original credit.
            *guard = Some(admitted);
            lock_mount_file_with_work(
                guard
                    .as_ref()
                    .and_then(|guard| guard.file.as_ref())
                    .ok_or(NativeMountError::ControlPending)?,
                false,
                work,
            )?;
        } else {
            lock_mount_file_with_work(
                admitted
                    .file
                    .as_ref()
                    .ok_or(NativeMountError::ControlPending)?,
                false,
                work,
            )?;
            *guard = Some(admitted);
        }
        #[cfg(test)]
        super::tests::fence::before_registry_unlock(native);
        held.unlock(work)
    }

    #[cfg(test)]
    pub(super) fn acquire_canonical(canonical: &Path) -> Result<Self, NativeMountError> {
        let (lock_path, registry_path) = Self::paths_for_canonical(canonical)?;
        let registry = acquire_mount_lock(&registry_path, true)?;
        let file = acquire_mount_lock(&lock_path, false)?;
        drop(registry);
        Ok(Self {
            file: Some(file.into()),
            lock_path,
            parent_directory: None,
            fence_name: None,
            registry_file: None,
            cleanup_name: None,
            birth_cleanup: None,
            destination_cleanup: None,
        })
    }

    pub(super) fn paths_for_canonical(
        canonical: &Path,
    ) -> Result<(PathBuf, PathBuf), NativeMountError> {
        let parent = canonical
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?;
        let mut identity = blake3::Hasher::new();
        identity.update(b"acyclic-fs/native-mount-destination/v1\0");
        update_path_identity(&mut identity, canonical);
        let lock_path = parent.join(format!(
            ".acyclic-fs-mount-{}.lock",
            identity.finalize().to_hex()
        ));
        let registry_path = registry::path()?;
        Ok((lock_path, registry_path))
    }

    pub(super) fn reclaim_canonical(canonical: &Path) -> Result<(), NativeMountError> {
        let (lock_path, registry_path) = Self::paths_for_canonical(canonical)?;
        let registry = acquire_mount_lock(&registry_path, true)?;
        if !lock_path
            .try_exists()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?
        {
            return Ok(());
        }
        let file = acquire_existing_mount_lock(&lock_path)?;
        drop(file);
        match std::fs::remove_file(&lock_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NativeMountError::Driver(error.to_string())),
        }
        drop(registry);
        Ok(())
    }

    /// Administrative `LastDrop` closes only the original held fence.
    pub(super) fn close_administrative_fence(
        &mut self,
        original: &mut crate::NativeWorkReservation,
    ) {
        if self.file.is_some() {
            original.started_drop_io(
                crate::materializer::NativeResourceCredit::WriteOperations.delta(1),
            );
            drop(self.file.take());
        }
    }

    pub(super) fn release_cleanup(
        &mut self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        if self.file.is_none() {
            return Ok(());
        }
        let invalid = || {
            NativeMountError::Materialization(crate::NativeTreeMaterializationError::InvalidLayout)
        };
        let directory = self.parent_directory.as_ref().ok_or_else(invalid)?;
        let registry = self.registry_file.as_ref().ok_or_else(invalid)?;
        let name = self.cleanup_name.as_ref().ok_or_else(invalid)?;
        let credit = self.birth_cleanup.as_mut().ok_or_else(invalid)?;
        // Fund the whole transaction before acquiring the global registry.
        // The unlock has exclusive credit that unlink cannot consume.
        credit.refill_cleanup(Self::cleanup_work())?;
        let unlock = credit.partition_work(
            crate::materializer::NativeResourceCredit::WriteOperations.delta(1),
            wait,
        )?;
        let mut work = NativeFenceReleaseWork {
            credit,
            wait,
            pending: None,
        };
        // Retirement owns its original cleanup credit while the OS waits.
        // Contention is one blocking syscall, not repeated charged attempts
        // which could exhaust birth credit before the fence is removed.
        let held = registry.lock(&mut work)?;
        // Completing the accepted retirement owns its birth credit even if
        // the caller stops waiting after registry acquisition.
        work.wait = None;
        let removed = crate::native_host::remove_ascii_leaf(directory, name, &mut work);
        // The held lock must be released on every unlink outcome. Caller
        // cancellation can stop new work, but cannot revoke this accepted
        // physical release and strand all future starters behind the lock.
        let unlocked = held.unlock(&mut NativeFenceReleaseWork {
            credit: &unlock,
            wait: None,
            pending: None,
        });
        match removed {
            Ok(()) => {}
            Err(crate::NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        unlocked?;
        self.file.take();
        Ok(())
    }

    pub(super) fn release<W: crate::native_host::NativeHostWork>(
        &mut self,
        work: &mut W,
    ) -> Result<(), NativeMountError>
    where
        NativeMountError: From<W::Error>,
    {
        if self.file.is_none() {
            return Ok(());
        }
        let opened_directory;
        let directory = if let Some(directory) = self.parent_directory.as_ref() {
            directory
        } else {
            let parent = self
                .lock_path
                .parent()
                .ok_or(NativeMountError::InvalidDestination)?;
            opened_directory = crate::native_host::open_root_directory(parent, work)
                .map(|(directory, _)| directory)?;
            &opened_directory
        };
        let lock_name = match self.fence_name.as_ref() {
            Some(name) => Path::new(
                std::str::from_utf8(name).map_err(|_| NativeMountError::InvalidDestination)?,
            ),
            None => Path::new(
                self.lock_path
                    .file_name()
                    .ok_or(NativeMountError::InvalidDestination)?,
            ),
        };
        let registry = registry::shared(work)?;
        let held = registry.lock(work)?;
        match crate::native_host::remove_leaf(directory, lock_name, false, work) {
            Ok(()) => {}
            Err(error) if W::io_error_kind(&error) == Some(std::io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        self.file.take();
        drop(held);
        Ok(())
    }
}

pub(super) fn lock_mount_file_with_work<W: crate::native_host::NativeHostWork>(
    file: &MountLockFile,
    blocking: bool,
    work: &mut W,
) -> Result<(), NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    let delta = crate::materializer::NativeResourceCredit::WriteOperations.delta(1);
    work.attempt_io(delta, || {
        let result = if blocking {
            fence_io::lock(file)
        } else {
            fence_io::try_lock(file)
        };
        match result {
            Err(error) if lock_is_contended(&error) => Ok(false),
            result => result.map(|()| true),
        }
    })
    .map_err(NativeMountError::from)?
    .then_some(())
    .ok_or(NativeMountError::DestinationBusy)
}

impl Drop for MountDestinationGuard {
    fn drop(&mut self) {
        // A finite fence is retired only by its original invocation. Dropping
        // it must not invent an unbounded cleanup engine after denied work.
        if self.parent_directory.is_none() && self.file.is_some() {
            let cancellation = crate::CancellationToken::new();
            let mut work =
                crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
            let _ = self.release(&mut work);
        }
    }
}

pub(super) fn acquire_mount_lock(
    path: &Path,
    blocking: bool,
) -> Result<MountLockFile, NativeMountError> {
    open_mount_lock(
        path,
        &crate::native_host::NativeOpenOptions::ReadWriteCreate,
        blocking,
    )
}

pub(super) fn acquire_existing_mount_lock(path: &Path) -> Result<MountLockFile, NativeMountError> {
    open_mount_lock(
        path,
        &crate::native_host::NativeOpenOptions::ReadWrite,
        false,
    )
}

fn open_mount_lock(
    path: &Path,
    options: &crate::native_host::NativeOpenOptions,
    blocking: bool,
) -> Result<MountLockFile, NativeMountError> {
    let cancellation = crate::CancellationToken::new();
    let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
    let parent = path.parent().ok_or(NativeMountError::InvalidDestination)?;
    let leaf = path
        .file_name()
        .ok_or(NativeMountError::InvalidDestination)?;
    let (parent, _) = crate::native_host::open_root_directory(parent, &mut work)?;
    // Every entry point uses held-parent, no-follow opening and the same lock
    // syscall shim. A lexical metadata check after opening could follow a
    // replaced symlink and cannot establish the lock's identity.
    let file = crate::native_host::open_leaf(&parent, Path::new(leaf), options, &mut work)?;
    let file = file.into();
    lock_mount_file_with_work(&file, blocking, &mut work)?;
    Ok(file)
}

pub(super) fn lock_is_contended(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(windows)]
    {
        // Windows reports ERROR_LOCK_VIOLATION rather than WouldBlock.
        error.raw_os_error() == Some(33)
    }
    #[cfg(not(windows))]
    {
        false
    }
}

#[cfg(unix)]
pub(super) fn update_path_identity(hasher: &mut blake3::Hasher, path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    hasher.update(path.as_os_str().as_bytes());
}

#[cfg(windows)]
pub(super) fn update_path_identity(hasher: &mut blake3::Hasher, path: &Path) {
    use std::os::windows::ffi::OsStrExt;
    for unit in path.as_os_str().encode_wide() {
        hasher.update(&unit.to_le_bytes());
    }
}

#[cfg(not(any(unix, windows)))]
pub(super) fn update_path_identity(hasher: &mut blake3::Hasher, path: &Path) {
    hasher.update(path.to_string_lossy().as_bytes());
}

#[cfg(test)]
#[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
mod models;

#[cfg(test)]
mod cleanup_credit_tests {
    use super::*;

    #[test]
    fn fence_stays_with_its_opened_parent_after_path_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::native_mount::registry::initialize()?;
        let temporary = tempfile::tempdir()?;
        let original = temporary.path().join("original");
        let replacement = temporary.path().join("replacement");
        let alias = temporary.path().join("alias");
        std::fs::create_dir_all(original.join("parent/mount"))?;
        std::fs::create_dir_all(replacement.join("parent/mount"))?;
        directory_alias(&alias, &original)?;
        let destination = alias.join("parent/mount");
        let work = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        let (directory, held_parent) =
            MountDestinationGuard::destination_directory(&destination, false, false, &work)?;
        let expected = crate::native_host::directory_identity(
            held_parent.as_ref().ok_or("missing held parent")?,
            &mut &work,
        )?;
        // Windows forbids renaming an ancestor with an open descendant. An
        // intermediate junction can still be replaced. Exercise that actual
        // alias race on both hosts without renaming the held directory.
        remove_directory_alias(&alias)?;
        directory_alias(&alias, &replacement)?;
        let mut guard = None;
        MountDestinationGuard::acquire_held(
            &destination,
            directory,
            held_parent,
            &work,
            &mut guard,
            false,
        )?;
        let mut guard = guard.ok_or("missing fence owner")?;
        let actual = crate::native_host::directory_identity(
            guard
                .parent_directory
                .as_ref()
                .ok_or("missing fence parent")?,
            &mut &work,
        )?;
        let name = std::str::from_utf8(guard.fence_name.as_ref().ok_or("missing fence name")?)?;
        let stays_with_root = original.join("parent").join(name).is_file()
            && !replacement.join("parent").join(name).exists();
        // Drain the owned fence before any control assertion, including the
        // old lexical-reopen negative, so a failed proof leaves no live lock.
        guard.release_cleanup(Some(&crate::CancellationToken::new()))?;
        assert_eq!(actual, expected);
        assert!(stays_with_root);
        Ok(())
    }

    fn directory_alias(alias: &Path, target: &Path) -> std::io::Result<()> {
        #[cfg(windows)]
        {
            crate::windows_test_filesystems::junction(alias, target)
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, alias)
        }
    }

    fn remove_directory_alias(alias: &Path) -> std::io::Result<()> {
        #[cfg(windows)]
        {
            std::fs::remove_dir(alias)
        }
        #[cfg(unix)]
        {
            std::fs::remove_file(alias)
        }
    }

    #[test]
    fn prepared_fence_cleanup_needs_no_second_name_scan() -> Result<(), Box<dyn std::error::Error>>
    {
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(
            crate::WorkBudget {
                items_examined: 0,
                ..crate::WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        // PreparedAsciiLeaf already owns validation. Reserve the actual
        // retirement transaction with no remaining name-validation credit.
        let _credit = work.reserve_work(MountDestinationGuard::cleanup_work())?;
        assert_eq!(work.receipt().items_examined, 0);
        Ok(())
    }
}
