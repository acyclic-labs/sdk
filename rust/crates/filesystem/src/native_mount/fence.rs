use super::*;

pub(super) const NATIVE_FENCE_PREFIX: &str = ".acyclic-fs-mount-";
pub(super) const NATIVE_FENCE_SUFFIX: &str = ".lock";
pub(super) const NATIVE_FENCE_NAME_BYTES: usize =
    NATIVE_FENCE_PREFIX.len() + 64 + NATIVE_FENCE_SUFFIX.len();
pub(super) const NATIVE_REGISTRY_NAME: &str = ".acyclic-fs-mount-registry.lock";

#[derive(Debug)]
pub(super) struct MountDestinationGuard {
    pub(super) file: Option<File>,
    pub(super) lock_path: PathBuf,
    // Retain the physical parent and fixed leaf so replacing an alias cannot redirect cleanup.
    pub(super) parent_directory: Option<cap_std::fs::Dir>,
    pub(super) fence_name: Option<[u8; NATIVE_FENCE_NAME_BYTES]>,
    pub(super) registry_file: Option<File>,
    pub(super) cleanup_name:
        Option<crate::native_host::NativePreparedAsciiLeaf<{ NATIVE_FENCE_NAME_BYTES + 1 }>>,
    pub(super) birth_cleanup: Option<crate::NativeWorkReservation>,
}

/// The accepted fence path has no allocator or new cleanup-reservation ingress.
pub(super) struct NativeFenceReleaseWork<'a, 'credit> {
    pub(super) loan: &'a crate::materializer::NativeCleanupLoan<'credit>,
    pub(super) pending: Option<crate::NativeWorkReservation>,
}

impl crate::native_host::NativeHostWork for NativeFenceReleaseWork<'_, '_> {
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
        self.loan.check_wait()
    }
    fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        if self.pending.is_some() {
            return Err(Self::Error::InvalidLayout);
        }
        self.pending = Some(self.loan.admit_completed(delta)?);
        Ok(())
    }
    fn record(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.pending
            .take()
            .ok_or(Self::Error::InvalidLayout)?
            .record_completed(delta)
    }
    fn started_io(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.loan.started_io(delta)
    }
    fn claim(&mut self, _bytes: u64) -> Result<Self::Allocation, Self::Error> {
        Err(Self::Error::InvalidLayout)
    }
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
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        Err(Self::Error::InvalidLayout)
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        match reservation {}
    }
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
        use crate::native_host::NativeHostWork as _;
        let work = &mut &*native;
        if guard.is_some() {
            return Err(NativeMountError::ControlPending);
        }
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
        let directory = crate::native_host::open_root_directory(recovery_parent.unwrap_or(destination), work).map(|(directory, _)| directory)?;
        if !allow_nonempty {
            let (mut reader, allocation) =
                crate::native_host::read_dir_held(&directory, work)?;
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
        #[cfg(unix)]
        let canonical_owner =
            crate::native_host::canonical_directory_path_held(directory, work)?;
        #[cfg(windows)]
        let canonical_owner =
            crate::native_host::canonical_directory_wide(&directory, work)?;
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
                .is_some_and(|unit| *unit == b'\\' as u16 || *unit == b'/' as u16);
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
        let result = (|| {
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
                    identity.update(&(b'\\' as u16).to_le_bytes());
                }
                update_path_identity(&mut identity, Path::new(leaf));
            }
            work.record(hashed)?;
            let hex = identity.finalize().to_hex();
            let mut name = [0u8; NATIVE_FENCE_NAME_BYTES];
            name[..NATIVE_FENCE_PREFIX.len()].copy_from_slice(NATIVE_FENCE_PREFIX.as_bytes());
            name[NATIVE_FENCE_PREFIX.len()..NATIVE_FENCE_PREFIX.len() + 64]
                .copy_from_slice(hex.as_bytes());
            name[NATIVE_FENCE_PREFIX.len() + 64..].copy_from_slice(NATIVE_FENCE_SUFFIX.as_bytes());
            Ok::<_, NativeMountError>(name)
        })();
        let recovery_directory: Result<Option<cap_std::fs::Dir>, NativeMountError> =
            if recovery && result.is_ok() {
                #[cfg(unix)]
                {
                    crate::native_host::open_root_directory(canonical, work).map(|(directory, _)| directory)
                        .map(Some)
                        .map_err(Into::into)
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
        let (cleanup_name, birth_cleanup) = {
            let mut terminated = [0u8; NATIVE_FENCE_NAME_BYTES + 1];
            let mut copied = native.reserve_work(crate::WorkCounters {
                bytes_copied: NATIVE_FENCE_NAME_BYTES as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            terminated[..NATIVE_FENCE_NAME_BYTES].copy_from_slice(&name);
            copied.record_completed(crate::WorkCounters {
                bytes_copied: NATIVE_FENCE_NAME_BYTES as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let prepared = crate::native_host::NativePreparedAsciiLeaf::new(terminated, native)?;
            let credit = native.reserve_work(crate::WorkCounters {
                backend_write_operations: 2,
                #[cfg(windows)]
                backend_read_operations: 1,
                #[cfg(windows)]
                items_examined: NATIVE_FENCE_NAME_BYTES as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            (Some(prepared), Some(credit))
        };
        let parent = destination
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?;
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        let parent_directory = match recovery_directory? {
            Some(directory) => directory,
            None => crate::native_host::open_root_directory(parent, work).map(|(directory, _)| directory)?,
        };
        let registry = crate::native_host::open_leaf(
            &parent_directory,
            Path::new(NATIVE_REGISTRY_NAME),
            &crate::native_host::NativeOpenOptions::ReadWriteCreate,
            work,
        )?;
        lock_mount_file_with_work(&registry, true, work)?;
        let file = crate::native_host::open_leaf(
            &parent_directory,
            Path::new(
                std::str::from_utf8(&name).expect("literal prefix, hex and suffix are UTF-8"),
            ),
            &crate::native_host::NativeOpenOptions::ReadWriteCreate,
            work,
        )?;
        let admitted = Self {
            file: Some(file),
            lock_path: PathBuf::new(),
            parent_directory: Some(parent_directory),
            fence_name: Some(name),
            registry_file: Some(registry),
            cleanup_name,
            birth_cleanup,
        };
        if recovery {
            // Even a post-open lock failure belongs to the preborn admin
            // worker. It closes the actual file using its original credit.
            *guard = Some(admitted);
            lock_mount_file_with_work(
                guard
                    .as_ref()
                    .and_then(|guard| guard.file.as_ref())
                    .expect("actual opened administrative fence"),
                false,
                work,
            )?;
        } else {
            lock_mount_file_with_work(
                admitted.file.as_ref().expect("actual destination fence"),
                false,
                work,
            )?;
            *guard = Some(admitted);
        }
        let registry = guard
            .as_ref()
            .and_then(|guard| guard.registry_file.as_ref())
            .expect("the admitted fence retains its actual registry owner");
        work.attempt_io(
            crate::WorkCounters {
                backend_write_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            },
            || fs2::FileExt::unlock(registry),
        )?;
        Ok(())
    }

    pub(super) fn acquire(destination: &Path) -> Result<Self, NativeMountError> {
        let canonical = destination
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?;
        Self::acquire_canonical(&canonical)
    }

    pub(super) fn acquire_for_recovery(destination: &Path) -> Result<Self, NativeMountError> {
        let name = destination
            .file_name()
            .ok_or(NativeMountError::InvalidDestination)?;
        let parent = destination
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?;
        Self::acquire_canonical(&parent.join(name))
    }

    pub(super) fn acquire_canonical(canonical: &Path) -> Result<Self, NativeMountError> {
        let (lock_path, registry_path) = Self::paths_for_canonical(canonical)?;
        let registry = acquire_mount_lock(&registry_path, true)?;
        let file = acquire_mount_lock(&lock_path, false)?;
        drop(registry);
        Ok(Self {
            file: Some(file),
            lock_path,
            parent_directory: None,
            fence_name: None,
            registry_file: None,
            cleanup_name: None,
            birth_cleanup: None,
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
        let registry_path = parent.join(".acyclic-fs-mount-registry.lock");
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

    /// Administrative LastDrop closes only the original held fence.
    pub(super) fn close_administrative_fence(
        &mut self,
        original: &mut crate::NativeWorkReservation,
    ) {
        if self.file.is_some() {
            original.started_drop_io(crate::WorkCounters {
                backend_write_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            });
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
        let loan = credit.loan(wait);
        let mut work = NativeFenceReleaseWork {
            loan: &loan,
            pending: None,
        };
        // A contended registry is a real pending control. Do not spend an
        // unbounded sequence of new lock attempts after cancellation.
        lock_mount_file_with_work(registry, false, &mut work)?;
        match crate::native_host::remove_prepared_ascii_leaf(directory, name, &mut work) {
            Ok(()) => {}
            Err(crate::NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
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
        let directory = match self.parent_directory.as_ref() {
            Some(directory) => directory,
            None => {
                let parent = self
                    .lock_path
                    .parent()
                    .ok_or(NativeMountError::InvalidDestination)?;
                opened_directory = crate::native_host::open_root_directory(parent, work).map(|(directory, _)| directory)?;
                &opened_directory
            }
        };
        let lock_name = match self.fence_name.as_ref() {
            Some(name) => Path::new(
                std::str::from_utf8(name).expect("literal prefix, hex and suffix are UTF-8"),
            ),
            None => Path::new(
                self.lock_path
                    .file_name()
                    .ok_or(NativeMountError::InvalidDestination)?,
            ),
        };
        let registry = crate::native_host::open_leaf(
            directory,
            Path::new(NATIVE_REGISTRY_NAME),
            &crate::native_host::NativeOpenOptions::ReadWriteCreate,
            work,
        )?;
        lock_mount_file_with_work(&registry, true, work)?;
        match crate::native_host::remove_leaf(directory, lock_name, false, work) {
            Ok(()) => {}
            Err(error) if W::io_error_kind(&error) == Some(std::io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        self.file.take();
        drop(registry);
        Ok(())
    }
}

pub(super) fn lock_mount_file_with_work<W: crate::native_host::NativeHostWork>(
    file: &File,
    blocking: bool,
    work: &mut W,
) -> Result<(), NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    loop {
        let delta = crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        };
        let result = work.attempt_io(delta, || match fs2::FileExt::try_lock_exclusive(file) {
            Ok(()) => Ok(true),
            Err(error) if lock_is_contended(&error) => Ok(false),
            Err(error) => Err(error),
        });
        match result {
            Ok(true) => return Ok(()),
            Ok(false) if blocking => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Ok(false) => return Err(NativeMountError::DestinationBusy),
            Err(error) => return Err(error.into()),
        }
    }
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

pub(super) fn acquire_mount_lock(path: &Path, blocking: bool) -> Result<File, NativeMountError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let lock_metadata = std::fs::symlink_metadata(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    if lock_metadata.file_type().is_symlink() || !file.metadata().is_ok_and(|value| value.is_file())
    {
        return Err(NativeMountError::InvalidDestination);
    }
    if blocking {
        fs2::FileExt::lock_exclusive(&file)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    } else {
        fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
            if lock_is_contended(&error) {
                NativeMountError::DestinationBusy
            } else {
                NativeMountError::Driver(error.to_string())
            }
        })?;
    }
    Ok(file)
}

pub(super) fn acquire_existing_mount_lock(path: &Path) -> Result<File, NativeMountError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let lock_metadata = std::fs::symlink_metadata(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    if lock_metadata.file_type().is_symlink() || !file.metadata().is_ok_and(|value| value.is_file())
    {
        return Err(NativeMountError::InvalidDestination);
    }
    fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
        if lock_is_contended(&error) {
            NativeMountError::DestinationBusy
        } else {
            NativeMountError::Driver(error.to_string())
        }
    })?;
    Ok(file)
}

pub(super) fn lock_is_contended(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(windows)]
    {
        // LockFileEx reports ERROR_LOCK_VIOLATION rather than WouldBlock.
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
