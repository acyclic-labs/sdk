use super::*;

#[derive(Debug)]
pub(super) struct RecordPaths<'a> {
    pub(super) current: PathBuf,
    pub(super) previous: PathBuf,
    pub(super) temporary: PathBuf,
    pub(super) lock: PathBuf,
    pub(super) namespace: PhantomData<&'a Namespace>,
    pub(super) allocation: Option<crate::NativeAllocation>,
}

impl RecordPaths<'_> {
    pub(super) fn directory(&self) -> &Path {
        self.current.parent().expect("record paths have a family")
    }

    fn read<T: DeserializeOwned>(
        &self,
        path: &Path,
    ) -> Result<Option<(T, Option<crate::NativeAllocation>)>, LocalCoreStateStoreError> {
        match &self.allocation {
            Some(allocation) => read_file(path, allocation.work(), None)
                .map(|value| value.map(|(value, allocation)| (value, Some(allocation)))),
            None => read_json(path, None).map(|value| value.map(|value| (value, None))),
        }
    }

    pub(super) fn sync(&self) -> Result<(), LocalCoreStateStoreError> {
        if let Some(allocation) = &self.allocation {
            let work = allocation.work();
            let root = crate::native_host::HostRoot::open(self.directory(), &mut &*work)?;
            crate::native_host::sync_directory_held(&root.directory, &mut &*work)?;
        } else {
            sync_directory(self.directory())?;
        }
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, LocalCoreStateStoreError> {
        let Some(allocation) = &self.allocation else {
            return Ok(path.exists());
        };
        let work = allocation.work();
        let parent = materialization_parent(path, work)?;
        let leaf = path.file_name().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal file has no leaf")
        })?;
        Ok(crate::native_host::entry_identity(&parent, Path::new(leaf), &mut &*work)?.is_some())
    }

    #[cfg(not(windows))]
    fn rename(&self, from: &Path, to: &Path) -> Result<(), LocalCoreStateStoreError> {
        let Some(allocation) = &self.allocation else {
            return std::fs::rename(from, to).map_err(Into::into);
        };
        let work = allocation.work();
        let parent = materialization_parent(from, work)?;
        let source = from.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "journal source has no leaf",
            )
        })?;
        let target = to.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "journal destination has no leaf",
            )
        })?;
        crate::native_host::rename_held(
            &parent,
            Path::new(source),
            &parent,
            Path::new(target),
            acyclic_native_runtime::RenameMode::NoReplace,
            None,
            &mut &*work,
        )?;
        Ok(())
    }

    pub(super) fn copy(&self, from: &Path, to: &Path) -> Result<(), LocalCoreStateStoreError> {
        let Some(allocation) = &self.allocation else {
            std::fs::copy(from, to)?;
            OpenOptions::new().write(true).open(to)?.sync_all()?;
            return Ok(());
        };
        let work = allocation.work();
        let file =
            materialization_open_options(from, &crate::native_host::NativeOpenOptions::Read, work)?;
        let length = crate::native_host::stat_file(&file, &mut &*work)?.len();
        let mut source = MaterializationReader {
            file: &file,
            work,
            remaining_file_bytes: length,
            original_length: length,
        };
        let file = materialization_open_options(
            to,
            &crate::native_host::NativeOpenOptions::ReadWriteCreate,
            work,
        )?;
        crate::native_host::truncate_file(&file, 0, &mut &*work)?;
        let mut destination = MaterializationWriter {
            file,
            work: Some(work),
            encoded: false,
        };
        let mut bytes = [0; 64 * 1024];
        loop {
            let count = source.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            destination.write_all(&bytes[..count])?;
        }
        crate::native_host::sync_file_held(&destination.file, &mut &*work)?;
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<(), LocalCoreStateStoreError> {
        let Some(allocation) = &self.allocation else {
            return remove_if_present(path).map_err(Into::into);
        };
        let work = allocation.work();
        let parent = materialization_parent(path, work)?;
        let leaf = path.file_name().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal file has no leaf")
        })?;
        match crate::native_host::remove_leaf(&parent, Path::new(leaf), false, &mut &*work) {
            Ok(()) => Ok(()),
            Err(crate::NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

pub(super) fn compare_and_swap<T: DeserializeOwned + Serialize + Revisioned>(
    paths: &mut RecordPaths,
    expected_revision: u64,
    replacement: &T,
) -> Result<bool, LocalCoreStateStoreError> {
    with_lock(paths, |paths| {
        let current = read_recoverable::<T>(paths)?;
        if current.as_ref().map_or(0, |(value, _)| value.revision()) != expected_revision {
            return Ok(false);
        }
        write_journaled(paths, replacement)?;
        Ok(true)
    })
}

/// Removes a record with its recovery copies.
pub(super) fn remove_record(paths: &RecordPaths) -> Result<(), LocalCoreStateStoreError> {
    paths.remove(&paths.temporary)?;
    paths.remove(&paths.previous)?;
    paths.remove(&paths.current)?;
    paths.sync()?;
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) enum LockMode {
    Shared,
    Exclusive,
}

pub(super) fn with_lock<T>(
    paths: &mut RecordPaths,
    action: impl FnOnce(&RecordPaths) -> Result<T, LocalCoreStateStoreError>,
) -> Result<T, LocalCoreStateStoreError> {
    with_lock_with_retry(paths, action, retry_record_lock)
}

pub(super) fn with_lock_with_retry<T>(
    paths: &mut RecordPaths,
    action: impl FnOnce(&RecordPaths) -> Result<T, LocalCoreStateStoreError>,
    retry: impl FnMut(Instant) -> bool,
) -> Result<T, LocalCoreStateStoreError> {
    let mut lock = acquire_lock_with_retry(paths, LockMode::Exclusive, retry)?;
    let result = action(paths);
    let unlocked = lock.unlock();
    match (result, unlocked) {
        (Err(error), _) if paths.allocation.is_some() => Err(error),
        (_, Err(error)) => Err(error),
        (result, _) => result,
    }
}

pub(super) fn read_locked<T: DeserializeOwned>(
    paths: &mut RecordPaths,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    read_locked_with_retry(paths, retry_record_lock)
}

pub(super) fn read_locked_with_retry<T: DeserializeOwned>(
    paths: &mut RecordPaths,
    mut retry: impl FnMut(Instant) -> bool,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let mut lock = acquire_lock_with_retry(paths, LockMode::Shared, &mut retry)?;
    let current = read_json(&paths.current, None).ok().flatten();
    lock.unlock()?;
    drop(lock);
    match current {
        Some(value) => Ok(Some(value)),
        None => with_lock_with_retry(
            paths,
            |paths| read_recoverable(paths).map(|value| value.map(|(value, _)| value)),
            retry,
        ),
    }
}

pub(super) fn acquire_lock(
    paths: &mut RecordPaths,
    mode: LockMode,
) -> Result<OwnerLock, LocalCoreStateStoreError> {
    acquire_lock_with_retry(paths, mode, retry_record_lock)
}

pub(super) fn retry_record_lock(deadline: Instant) -> bool {
    retry_record_lock_at(deadline, Instant::now(), std::thread::sleep)
}

pub(super) fn retry_record_lock_at(
    deadline: Instant,
    now: Instant,
    wait: impl FnOnce(Duration),
) -> bool {
    if now >= deadline {
        return false;
    }
    wait(RECORD_LOCK_RETRY);
    true
}

fn acquire_lock_with_retry(
    paths: &mut RecordPaths,
    mode: LockMode,
    mut retry: impl FnMut(Instant) -> bool,
) -> Result<OwnerLock, LocalCoreStateStoreError> {
    let work = paths
        .allocation
        .as_ref()
        .map(|allocation| allocation.work().clone());
    let lock = if let Some(work) = &work {
        crate::native_host::create_directories(paths.directory(), &mut &*work)?;
        materialization_open_options(
            &paths.lock,
            &crate::native_host::NativeOpenOptions::ReadWriteCreate,
            work,
        )?
    } else {
        std::fs::create_dir_all(paths.directory())?;
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&paths.lock)?
    };
    let cleanup = work
        .as_ref()
        .map(|work| work.reserve_cleanup_write())
        .transpose()?;
    let deadline = Instant::now() + RECORD_LOCK_DEADLINE;
    loop {
        if let Some(work) = &work {
            work.charge(crate::WorkCounters {
                backend_read_operations: 1,
                ..Default::default()
            })?;
        }
        let locked = match mode {
            LockMode::Shared => FileExt::try_lock_shared(&lock),
            LockMode::Exclusive => lock.try_lock_exclusive(),
        };
        match locked {
            Ok(()) => break,
            Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => {
                if !retry(deadline) {
                    if let Some(mut allocation) = paths.allocation.take() {
                        drop((
                            std::mem::take(&mut paths.current),
                            std::mem::take(&mut paths.previous),
                            std::mem::take(&mut paths.temporary),
                        ));
                        allocation.resize(paths.lock.capacity() as u64)?;
                        return Err(LocalCoreStateStoreError::LazyWork(
                            crate::LazyWorkspaceError::NativeRecordLockTimeout(
                                std::mem::take(&mut paths.lock),
                                Some(allocation),
                            ),
                        ));
                    }
                    return Err(LocalCoreStateStoreError::LockTimeout(paths.lock.clone()));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(OwnerLock::new(lock, cleanup))
}

pub(super) fn read_recoverable<T: DeserializeOwned>(
    paths: &RecordPaths,
) -> Result<Option<(T, Option<crate::NativeAllocation>)>, LocalCoreStateStoreError> {
    match paths.read(&paths.current) {
        Ok(Some(value)) => Ok(Some(value)),
        Err(error)
            if paths.allocation.is_some()
                && (matches!(
                    error,
                    LocalCoreStateStoreError::Io(_) | LocalCoreStateStoreError::LazyWork(_)
                ) || matches!(&error, LocalCoreStateStoreError::Json(error) if error.is_io())) =>
        {
            Err(error)
        }
        current => match paths.read(&paths.previous) {
            Ok(Some(value)) => {
                promote_previous(paths)?;
                Ok(Some(value))
            }
            Err(previous) if paths.allocation.is_some() || current.is_ok() => Err(previous),
            previous => match current {
                Ok(None) => previous,
                current => current,
            },
        },
    }
}

pub(super) fn promote_previous(paths: &RecordPaths) -> Result<(), LocalCoreStateStoreError> {
    paths.copy(&paths.previous, &paths.current)?;
    paths.sync()
}

pub(super) fn read_json<T: DeserializeOwned>(
    path: &Path,
    maximum_bytes: Option<u64>,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if let Some(maximum) = maximum_bytes
        && file.metadata()?.len() > maximum
    {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    let mut bytes = Vec::new();
    match maximum_bytes {
        Some(maximum) => {
            file.take(maximum.saturating_add(1))
                .read_to_end(&mut bytes)?;
        }
        None => {
            file.read_to_end(&mut bytes)?;
        }
    }
    if let Some(maximum) = maximum_bytes
        && bytes.len() as u64 > maximum
    {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(LocalCoreStateStoreError::Json)
}

// Ordinary records also need token coalescing. Flush the buffer before the
// caller's file durability barrier; encoding or flush failures cannot publish.
pub(super) fn write_buffered_json(
    writer: impl Write,
    value: &impl Serialize,
) -> Result<(), LocalCoreStateStoreError> {
    let mut writer = std::io::BufWriter::with_capacity(8192, writer);
    serde_json::to_writer(&mut writer, value)?;
    writer.flush()?;
    Ok(())
}

pub(super) fn write_journaled<T: Serialize>(
    paths: &RecordPaths,
    value: &T,
) -> Result<(), LocalCoreStateStoreError> {
    let work = paths
        .allocation
        .as_ref()
        .map(|allocation| allocation.work());
    let file = match work {
        Some(work) => {
            let file = materialization_open_options(
                &paths.temporary,
                &crate::native_host::NativeOpenOptions::ReadWriteCreate,
                work,
            )?;
            crate::native_host::truncate_file(&file, 0, &mut &*work)?;
            file
        }
        None => OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&paths.temporary)?,
    };
    let mut temporary = MaterializationWriter {
        file,
        work,
        encoded: false,
    };
    if let Some(work) = work {
        // Encode once on the original ledger. Writing each serde token as a
        // separate syscall creates thousands of needless interruption points.
        let mut encoded = LogBuffer {
            bytes: Vec::new(),
            allocation: Some(work.claim(0, 0, crate::WorkCounters::UNCHARGED)?),
        };
        serde_json::to_writer(&mut encoded, value)?;
        temporary.write_all(&encoded.bytes)?;
    } else {
        write_buffered_json(&mut temporary, value)?;
    }
    match work {
        Some(work) => crate::native_host::sync_file_held(&temporary.file, &mut &*work)?,
        None => temporary.file.sync_all()?,
    }
    drop(temporary);

    #[cfg(windows)]
    {
        // Keep a flushed recovery copy until the replacement is durable.
        paths.remove(&paths.previous)?;
        if paths.exists(&paths.current)? {
            paths.copy(&paths.current, &paths.previous)?;
        } else {
            paths.copy(&paths.temporary, &paths.previous)?;
        }
        paths.copy(&paths.temporary, &paths.current)?;
        paths.remove(&paths.temporary)?;
        paths.remove(&paths.previous)?;
        Ok(())
    }

    #[cfg(not(windows))]
    {
        paths.remove(&paths.previous)?;
        if paths.exists(&paths.current)? {
            paths.rename(&paths.current, &paths.previous)?;
        }
        if let Err(error) = paths.rename(&paths.temporary, &paths.current) {
            if paths.exists(&paths.previous).unwrap_or(false)
                && !paths.exists(&paths.current).unwrap_or(true)
            {
                let _ = paths.rename(&paths.previous, &paths.current);
            }
            return Err(error.into());
        }
        paths.sync()?;
        paths.remove(&paths.previous)?;
        Ok(())
    }
}

pub(super) fn remove_if_present(path: &Path) -> Result<(), std::io::Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Makes the entries of directory `path` durable, with the one directory
/// synchronization every store shares; Windows flushes a directory handle.
pub(super) fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    acyclic_native_runtime::sync_parent(path, acyclic_native_runtime::Durability::Full)
}
