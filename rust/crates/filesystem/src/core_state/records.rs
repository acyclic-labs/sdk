use super::*;

/// Record files confined to their admitted namespace.
#[derive(Debug)]
pub(super) struct RecordPaths<'a> {
    pub(super) directory: PathBuf,
    pub(super) current: PathBuf,
    pub(super) previous: PathBuf,
    pub(super) temporary: PathBuf,
    pub(super) lock: PathBuf,
    pub(super) namespace: PhantomData<&'a Namespace>,
    pub(super) allocation: Option<crate::materializer::NativeAllocation>,
}

pub(super) fn compare_and_swap<T: DeserializeOwned + Serialize + Revisioned>(
    paths: &RecordPaths,
    expected_revision: u64,
    replacement: &T,
) -> Result<bool, LocalCoreStateStoreError> {
    with_lock(paths, || {
        let current: Option<T> = read_recoverable(paths)?;
        if current.as_ref().map_or(0, T::revision) != expected_revision {
            return Ok(false);
        }
        write_journaled(paths, replacement)?;
        Ok(true)
    })
}

pub(super) fn compare_and_delete<T: DeserializeOwned + Revisioned>(
    paths: &RecordPaths,
    expected_revision: u64,
) -> Result<bool, LocalCoreStateStoreError> {
    with_lock(paths, || {
        let current: Option<T> = read_recoverable(paths)?;
        if current.as_ref().map_or(0, T::revision) != expected_revision {
            return Ok(false);
        }
        remove_record(paths)?;
        Ok(true)
    })
}

/// Removes a record with its recovery copies.
pub(super) fn remove_record(paths: &RecordPaths) -> Result<(), LocalCoreStateStoreError> {
    remove_if_present(&paths.temporary)?;
    remove_if_present(&paths.previous)?;
    remove_if_present(&paths.current)?;
    sync_directory(&paths.directory)?;
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) enum LockMode {
    Shared,
    Exclusive,
}

pub(super) fn with_lock<T>(
    paths: &RecordPaths,
    action: impl FnOnce() -> Result<T, LocalCoreStateStoreError>,
) -> Result<T, LocalCoreStateStoreError> {
    with_lock_mode(paths, LockMode::Exclusive, action)
}

/// Reads one record. A readable current record needs no recovery, so readers
/// share the lock; only a missing or torn current record takes the exclusive
/// recovery path, which may promote the previous record.
pub(super) fn read_locked<T: DeserializeOwned>(
    paths: &RecordPaths,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let current = with_lock_mode(paths, LockMode::Shared, || {
        Ok(read_json(&paths.current).ok().flatten())
    })?;
    match current {
        Some(value) => Ok(Some(value)),
        None => with_lock(paths, || read_recoverable(paths)),
    }
}

pub(super) fn with_lock_mode<T>(
    paths: &RecordPaths,
    mode: LockMode,
    action: impl FnOnce() -> Result<T, LocalCoreStateStoreError>,
) -> Result<T, LocalCoreStateStoreError> {
    std::fs::create_dir_all(&paths.directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&paths.lock)?;
    let deadline = Instant::now() + RECORD_LOCK_DEADLINE;
    loop {
        let locked = match mode {
            LockMode::Shared => FileExt::try_lock_shared(&lock),
            LockMode::Exclusive => lock.try_lock_exclusive(),
        };
        match locked {
            Ok(()) => break,
            Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => {
                if Instant::now() >= deadline {
                    return Err(LocalCoreStateStoreError::LockTimeout(paths.lock.clone()));
                }
                std::thread::sleep(RECORD_LOCK_RETRY);
            }
            Err(error) => return Err(error.into()),
        }
    }
    let result = action();
    FileExt::unlock(&lock)?;
    result
}

pub(super) fn read_recoverable<T: DeserializeOwned>(
    paths: &RecordPaths,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    match read_json(&paths.current) {
        Ok(Some(value)) => Ok(Some(value)),
        Ok(None) => match read_json(&paths.previous) {
            Ok(Some(value)) => {
                promote_previous(paths)?;
                Ok(Some(value))
            }
            other => other,
        },
        Err(current_error) => match read_json(&paths.previous) {
            Ok(Some(value)) => {
                promote_previous(paths)?;
                Ok(Some(value))
            }
            _ => Err(current_error),
        },
    }
}

pub(super) fn promote_previous(paths: &RecordPaths) -> Result<(), LocalCoreStateStoreError> {
    std::fs::copy(&paths.previous, &paths.current)?;
    OpenOptions::new()
        .write(true)
        .open(&paths.current)?
        .sync_all()?;
    sync_directory(&paths.directory)?;
    Ok(())
}

pub(super) fn read_json<T: DeserializeOwned>(
    path: &Path,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(LocalCoreStateStoreError::Json)
}

pub(super) fn read_json_bounded<T: DeserializeOwned>(
    path: &Path,
    maximum_bytes: u64,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if file.metadata()?.len() > maximum_bytes {
        return Err(LocalCoreStateStoreError::ContextTransaction(
            "workspace-context child bucket exceeds its declared bound".to_owned(),
        ));
    }
    let mut bytes = Vec::new();
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    let length = u64::try_from(bytes.len()).map_err(|_| {
        LocalCoreStateStoreError::ContextTransaction(
            "workspace-context child bucket length is not representable".to_owned(),
        )
    })?;
    if length > maximum_bytes {
        return Err(LocalCoreStateStoreError::ContextTransaction(
            "workspace-context child bucket grew beyond its declared bound".to_owned(),
        ));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(LocalCoreStateStoreError::Json)
}

pub(super) fn write_journaled<T: Serialize>(
    paths: &RecordPaths,
    value: &T,
) -> Result<(), LocalCoreStateStoreError> {
    let bytes = serde_json::to_vec(value)?;
    let mut temporary = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&paths.temporary)?;
    temporary.write_all(&bytes)?;
    temporary.sync_all()?;
    drop(temporary);

    #[cfg(windows)]
    {
        // Windows has no safe, portable directory-fsync API. Preserve a
        // fully flushed recovery copy before modifying the current file, then
        // flush the current file itself. A crash can therefore expose either
        // the prior record or the complete replacement, never only a torn
        // current record.
        remove_if_present(&paths.previous)?;
        if paths.current.exists() {
            std::fs::copy(&paths.current, &paths.previous)?;
        } else {
            std::fs::copy(&paths.temporary, &paths.previous)?;
        }
        OpenOptions::new()
            .write(true)
            .open(&paths.previous)?
            .sync_all()?;
        std::fs::copy(&paths.temporary, &paths.current)?;
        OpenOptions::new()
            .write(true)
            .open(&paths.current)?
            .sync_all()?;
        remove_if_present(&paths.temporary)?;
        remove_if_present(&paths.previous)?;
        Ok(())
    }

    #[cfg(not(windows))]
    {
        remove_if_present(&paths.previous)?;
        if paths.current.exists() {
            std::fs::rename(&paths.current, &paths.previous)?;
        }
        if let Err(error) = std::fs::rename(&paths.temporary, &paths.current) {
            if paths.previous.exists() && !paths.current.exists() {
                let _ = std::fs::rename(&paths.previous, &paths.current);
            }
            return Err(error.into());
        }
        sync_directory(&paths.directory)?;
        remove_if_present(&paths.previous)?;
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
