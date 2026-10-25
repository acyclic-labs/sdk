//! Read existing durable state without publishing replay or recovery repairs.
use super::*;

pub(super) fn is_missing(error: &LocalCoreStateStoreError) -> bool {
    match error {
        LocalCoreStateStoreError::Io(error)
        | LocalCoreStateStoreError::LazyWork(crate::LazyWorkspaceError::NativeWork(
            crate::NativeTreeMaterializationError::Io(error),
        )) => error.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

/// The publication lock must already exist. Its absence linearizes this read
/// before the first writer creates the record, without creating a lock file.
pub(super) fn with_record<T>(
    paths: &mut RecordPaths,
    read: impl FnOnce(&RecordPaths) -> Result<Option<T>, LocalCoreStateStoreError>,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let lock = match records::acquire_existing_lock(paths) {
        Ok(lock) => lock,
        Err(error) if is_missing(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let result = read(paths);
    finish_read(lock, result)
}

fn finish_read<T>(
    mut lock: OwnerLock,
    result: Result<Option<T>, LocalCoreStateStoreError>,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let unlocked = lock.unlock();
    match result {
        Err(error) => Err(error),
        Ok(value) => unlocked.map(|()| value),
    }
}

/// An owned writer can leave a WAL without ever creating its record lock.
/// Shared namespace admission excludes owned writers. Shared writers create
/// the record lock before opening or changing the WAL, and never remove it.
/// Therefore an absent lock both before and after replay validates that cut.
/// If a first shared writer races replay, discard even a failed speculative
/// parse and read again under its publication lock.
fn with_wal<T>(
    paths: &mut RecordPaths,
    mut read: impl FnMut() -> Result<Option<T>, LocalCoreStateStoreError>,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    match records::acquire_existing_lock(paths) {
        Ok(lock) => return finish_read(lock, read()),
        Err(error) if is_missing(&error) => {}
        Err(error) => return Err(error),
    }
    let speculative = read();
    match records::acquire_existing_lock(paths) {
        Ok(lock) => {
            drop(speculative);
            finish_read(lock, read())
        }
        Err(error) if is_missing(&error) => speculative,
        Err(error) => Err(error),
    }
}

impl LocalCoreStateStore {
    pub(super) async fn load_inspected_logged<T: Logged>(
        &self,
        id: T::Id,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        self.read_transaction(move |root| {
            let read = |maximum| {
                let control = root
                    ._allocation
                    .as_ref()
                    .map(|allocation| group::shared_control::<RwLock<Residents>>(allocation.work()))
                    .transpose()?;
                let mut log = match CoreLog::open_prefix(&root, Arc::default(), maximum) {
                    Ok(log) => log,
                    Err(error) if is_missing(&error) => {
                        let paths = T::key(id).paths(&root)?;
                        return records::read_recoverable_mode::<Option<T>>(&paths, false)
                            .map(|value| value.and_then(|(value, _)| value));
                    }
                    Err(error) => return Err(error),
                };
                log.residents_control = control;
                log.get::<T>(&root, id, None, None)
                    .map(|value| value.map(|(value, _)| value))
            };
            match &root.admission {
                Admission::Owned(owner) => {
                    let mut slot = owner.log();
                    if owner.failed.load(Ordering::Acquire) {
                        return Err(LocalCoreStateStoreError::DurabilityPoisoned);
                    }
                    match slot.as_mut() {
                        Some(log) if !owner.pending.load(Ordering::Acquire) => log
                            .get::<T>(&root, id, None, None)
                            .map(|value| value.map(|(value, _)| value)),
                        Some(log) => read(Some(log.flushed)),
                        None => read(None),
                    }
                }
                Admission::Shared { .. } => {
                    let mut paths = root.record(CORE_LOG_FAMILY, &[0; 16])?;
                    with_wal(&mut paths, || read(None))
                }
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests;
