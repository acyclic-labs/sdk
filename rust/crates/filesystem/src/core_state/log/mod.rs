use super::*;

#[derive(Debug)]
pub(super) struct CoreLog {
    pub(super) file: File,
    /// Bytes appended to the log.
    pub(super) length: u64,
    /// Bytes of the log known to be durable.
    pub(super) flushed: u64,
    pub(super) epoch: Uuid,
    pub(super) sequence: u64,
    pub(super) residents: Arc<RwLock<Residents>>,
    pub(super) dirty: Vec<RecordKey>,
    /// The log may hold a frame whose outcome is unknown, so it must be
    pub(super) broken: bool,
    pub(super) slots: Option<crate::materializer::NativeAllocation>,
    pub(super) residents_control: Option<crate::materializer::NativeAllocation>,
}

impl CoreLog {
    pub(in crate::core_state) fn residents(&self) -> RwLockWriteGuard<'_, Residents> {
        self.residents
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Reads one record from memory, or from its file on first use.
    pub(in crate::core_state) fn get<T: Logged>(
        &mut self,
        root: &Namespace,
        id: T::Id,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        self.get_bounded(root, id, None)
    }

    /// Reads one record, refusing a file larger than `maximum_bytes`.
    pub(in crate::core_state) fn get_bounded<T: Logged>(
        &mut self,
        root: &Namespace,
        id: T::Id,
        maximum_bytes: Option<u64>,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        let key = T::key(id);
        if maximum_bytes.is_none()
            && matches!(key, RecordKey::LazyWorkspace(_))
            && let Some(allocation) = &root._allocation
        {
            let work = allocation.work();
            return self
                .get_native_value(root, key, work, work.cancellation())?
                .map(|owned| {
                    T::from_value(owned.as_state())
                        .cloned()
                        .ok_or(LocalCoreStateStoreError::Integrity)
                })
                .transpose();
        }
        if let Some(value) = self.residents().get(&key) {
            return match value {
                Some(RecordValue::FlatContext(_)) | Some(RecordValue::FlatChildren(_)) => Ok(
                    serde_json::from_slice(&RecordValue::encode(value.as_ref())?)?,
                ),
                _ => Ok(value.as_ref().and_then(T::from_value).cloned()),
            };
        }
        let paths = key.paths(root)?;
        let value = read_json::<Option<T>>(&paths.current, maximum_bytes)?.flatten();
        self.residents()
            .insert(key, value.clone().map(T::into_value));
        Ok(value)
    }

    /// Keys of resident records that satisfy `select`.
    pub(in crate::core_state) fn resident_keys<K>(
        &self,
        select: impl Fn(RecordKey) -> Option<K>,
    ) -> Vec<K> {
        self.residents
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .filter_map(|key| select(*key))
            .collect()
    }
}

/// Aborts the process at the configured crash point of a crash-atomicity test,
#[cfg(test)]
pub(super) fn crash_point(label: &str, flushed: u64) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static REACHED: AtomicU64 = AtomicU64::new(0);
    let Some(target) = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_POINT") else {
        return;
    };
    let reached = REACHED.fetch_add(1, Ordering::SeqCst) + 1;
    if target.to_str().and_then(|target| target.parse().ok()) == Some(reached) {
        if let Some(report) = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_REPORT") {
            let _ = std::fs::write(report, format!("{label}\n{flushed}"));
        }
        std::process::abort();
    }
}

#[cfg(not(test))]
pub(super) fn crash_point(_label: &str, _flushed: u64) {}

mod checkpoint;
mod commit;
mod replay;
