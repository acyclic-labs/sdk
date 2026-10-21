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

    /// Reads one record, refusing a file larger than `maximum_bytes`.
    pub(in crate::core_state) fn get<T: Logged>(
        &mut self,
        root: &Namespace,
        id: T::Id,
        maximum_bytes: Option<u64>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<Option<(T, Option<crate::NativeAllocation>)>, LocalCoreStateStoreError> {
        let key = T::key(id);
        let work = root
            ._allocation
            .as_ref()
            .map(|allocation| allocation.work());
        if let Some(work) = work {
            self.adopt_native(work)?;
        }
        let copy = |value: &RecordValue| {
            let (value, allocation) = match work {
                Some(work) => {
                    let (value, allocation) = native_records::clone_native_record_value(
                        value,
                        work,
                        cancellation.unwrap_or(work.cancellation()),
                    )?;
                    (value, Some(allocation))
                }
                None => (value.clone(), None),
            };
            let value = T::from_value(value);
            if work.is_some() && value.is_none() {
                return Err(LocalCoreStateStoreError::Integrity);
            }
            Ok::<_, LocalCoreStateStoreError>(value.map(|value| (value, allocation)))
        };
        {
            let residents = self.residents();
            if let Some(index) = residents.position(key, work)? {
                return residents
                    .0
                    .get(index)
                    .ok_or(LocalCoreStateStoreError::Integrity)?
                    .value
                    .as_ref()
                    .map(copy)
                    .transpose()
                    .map(Option::flatten);
            }
        }
        let paths = key.paths(root)?;
        if let Some(work) = work {
            native_records::check(work, cancellation.unwrap_or(work.cancellation()))?;
        }
        let value = match work {
            Some(work) => read_file::<Option<T>>(&paths.current, work, maximum_bytes)?
                .and_then(|(value, allocation)| value.map(|value| (value, Some(allocation)))),
            None => read_json::<Option<T>>(&paths.current, maximum_bytes)?
                .flatten()
                .map(|value| (value, None)),
        };
        let (value, allocation) = match value {
            Some((value, allocation)) => (Some(value.into_value()), allocation),
            None => (
                None,
                work.map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
                    .transpose()?,
            ),
        };
        let mut record = Resident {
            key,
            value,
            allocation,
        };
        if let (Some(value), Some(allocation)) = (&record.value, &mut record.allocation) {
            allocation.resize(native_records::native_record_owned_bytes(
                value,
                allocation.work(),
            )?)?;
        }
        let output = record.value.as_ref().map(copy).transpose()?.flatten();
        let mut residents = self.residents();
        if let Some(work) = work {
            let needed = residents.0.len() + 1;
            let Residents(values, slots) = &mut *residents;
            reserve_slots(values, needed, work, slots)?;
        }
        residents.0.push(record);
        Ok(output)
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

#[cfg(test)]
static REACHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(super) fn crash_point_count() -> u64 {
    REACHED.load(std::sync::atomic::Ordering::SeqCst)
}

/// Stops at a physical WAL cut until the supervising process hard-kills the child.
#[cfg(test)]
pub(super) fn crash_point(label: &str, flushed: u64) {
    use std::sync::atomic::Ordering;
    let Some(target) = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_POINT") else {
        return;
    };
    let reached = REACHED.fetch_add(1, Ordering::SeqCst) + 1;
    if target.to_str().and_then(|target| target.parse().ok()) == Some(reached) {
        let report = std::path::PathBuf::from(
            std::env::var_os("ACYCLIC_CORE_STATE_CRASH_REPORT")
                .expect("crash child report path is absent"),
        );
        let pending = report.with_extension("pending");
        std::fs::write(&pending, format!("{label}\n{flushed}")).unwrap();
        std::fs::rename(pending, report).unwrap();
        // Keep every log/resident guard and file alive until the parent's
        // process-tree kill. An abort can invoke platform crash reporting;
        // neither that latency nor graceful shutdown is part of this cut.
        loop {
            std::thread::park();
        }
    }
}

#[cfg(not(test))]
pub(super) fn crash_point(_label: &str, _flushed: u64) {}

mod checkpoint;
mod commit;
mod replay;
