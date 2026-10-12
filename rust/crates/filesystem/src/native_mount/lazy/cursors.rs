use super::*;

pub(in crate::native_mount::lazy) struct StampedCursor<T> {
    pub(in crate::native_mount::lazy) generation: (u64, u64),
    pub(in crate::native_mount::lazy) cursor: T,
    pub(in crate::native_mount::lazy) native_cleanup: Option<crate::materializer::NativeWorkReservation>,
    pub(in crate::native_mount::lazy) _native_cleanup_allocation: Option<crate::NativeAllocation>,
}

pub(in crate::native_mount::lazy) struct CursorTable<T> {
    pub(in crate::native_mount::lazy) entries: Mutex<BTreeMap<u64, StampedCursor<T>>>,
    pub(in crate::native_mount::lazy) next: AtomicU64,
    pub(in crate::native_mount::lazy) retiring: AtomicU64,
    pub(in crate::native_mount::lazy) maximum: usize,
}

impl<T> CursorTable<T> {
    pub(in crate::native_mount::lazy) fn new(maximum: usize) -> Self {
        Self { entries: Mutex::new(BTreeMap::new()), next: AtomicU64::new(1),
            retiring: AtomicU64::new(0), maximum }
    }

    pub(in crate::native_mount::lazy) fn physically_empty(&self) -> Result<bool, MountSourceError> {
        let entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(entries.is_empty() && self.retiring.load(Ordering::Acquire) == 0)
    }

    pub(in crate::native_mount::lazy) fn next_token(&self) -> u64 {
        let mut token = self.next.fetch_add(1, Ordering::Relaxed);
        if token == 0 { token = self.next.fetch_add(1, Ordering::Relaxed); }
        token
    }

    pub(in crate::native_mount::lazy) fn take(
        &self,
        generation: (u64, u64),
        cursor: Option<&[u8]>,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<T>, MountSourceError> {
        let Some(cursor) = cursor else { return Ok(None); };
        if let Some(original) = carrier.original_native_work() {
            original.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            let bytes: [u8; 8] = cursor.try_into().map_err(|_| MountSourceError::Stale)?;
            original.charge(crate::WorkCounters {
                bytes_copied: 8, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            let stamped = self.entries.lock().map_err(|_| MountSourceError::Stale)?
                .remove(&u64::from_le_bytes(bytes)).ok_or(MountSourceError::Stale)?;
            if stamped.generation != generation { return Err(MountSourceError::Stale); }
            return Ok(Some(stamped.cursor));
        }
        let stamped = carrier.with_work(|work| {
            mount_items(work, 1)?;
            let delta = crate::WorkCounters { bytes_copied: 8, ..crate::WorkCounters::UNCHARGED };
            work.work.admit(&delta, &work.budget).map_err(MountSourceError::Work)?;
            let bytes: [u8; 8] = cursor.try_into().map_err(|_| MountSourceError::Stale)?;
            work.record(delta).map_err(mount_demand_error)?;
            self.entries.lock().map_err(|_| MountSourceError::Stale)?
                .remove(&u64::from_le_bytes(bytes)).ok_or(MountSourceError::Stale)
        })?;
        if stamped.generation != generation {
            drop(stamped);
            carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED)).map_err(mount_demand_error)?;
            return Err(MountSourceError::Stale);
        }
        Ok(Some(stamped.cursor))
    }

    pub(in crate::native_mount::lazy) fn remember(
        &self,
        generation: (u64, u64),
        cursor: T,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountOwnedBuffer<Vec<u8>>, MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            use crate::native_mount::adapter::CaptureInputScratch;
            let native_cleanup = original.reserve_work(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
            let native_cleanup_allocation = original.claim(
                (std::mem::size_of::<Option<crate::materializer::NativeWorkReservation>>()
                    + std::mem::size_of::<Option<crate::NativeAllocation>>()) as u64, 0, crate::WorkCounters::UNCHARGED,
            ).map_err(MountSourceError::NativeWork)?;
            let mut scratch = crate::native_mount::adapter::NativeCaptureScratchAllocation::new(&original)?;
            let delta = crate::WorkCounters {
                bytes_copied: 8, ..crate::WorkCounters::UNCHARGED
            };
            scratch.admit(delta)?;
            let mut output = acyclic_native_runtime::NativeByteReservation::new(
                original.runtime_meter(), acyclic_native_runtime::NativeOperationKind::Write, 8,
            ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?;
            let mut token_bytes = scratch.vec::<u8>(8)?;
            loop {
                let evicted = {
                    let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                    if entries.len() < self.maximum || entries.is_empty() { None } else {
                        original.charge(crate::WorkCounters {
                            items_examined: 1, ..crate::WorkCounters::UNCHARGED
                        }).map_err(MountSourceError::NativeWork)?;
                        entries.pop_first()
                    }
                };
                let Some(evicted) = evicted else { break; };
                drop(evicted);
            }
            let replaced = {
                let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                original.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(MountSourceError::NativeWork)?;
                let token = self.next_token();
                let replaced = entries.insert(token, StampedCursor {
                    generation, cursor, native_cleanup: Some(native_cleanup),
                    _native_cleanup_allocation: Some(native_cleanup_allocation),
                });
                token_bytes.extend_from_slice(&token.to_le_bytes());
                replaced
            };
            drop(replaced);
            let recorded_copy = scratch.record(delta);
            let recorded_output = output.record(8)
                .map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)));
            recorded_copy.and(recorded_output)?;
            return Ok(MountOwnedBuffer {
                value: token_bytes, legacy_bytes: 0, allocation: Some(scratch.retain()),
            });
        }
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let mut cursor = Some(cursor);
        let delta = crate::WorkCounters { bytes_copied: 8, output_bytes: 8, ..crate::WorkCounters::UNCHARGED };
        let mut token_bytes = scratch.carrier.with_work(|work| {
            work.work.admit(&delta, &work.budget).map_err(MountSourceError::Work)?;
            mount_vec::<u8>(8, work)
        })?;
        scratch.track(token_bytes.capacity() as u64)?;
        loop {
            let evicted = scratch.carrier.with_work(|work| {
                let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                if entries.len() < self.maximum || entries.is_empty() { return Ok(None); }
                mount_items(work, 1)?;
                Ok::<_, MountSourceError>(entries.pop_first())
            })?;
            let Some(evicted) = evicted else { break; };
            drop(evicted);
            scratch.carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED)).map_err(mount_demand_error)?;
        }
        let replaced = scratch.carrier.with_work(|work| {
            let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
            mount_items(work, 1)?;
            work.work.admit(&delta, &work.budget).map_err(MountSourceError::Work)?;
            let token = self.next_token();
            let replaced = entries.insert(token, StampedCursor {
                generation, cursor: cursor.take().expect("the new cursor is inserted once"),
                native_cleanup: None,
                _native_cleanup_allocation: None,
            });
            token_bytes.extend_from_slice(&token.to_le_bytes());
            work.record(delta).map_err(mount_demand_error)?;
            Ok::<_, MountSourceError>(replaced)
        })?;
        drop(replaced);
        scratch.carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED)).map_err(mount_demand_error)?;
        scratch.keep(token_bytes.capacity() as u64)?;
        Ok(MountOwnedBuffer { value: token_bytes, legacy_bytes: 8, allocation: None })
    }

    pub(in crate::native_mount::lazy) fn clear_with_cleanup(
        &self, wait: Option<&crate::CancellationToken>,
    ) -> Result<(), MountSourceError> {
        loop {
            let removed = {
                let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                let Some((_, first)) = entries.first_key_value() else {
                    return if self.retiring.load(Ordering::Acquire) == 0 { Ok(()) }
                        else { Err(MountSourceError::Stale) };
                };
                if let Some(wait) = wait { wait.check().map_err(|_| MountSourceError::Cancelled)?; }
                if first.native_cleanup.is_none() {
                    return Err(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ));
                }
                self.retiring.fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_add(1))
                    .map_err(|_| MountSourceError::Work(crate::WorkError::Overflow))?;
                entries.pop_first().expect("accepted original cursor is present").1
            };
            let StampedCursor {
                cursor, mut native_cleanup, _native_cleanup_allocation, ..
            } = removed;
            drop(cursor);
            let completed = native_cleanup.as_mut().expect("birth owns its terminal visit")
                .record_completed(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork);
            drop(native_cleanup);
            drop(_native_cleanup_allocation);
            self.retiring.fetch_sub(1, Ordering::AcqRel);
            completed?;
        }
    }

    pub(in crate::native_mount::lazy) fn retire(
        &self,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            return self.clear_with_cleanup(Some(original.cancellation()));
        }
        loop {
            let removed = carrier.with_work(|work| {
                let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                if entries.is_empty() { return Ok(None); }
                mount_items(work, 1)?;
                Ok::<_, MountSourceError>(entries.pop_first())
            })?;
            let Some(removed) = removed else { return Ok(()); };
            drop(removed);
            carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED)).map_err(mount_demand_error)?;
        }
    }

    pub(in crate::native_mount::lazy) fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() { entries.clear(); }
    }
}
