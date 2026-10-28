//! Acknowledgement barriers shared by concurrent transactions of one owner.
use super::*;

pub(super) fn shared_control<T>(
    work: &crate::NativeMaterializationWork,
) -> Result<crate::NativeAllocation, LocalCoreStateStoreError> {
    let bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<T>())
        .map_err(|_| crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
    Ok(work.claim(bytes as u64, 1, crate::WorkCounters::UNCHARGED)?)
}

impl Namespace {
    /// Reserve the possible physical barrier before appending any owned frame.
    pub(super) fn reserve_barrier(&self) -> Result<(), LocalCoreStateStoreError> {
        if self.durability == Durability::Flushed
            && matches!(self.admission, Admission::Owned(_))
            && let Some(allocation) = &self._allocation
        {
            let mut slot = self.barrier.lock().unwrap_or_else(PoisonError::into_inner);
            if slot.is_none() {
                *slot = Some(allocation.work().reserve_work(crate::WorkCounters {
                    durability_operations: 1,
                    ..Default::default()
                })?);
            }
        }
        Ok(())
    }
}

struct UnwindFence<'a>(&'a Owner);
impl Drop for UnwindFence<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.failed.store(true, Ordering::Release);
        }
    }
}

fn usable(owner: &Owner) -> Result<(), LocalCoreStateStoreError> {
    if owner.failed.load(Ordering::Acquire) {
        Err(LocalCoreStateStoreError::DurabilityPoisoned)
    } else {
        Ok(())
    }
}

fn needs_checkpoint(owner: &Owner, log: &CoreLog) -> bool {
    log.sequence >= OWNED_CHECKPOINT
        || owner
            .cache_limit
            .is_some_and(|limit| log.residents().len() >= limit)
}

pub(super) fn with_owned<T>(
    root: &Namespace,
    owner: &Owner,
    operation: impl FnOnce(&mut CoreLog) -> Result<T, LocalCoreStateStoreError>,
) -> Result<T, LocalCoreStateStoreError> {
    let _unwind = UnwindFence(owner);
    let mut slot = owner.log();
    usable(owner)?;
    if slot.is_none() {
        *slot = Some(CoreLog::open(root, Arc::clone(&owner.residents))?);
    }
    if needs_checkpoint(
        owner,
        slot.as_ref().ok_or(LocalCoreStateStoreError::Integrity)?,
    ) {
        // Always acquire barrier -> append, including when the checkpoint was
        // discovered under append. A physical flush holds no append mutex.
        drop(slot);
        let _barrier = owner
            .flush_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        slot = owner.log();
        usable(owner)?;
        let log = slot.as_mut().ok_or(LocalCoreStateStoreError::Integrity)?;
        if needs_checkpoint(owner, log) {
            let checkpoint = log.checkpoint(root);
            if log.broken {
                owner.failed.store(true, Ordering::Release);
            }
            checkpoint?;
            owner.pending.store(false, Ordering::Release);
            if owner.cache_limit.is_some() {
                *log.residents() = Residents::default();
            }
        }
    }
    let log = slot.as_mut().ok_or(LocalCoreStateStoreError::Integrity)?;
    let result = operation(log);
    if log.broken {
        owner.failed.store(true, Ordering::Release);
    }
    let target = (log.epoch, log.length);
    drop(slot);
    // Explicit deferral retains its documented scope. Ordinary operations,
    // including reads of pending after-images, cannot acknowledge early.
    if root.durability == Durability::Flushed {
        let barrier = flush_through(root, owner, target);
        // Preserve an operation's original typed failure after settling any
        // earlier successfully appended frames from that operation.
        match result {
            Ok(value) => barrier.map(|()| value),
            Err(error) => {
                drop(barrier);
                Err(error)
            }
        }
    } else {
        result
    }
}

fn flush_through(
    root: &Namespace,
    owner: &Owner,
    target: (Uuid, u64),
) -> Result<(), LocalCoreStateStoreError> {
    let _barrier = owner
        .flush_gate
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let slot = owner.log();
    usable(owner)?;
    let log = slot.as_ref().ok_or(LocalCoreStateStoreError::Integrity)?;
    if target.0 != log.epoch || target.1 <= log.flushed {
        return Ok(());
    }
    // A read that joins pending deferred writes needs its own admitted barrier;
    // a writer reserved this credit before it appended anything.
    root.reserve_barrier()?;
    let credit = root
        .barrier
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    let file = log.lease(
        root._allocation
            .as_ref()
            .map(|allocation| allocation.work()),
    )?;
    let covered = (log.epoch, log.length);
    drop(slot);
    let synced = file.flush(acyclic_native_runtime::flush_file_data);
    let recorded = credit
        .as_ref()
        .map(|credit| {
            credit.record_completed(crate::WorkCounters {
                durability_operations: 1,
                ..Default::default()
            })
        })
        .transpose();
    // Both success and failure are observed on the original ledger, even if
    // cancellation arrives while the kernel owns the barrier.
    if let Err(error) = synced {
        owner.failed.store(true, Ordering::Release);
        return Err(error.into());
    }
    let mut slot = owner.log();
    let log = slot.as_mut().ok_or(LocalCoreStateStoreError::Integrity)?;
    if covered.0 != log.epoch {
        owner.failed.store(true, Ordering::Release);
        return Err(LocalCoreStateStoreError::Integrity);
    }
    log.flushed = log.flushed.max(covered.1);
    owner
        .pending
        .store(log.flushed < log.length, Ordering::Release);
    crash_point("frame flushed", log.flushed);
    recorded?;
    usable(owner)
}

#[cfg(test)]
mod tests;
