//! Original-funded admission to the existing local CoreLog transaction.

use super::*;
use crate::materializer::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError};

/// The lexical storage is destroyed before its original allocation custody.
struct NativeCorePath {
    path: PathBuf,
    allocation: NativeAllocation,
}

fn overflow() -> LocalCoreStateStoreError {
    NativeTreeMaterializationError::Work(crate::WorkError::Overflow).into()
}

fn check(work: &NativeMaterializationWork, cancellation: &crate::CancellationToken) -> Result<(), LocalCoreStateStoreError> {
    cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
    work.cancellation().check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
    work.admit(crate::WorkCounters::UNCHARGED)?;
    Ok(())
}

pub(super) fn checked_native_packet<T>(
    packet: NativeOwnedCoreState<T>,
    original: &NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    owned_bytes: impl FnOnce(&T, &NativeMaterializationWork) -> Result<u64, LocalCoreStateStoreError>,
) -> Result<NativeOwnedCoreState<T>, LocalCoreStateStoreError> {
    check(original, cancellation)?;
    // Keep the tuple intact on denial: physical DTO first, original guard last.
    let parts = packet.into_parts();
    if !parts.1.matches_work(original) {
        return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
    }
    if parts.1.bytes() != owned_bytes(&parts.0, original)? {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    Ok(NativeOwnedCoreState::from_parts(parts.0, parts.1))
}

fn copy_path(root: &Path, leaf: Option<&Path>, work: &NativeMaterializationWork) -> Result<NativeCorePath, LocalCoreStateStoreError> {
    let copied = root.as_os_str().len().checked_add(leaf.map_or(0, |path| path.as_os_str().len())).ok_or_else(overflow)?;
    let capacity = copied.checked_add(usize::from(leaf.is_some())).ok_or_else(overflow)?;
    let copy = crate::WorkCounters { bytes_copied: u64::try_from(copied).map_err(|_| overflow())?, ..crate::WorkCounters::UNCHARGED };
    work.admit(copy)?;
    let allocation = work.claim_allocation(u64::try_from(capacity).map_err(|_| overflow())?, u64::from(capacity != 0))?;
    let mut owned = NativeCorePath { path: PathBuf::new(), allocation };
    owned.path.try_reserve_exact(capacity).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    // Keep custody of the real allocator capacity before any late refusal.
    owned.allocation.resize_retained(u64::try_from(owned.path.capacity()).map_err(|_| overflow())?)?;
    owned.path.push(root);
    if let Some(leaf) = leaf { owned.path.push(leaf); }
    work.spent(copy)?;
    Ok(owned)
}

struct NativeCoreLogFrame<F> {
    operation: Option<F>,
    root: Option<NativeCorePath>,
    ownership: Ownership,
    deferred: bool,
    work: NativeMaterializationWork,
    cancellation: crate::CancellationToken,
}

/// Box backing is physically freed before the guard which admitted that Box.
/// The named frame stays with the accepted worker, not its waiting future.
struct NativeCoreLogSource<F> {
    frame: Box<NativeCoreLogFrame<F>>,
    _allocation: NativeAllocation,
}

struct NativeCoreNamespace {
    namespace: Namespace,
    _allocation: NativeAllocation,
}

impl<F> NativeCoreLogSource<F> {
    fn run<T>(mut self) -> Result<T, LocalCoreStateStoreError>
    where F: FnOnce(&Namespace, &mut CoreLog, &NativeMaterializationWork, &crate::CancellationToken) -> Result<T, LocalCoreStateStoreError> {
        check(&self.frame.work, &self.frame.cancellation)?;
        let root = self.frame.root.take().ok_or(LocalCoreStateStoreError::Integrity)?;
        let admission = match &self.frame.ownership {
            Ownership::Shared => {
                crate::native_host::create_directories_materialization_measured(&root.path, &self.frame.work)?;
                let lock_path = copy_path(&root.path, Some(Path::new(OWNER_LOCK)), &self.frame.work)?;
                let lock = materialization_open_options(&lock_path.path, &crate::native_host::NativeOpenOptions::ReadWriteCreate, &self.frame.work)?;
                let cleanup = self.frame.work.reserve_cleanup_write()?;
                let read = crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED };
                self.frame.work.admit(read)?;
                check(&self.frame.work, &self.frame.cancellation)?;
                let acquired = FileExt::try_lock_shared(&lock);
                // Acquire the cleanup owner before reporting completed work.
                let held = acquired.as_ref().is_ok();
                let owner_lock = held.then(|| OwnerLock(lock, Some(cleanup)));
                self.frame.work.spent(read)?;
                match acquired {
                    Ok(()) => Admission::Shared { _lock: owner_lock.ok_or(LocalCoreStateStoreError::Integrity)? },
                    Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => {
                        return Err(LocalCoreStateStoreError::LazyWork(crate::LazyWorkspaceError::NativeRecordOwnershipConflict(root.path, Some(root.allocation))));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Ownership::Owned(owner) => Admission::Owned(Arc::clone(owner)),
        };
        let namespace = NativeCoreNamespace {
            namespace: Namespace {
                root: root.path,
                durability: if matches!(admission, Admission::Owned(_)) && self.frame.deferred { Durability::Deferred } else { Durability::Flushed },
                admission,
            },
            _allocation: root.allocation,
        };
        check(&self.frame.work, &self.frame.cancellation)?;
        let operation = self.frame.operation.take().ok_or(LocalCoreStateStoreError::Integrity)?;
        namespace.namespace.with_measured_log(&self.frame.work, |log| {
            operation(&namespace.namespace, log, &self.frame.work, &self.frame.cancellation)
        })
    }
}

impl LocalCoreStateStore {
    pub(super) async fn native_logged_transaction<T: Send + 'static, F>(
        &self,
        original: &NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        operation: F,
    ) -> Result<T, LocalCoreStateStoreError>
    where F: FnOnce(&Namespace, &mut CoreLog, &NativeMaterializationWork, &crate::CancellationToken) -> Result<T, LocalCoreStateStoreError> + Send + 'static {
        check(original, cancellation)?;
        let root = copy_path(&self.root, None, original)?;
        let allocation = original.claim_allocation(u64::try_from(std::mem::size_of::<NativeCoreLogFrame<F>>()).map_err(|_| overflow())?, 1)?;
        let source = NativeCoreLogSource {
            frame: Box::new(NativeCoreLogFrame {
                operation: Some(operation), root: Some(root), ownership: self.ownership.clone(),
                deferred: self.deferral.as_ref().is_some_and(|deferral| deferral.load(Ordering::Acquire)),
                work: original.clone(), cancellation: cancellation.clone(),
            }),
            _allocation: allocation,
        };
        // This getter only clones the SAME original/resource/cancellation owner.
        // The runtime funds its actual queue/completion/closure, not a new budget.
        let task = acyclic_native_runtime::run_blocking_io_measured(move || source.run(), original.runtime_meter_owner())
            .map_err(NativeTreeMaterializationError::from_runtime_error)?;
        task.await.map_err(NativeTreeMaterializationError::from_runtime_error)?
    }
}
