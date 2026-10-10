//! One shared physical-root acquisition path with truthful producer receipts.

use super::*;
use acyclic_fs::{MeasuredResult, OperationFailure, OperationReceipt, WorkCounters};

fn rejected(error: impl std::fmt::Display, work: WorkCounters) -> OperationFailure<String> {
    OperationFailure::new(error.to_string(), work)
}

fn combine<T, E: std::fmt::Display>(
    prior: &mut WorkCounters,
    result: MeasuredResult<OperationReceipt<T>, E>,
) -> MeasuredResult<T, String> {
    match result {
        Ok(receipt) => {
            *prior = prior.checked_add(receipt.work).map_err(|error| rejected(error, *prior))?;
            Ok(receipt.value)
        }
        Err(failure) => {
            let work = prior.checked_add(*failure.work).map_err(|error| rejected(error, *prior))?;
            Err(rejected(failure.error, work))
        }
    }
}

fn admit(work: &mut WorkCounters, delta: WorkCounters, budget: WorkBudget) -> MeasuredResult<(), String> {
    let next = work.checked_add(delta).map_err(|error| rejected(error, *work))?;
    next.verify_within(&budget).map_err(|error| rejected(error, *work))?;
    *work = next;
    Ok(())
}

/// Logical native-backend requests are metered here. Watcher and source engines
/// meter their own internal work; no claim of universal kernel allocator metering.
pub(crate) async fn acquire(
    registry: &SharedRootRegistry,
    root: &Path,
    admission: SharedRootAdmission,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Arc<SharedPhysicalRoot>>, String> {
    let mut work = WorkCounters::default();
    cancellation.check().map_err(|error| rejected(error, work))?;
    admit(&mut work, WorkCounters {
        backend_read_operations: 1,
        source_path_components: root.components().count() as u64,
        ..WorkCounters::default()
    }, budget)?;
    let canonical = root.canonicalize().map_err(|error| rejected(error, work))?;
    let registration = {
        let mut roots = registry.roots.lock().await;
        Arc::clone(roots.entry(canonical.clone()).or_insert_with(|| {
            Arc::new(AsyncMutex::new(SharedRootRegistration::default()))
        }))
    };
    let mut registration = registration.lock().await;
    cancellation.check().map_err(|error| rejected(error, work))?;
    if let Some(native_identity) = registration.native_identity {
        admit(&mut work, WorkCounters {
            backend_read_operations: 1, source_path_components: canonical.components().count() as u64,
            ..WorkCounters::default()
        }, budget)?;
        let identity_path = canonical.clone();
        let observed = tokio::task::spawn_blocking(move || HostRoot::open(&identity_path).map(|root| root.identity().to_bytes()))
            .await.map_err(|error| rejected(format!("physical root validation worker failed: {error}"), work))?
            .map_err(|error| rejected(error, work))?;
        let live = registration.live.upgrade();
        if observed == native_identity {
            let retained = *registration.reference.as_ref()
                .ok_or_else(|| rejected("shared root registration lost its source reference", work))?
                .lock().map_err(|_| rejected("shared root source reference is poisoned", work))?;
            if admission.reference().is_some_and(|expected| expected.identity != retained.identity) {
                return Err(rejected(format!("physical root {} is already registered with another source identity", canonical.display()), work));
            }
            if let Some(shared) = live {
                // Preserve the existing second identity check across the await.
                cancellation.check().map_err(|error| rejected(error, work))?;
                admit(&mut work, WorkCounters {
                    backend_read_operations: 1, source_path_components: canonical.components().count() as u64,
                    ..WorkCounters::default()
                }, budget)?;
                shared.validate_path_identity(canonical.clone()).await.map_err(|error| rejected(error, work))?;
                return Ok(OperationReceipt { value: shared, work });
            }
        } else {
            if live.is_some() || matches!(admission, SharedRootAdmission::Resume(_)) {
                return Err(rejected(format!("physical root {} changed identity while it was in use", canonical.display()), work));
            }
            *registration = SharedRootRegistration::default();
        }
    }
    let retained = registration.reference.as_ref().map(|reference| {
        *reference.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    });
    let remaining = work.remaining(budget).map_err(|error| rejected(error, work))?;
    let mut watcher = combine(&mut work, NativeWatch::open_with_profile_measured(
        &canonical, native_filesystem_profile(), NativeWatchOptions {
            limits: VolumeLimits::default(), maximum_queued_changes: 65_536,
            recursive: !cfg!(target_os = "linux"),
        }, remaining, cancellation,
    ))?;
    watcher.exclude_subtrees(&reserved_root_paths().map_err(|error| rejected(error, work))?)
        .map_err(|error| rejected(error, work))?;
    let remaining = work.remaining(budget).map_err(|error| rejected(error, work))?;
    combine(&mut work, watcher.accept_lazy_baseline_measured(remaining, cancellation))?;
    let watcher = Arc::new(Mutex::new(watcher));
    let reference = retained.or(admission.reference()).unwrap_or_else(|| SourceReference {
        identity: uuid::Uuid::new_v4().into_bytes(), epoch: 0,
    });
    let remaining = work.remaining(budget).map_err(|error| rejected(error, work))?;
    let source = combine(&mut work, NativeDemandSource::open_with_reference_measured(
        canonical.clone(), native_filesystem_profile(), VolumeLimits::default(), reference,
        Some(Arc::clone(&watcher) as Arc<dyn DemandDirectoryObserver>), remaining, cancellation,
    ).await)?;
    let source = Arc::new(FilteredDemandSource::new(source, reserved_root_paths().map_err(|error| rejected(error, work))?));
    let watched_identity = watcher.lock().map_err(|_| rejected("physical root watcher state is poisoned", work))?
        .root_identity().to_bytes();
    if source.inner().root_identity().to_bytes() != watched_identity {
        return Err(rejected(format!("physical root {} changed identity during admission", canonical.display()), work));
    }
    if matches!(admission, SharedRootAdmission::Resume(_)) { source.inner().invalidate(); }
    let reference = Arc::new(Mutex::new(source.reference()));
    let native_identity = source.inner().root_identity().to_bytes();
    let shared = Arc::new(SharedPhysicalRoot { source, watcher, reference: Arc::clone(&reference) });
    *registration = SharedRootRegistration {
        live: Arc::downgrade(&shared), reference: Some(reference), native_identity: Some(native_identity),
    };
    Ok(OperationReceipt { value: shared, work })
}
pub(crate) fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 4096, authority_records_appended: 64,
        authority_bytes_read: 8 * 1024 * 1024, authority_bytes_written: 1024 * 1024,
        object_probes: 4096, backend_read_operations: 4096, backend_write_operations: 4096,
        durability_operations: 4096, page_reads: 4096, page_writes: 4096,
        object_bytes_read: 8 * 1024 * 1024, object_bytes_written: 8 * 1024 * 1024,
        bytes_hashed: 8 * 1024 * 1024, bytes_copied: 8 * 1024 * 1024,
        bytes_encoded: 8 * 1024 * 1024, source_bytes_read: 8 * 1024 * 1024,
        source_path_components: 4096, source_entries_visited: 4096,
        output_bytes: 8 * 1024 * 1024, items_examined: 4096, items_returned: 4096,
        allocation_operations: 4096, peak_allocation_bytes: 8 * 1024 * 1024,
        materializations: 64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_or_cancelled_root_allowance_rejects_before_native_access() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("never-opened");
        let registry = SharedRootRegistry::default();
        let cancellation = CancellationToken::new();
        let failure = registry.acquire(&missing, SharedRootAdmission::Fresh, WorkBudget::default(), &cancellation)
            .await.err().expect("no root probe admitted");
        assert_eq!(*failure.work, WorkCounters::default());
        assert!(registry.roots.lock().await.is_empty());
        cancellation.cancel();
        let failure = registry.acquire(&missing, SharedRootAdmission::Fresh, budget(), &cancellation)
            .await.err().expect("cancelled root probe");
        assert_eq!(*failure.work, WorkCounters::default());
        assert!(failure.error.contains("cancel"));
        assert!(!missing.exists());
    }

    #[tokio::test]
    async fn later_root_admission_failure_preserves_earlier_native_probe_work() {
        let directory = tempfile::tempdir().unwrap();
        let registry = SharedRootRegistry::default();
        let mut allowance = budget();
        allowance.backend_read_operations = 1;
        let failure = registry.acquire(directory.path(), SharedRootAdmission::Fresh, allowance, &CancellationToken::new())
            .await.err().expect("watcher cannot reset the exhausted shared allowance");
        assert_eq!(failure.work.backend_read_operations, 1);
        assert!(failure.work.source_path_components > 0);
        assert_eq!(registry.live_roots().await, 0);
    }
}
