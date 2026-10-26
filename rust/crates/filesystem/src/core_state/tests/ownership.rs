use super::*;

#[test]
fn local_transactions_work_without_a_tokio_runtime() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let result = futures::executor::block_on(WorkspaceLineageStore::load(&store, workspace()));
    assert!(result.unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_local_transaction_does_not_block_executor_or_stop_on_observer_drop() {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let observer = tokio::spawn(async move {
        let directory = tempfile::tempdir().unwrap();
        LocalCoreStateStore::new(directory.path())
            .transaction(move |_| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                finished_tx.send(()).unwrap();
                Ok(())
            })
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while entered_rx.try_recv().is_err() {
        assert!(Instant::now() < deadline, "transaction did not enter");
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    observer.abort();
    release_tx.send(()).unwrap();
    while finished_rx.try_recv().is_err() {
        assert!(Instant::now() < deadline, "admitted transaction was lost");
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn owner_excludes_every_other_store_in_its_process() {
    let directory = tempfile::tempdir().unwrap();
    let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    let clone = owner.clone();
    let shared = LocalCoreStateStore::new(directory.path());
    assert!(is_ownership_conflict(&LocalCoreStateStore::open_owned(
        directory.path()
    )));
    assert!(is_ownership_conflict(
        &WorkspaceLineageStore::load(&shared, workspace()).await
    ));
    assert!(matches!(
        shared
            .load_lazy_workspace(
                workspace(),
                &mut unbounded_accounting(&crate::CancellationToken::new())
            )
            .await,
        Err(crate::LazyWorkspaceError::NativeRecordOwnershipConflict(..))
    ));

    drop(owner);
    assert!(
        is_ownership_conflict(&WorkspaceLineageStore::load(&shared, workspace()).await),
        "a clone keeps the namespace owned"
    );
    drop(clone);
    assert!(
        WorkspaceLineageStore::load(&shared, workspace())
            .await
            .unwrap()
            .is_none()
    );

    let admitted = shared.namespace().unwrap();
    let concurrent = LocalCoreStateStore::new(directory.path())
        .namespace()
        .unwrap();
    assert!(is_ownership_conflict(&LocalCoreStateStore::open_owned(
        directory.path()
    )));
    drop(admitted);
    drop(concurrent);
    LocalCoreStateStore::open_owned(directory.path()).unwrap();
}

#[test]
fn owner_excludes_other_processes_until_it_dies() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("core-state");
    let owner = LocalCoreStateStore::open_owned(&root)?;
    assert!(
        acyclic_native_runtime::process_status(
            &mut ownership_child(&root)?,
            std::time::Duration::from_secs(120)
        )?
        .success(),
        "another process entered an owned namespace"
    );
    drop(owner);

    let ready = directory.path().join("ready");
    let mut child = acyclic_native_runtime::ProcessTree::spawn(
        ownership_child(&root)?.env("ACYCLIC_CORE_STATE_CHILD_READY", &ready),
    )?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.is_file() {
        if let Some(status) = child.try_wait(None)? {
            return Err(format!("owner child exited before readiness: {status}").into());
        }
        if Instant::now() >= deadline {
            return Err("owner child readiness timed out".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(is_ownership_conflict(
        &LocalCoreStateStore::new(&root).namespace()
    ));
    assert!(is_ownership_conflict(&LocalCoreStateStore::open_owned(
        &root
    )));
    child.terminate_descendants()?;
    child.wait(Duration::from_secs(5))?;
    LocalCoreStateStore::open_owned(&root)?;
    Ok(())
}

/// The work one measured shared admission of `root` charges.
#[track_caller]
fn measured_admission(root: &Path) -> crate::WorkCounters {
    let cancellation = crate::CancellationToken::new();
    let work =
        crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation).unwrap();
    let store = LocalCoreStateStore::new(root).materialization(work.clone());
    drop(store.namespace().unwrap());
    work.receipt()
}

/// Backend reads one measured shared admission of `root` charges.
#[track_caller]
fn measured_admission_reads(root: &Path) -> u64 {
    measured_admission(root).backend_read_operations
}

#[test]
fn measured_shared_admission_charges_its_claim_check_allocations() {
    let directory = tempfile::tempdir().unwrap();
    let work = measured_admission(&directory.path().join("core-state"));
    assert!(
        work.peak_allocation_bytes >= acyclic_native_runtime::StoreClaim::CHECK_ALLOCATION_BYTES
            && work.allocation_operations >= acyclic_native_runtime::StoreClaim::CHECK_ALLOCATIONS,
        "{work:?}"
    );
}

#[test]
fn measured_shared_admission_charges_its_claim_check() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("core-state");
    measured_admission_reads(&root);
    // The lock, and the claim check's read of an empty record.
    let free = measured_admission_reads(&root);

    // A dead owner's record stays behind, so each check also probes it.
    let ready = directory.path().join("ready");
    let mut child = acyclic_native_runtime::ProcessTree::spawn(
        ownership_child(&root)?.env("ACYCLIC_CORE_STATE_CHILD_READY", &ready),
    )?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.is_file() {
        if let Some(status) = child.try_wait(None)? {
            return Err(format!("owner child exited before readiness: {status}").into());
        }
        if Instant::now() >= deadline {
            return Err("owner child readiness timed out".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.terminate_descendants()?;
    child.wait(Duration::from_secs(5))?;
    let probed = measured_admission_reads(&root);
    // One probe while the owner's process lock remains, three once another
    // process's sweep has removed it.
    assert!(
        probed > free && probed - free < acyclic_native_runtime::StoreClaim::CHECK_READ_OPERATIONS,
        "free admission {free} reads, after a dead owner {probed}"
    );
    Ok(())
}
