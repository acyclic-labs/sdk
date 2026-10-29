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
    use std::time::Duration;

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
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
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .expect("transaction did not enter")
        .unwrap();
    observer.abort();
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), finished_rx)
        .await
        .expect("admitted transaction was lost")
        .unwrap();
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

#[tokio::test]
async fn owner_excludes_other_processes_until_it_dies() -> Result<(), Box<dyn std::error::Error>> {
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

    let mut child = acyclic_native_runtime::ProcessTree::spawn(
        ownership_child(&root)?
            .env("ACYCLIC_CORE_STATE_CHILD_READY", "1")
            .stdout(std::process::Stdio::piped()),
    )?;
    ownership_child_ready(&mut child).await?;
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
fn measured_shared_admission_charges_its_actual_claim_check_allocations() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner.lock");
    let claim = acyclic_native_runtime::StoreClaim::acquire(&path).unwrap();
    let record_bytes = std::fs::metadata(&path).unwrap().len();
    let file = FileLock::new(std::fs::File::open(&path).unwrap());
    let cancellation = crate::CancellationToken::new();
    let work =
        crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation).unwrap();
    assert!(
        acyclic_native_runtime::StoreClaim::check_metered(&file, &work.runtime_meter())
            .unwrap()
            .claimed
    );
    let receipt = work.receipt();
    assert!(
        receipt.peak_allocation_bytes >= record_bytes && receipt.allocation_operations > 0,
        "{receipt:?}"
    );
    drop(claim);
}

#[tokio::test]
async fn measured_shared_admission_charges_its_claim_check()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("core-state");
    measured_admission_reads(&root);
    // The lock, and the claim check's read of an empty record.
    let free = measured_admission_reads(&root);

    // A dead owner's record stays behind, so each check also probes it.
    let mut child = acyclic_native_runtime::ProcessTree::spawn(
        ownership_child(&root)?
            .env("ACYCLIC_CORE_STATE_CHILD_READY", "1")
            .stdout(std::process::Stdio::piped()),
    )?;
    ownership_child_ready(&mut child).await?;
    child.terminate_descendants()?;
    child.wait(Duration::from_secs(5))?;
    let probed = measured_admission_reads(&root);
    // Count actual physical starts, including record framing and owner probes;
    // an empty record does not allocate or perform those additional reads.
    assert!(
        probed > free,
        "free admission {free} reads, after a dead owner {probed}"
    );
    Ok(())
}

/// Read the child's committed owner notification through the existing native
/// process-event hub. Arm before the read so a concurrent write cannot be lost.
async fn ownership_child_ready(
    child: &mut acyclic_native_runtime::ProcessTree,
) -> std::io::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let marker = b"\0acyclic-owner-ready\0";
        let mut matched = 0;
        let mut bytes = [0; 512];
        loop {
            let ready = child.readiness()?;
            match child.read_stdout(&mut bytes)? {
                Some(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                Some(count) => {
                    for byte in &bytes[..count] {
                        if *byte == marker[matched] {
                            matched += 1;
                            if matched == marker.len() {
                                return Ok(());
                            }
                        } else {
                            matched = usize::from(*byte == marker[0]);
                        }
                    }
                }
                None => ready.await,
            }
        }
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "owner child readiness"))?
}
