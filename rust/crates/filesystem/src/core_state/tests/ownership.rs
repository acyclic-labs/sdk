use super::*;

#[test]
fn local_transactions_work_without_a_tokio_runtime() {
    let directory = tempfile::tempdir().expect("local state directory");
    let store = LocalCoreStateStore::new(directory.path());
    let result = futures::executor::block_on(WorkspaceLineageStore::load(&store, workspace()));
    assert!(result.expect("runtime-independent transaction").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_local_transaction_does_not_block_executor_or_stop_on_observer_drop() {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let observer = tokio::spawn(async move {
        let directory = tempfile::tempdir().expect("local state directory");
        LocalCoreStateStore::new(directory.path())
            .transaction(move |_| {
                entered_tx.send(()).expect("test observer is alive");
                release_rx.recv().expect("test permits completion");
                finished_tx
                    .send(())
                    .expect("test completion observer is alive");
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
    release_tx.send(()).expect("transaction remains admitted");
    while finished_rx.try_recv().is_err() {
        assert!(Instant::now() < deadline, "admitted transaction was lost");
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
    async fn owner_excludes_every_other_store_in_its_process() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        let clone = owner.clone();
        let shared = LocalCoreStateStore::new(directory.path());
        assert!(is_ownership_conflict(&LocalCoreStateStore::open_owned(
            directory.path()
        )));
        assert!(is_ownership_conflict(
            &WorkspaceLineageStore::load(&shared, workspace()).await
        ));
        assert!(matches!(
            shared.load_lazy_workspace(workspace(), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())).await,
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
                .expect("released namespace")
                .is_none()
        );

        let admitted = shared.namespace().expect("shared admission");
        let concurrent = LocalCoreStateStore::new(directory.path())
            .namespace()
            .expect("shared admissions coexist");
        assert!(is_ownership_conflict(&LocalCoreStateStore::open_owned(
            directory.path()
        )));
        drop(admitted);
        drop(concurrent);
        LocalCoreStateStore::open_owned(directory.path()).expect("owner after admissions end");
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
