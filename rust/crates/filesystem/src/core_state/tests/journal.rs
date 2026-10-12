use super::*;

#[tokio::test]
async fn materialization_operations_are_discoverable_and_terminal_cleanup_is_safe() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let operation = OperationId::new();
    let (stored, mut journal) = MaterializationJournalStore::compare_and_swap(
        &store,
        operation,
        0,
        materialization_journal(operation),
    )
    .await
    .unwrap();
    assert!(stored);
    assert_eq!(
        store.materialization_operations_async().await.unwrap(),
        [operation]
    );
    assert!(matches!(
        store.remove_materialization_async(operation).await,
        Err(LocalCoreStateStoreError::MaterializationInProgress)
    ));
    journal.revision = 2;
    journal.phase = crate::MaterializationPhase::Applied;
    assert!(
        MaterializationJournalStore::compare_and_swap(&store, operation, 1, journal)
            .await
            .unwrap()
            .0
    );
    store.remove_materialization_async(operation).await.unwrap();
    assert!(
        store
            .materialization_operations_async()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn previous_only_materialization_journal_is_discoverable_and_recovered() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().unwrap();
    let operation = OperationId::from_bytes([0x73; 16]);
    let mut journal = materialization_journal(operation);
    journal
        .plan
        .edits
        .push(crate::MaterializationEdit::Install {
            path: "file.txt".to_owned(),
            image: vec![1, 2, 3],
        });
    journal.preimages.push(crate::MaterializationPreimage {
        image: vec![4, 5, 6],
        custody: None,
    });
    let paths = namespace
        .record(MATERIALIZATION_FAMILY, &operation.into_bytes())
        .unwrap();
    std::fs::create_dir_all(&paths.directory).unwrap();
    std::fs::write(&paths.previous, serde_json::to_vec(&journal).unwrap()).unwrap();
    assert_eq!(
        store.materialization_operations_async().await.unwrap(),
        [operation]
    );
    assert_eq!(
        MaterializationJournalStore::load(&store, operation)
            .await
            .unwrap(),
        Some(journal)
    );
    assert!(paths.current.exists());
    #[cfg(not(target_arch = "wasm32"))]
    {
        let budget = crate::WorkBudget {
            source_bytes_read: std::fs::metadata(&paths.current).unwrap().len(),
            ..native::finite_budget()
        };
        let cxl = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(budget, &cxl).unwrap();
        let baseline = work.live_allocation_bytes();
        let record =
            MaterializationJournalStore::load(&store.materialization(work.clone()), operation)
                .await
                .unwrap()
                .unwrap();
        let allocation = record.custody.as_ref().unwrap();
        assert!(allocation.bytes() > 0);
        assert!(allocation.work().shares_ledger(&work));
        assert_eq!(work.live_allocation_bytes(), baseline + allocation.bytes());
        assert_eq!(work.receipt().source_bytes_read, budget.source_bytes_read);
        work.receipt().verify(budget).unwrap();
        cxl.cancel();
        let prefix = work.receipt();
        drop(record);
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(work.receipt(), prefix);
    }
}

#[tokio::test]
async fn accepted_materialization_completion_survives_cancellation_without_new_allocation() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("checkout");
    std::fs::create_dir(&root).unwrap();
    let effect = root.join("file.txt");
    std::fs::write(&effect, b"before").unwrap();
    let state = directory.path().join("state");
    let store = LocalCoreStateStore::new(&state);
    let cxl = crate::CancellationToken::new();
    let budget = native::finite_budget();
    let work = crate::NativeMaterializationWork::new(budget, &cxl).unwrap();
    let held = crate::native_host::HostRoot::open(&root, &mut &work).unwrap();
    let operation = OperationId::new();
    let mut journal = materialization_journal(operation);
    journal.root_identity = Some(held.identity().to_bytes());
    journal.phase = crate::MaterializationPhase::Preparing;
    journal
        .plan
        .edits
        .push(crate::MaterializationEdit::Install {
            path: "file.txt".to_owned(),
            image: vec![1],
        });
    assert!(
        MaterializationJournalStore::compare_and_swap(&store, operation, 0, journal)
            .await
            .unwrap()
            .0
    );
    let measured = store.materialization(work.clone());
    let original = MaterializationJournalStore::load(&measured, operation)
        .await
        .unwrap()
        .unwrap();
    let root_identity = original.root_identity;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&effect)
        .unwrap();
    let completion = measured.prepare_action(original).await.unwrap();
    acyclic_native_runtime::write_all_at(
        &file,
        0,
        b"after",
        Some((work.runtime_meter().as_ref(), false)),
    )
    .unwrap();
    cxl.cancel();
    let prefix = work.journal_work().unwrap();
    let allocations = work.receipt().allocation_operations;
    let completed = completion.await.unwrap();
    assert_eq!(work.receipt().allocation_operations, allocations);
    assert_eq!(completed.revision, 2);
    assert_eq!(completed.root_identity, root_identity);
    assert_eq!(completed.work, prefix);
    assert_eq!(completed.phase, crate::MaterializationPhase::Preparing);
    assert!(
        completed
            .custody
            .as_ref()
            .unwrap()
            .work()
            .shares_ledger(&work)
    );
    work.receipt().verify(budget).unwrap();
    drop((completed, measured, store));
    let reopened = LocalCoreStateStore::new(&state);
    let recovered = MaterializationJournalStore::load(&reopened, operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.revision, 2);
    assert_eq!(recovered.root_identity, root_identity);
    assert_eq!(recovered.work, prefix);
    assert_eq!(std::fs::read(effect).unwrap(), b"aftere");
}
