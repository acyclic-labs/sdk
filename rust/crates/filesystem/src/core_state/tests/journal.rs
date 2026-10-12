use super::*;

#[tokio::test]
async fn materialization_operations_are_discoverable_and_terminal_cleanup_is_safe() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let operation_id = OperationId::new();
    let journal = materialization_journal(operation_id);
    assert!(
        MaterializationJournalStore::compare_and_swap(&store, operation_id, 0, {
            let mut journal =
                crate::materializer::duplicate_journal(&journal).expect("ordinary journal copy");
            journal.custody = None;
            journal
        })
        .await
        .expect("write journal")
        .0
    );
    assert_eq!(
        store
            .materialization_operations_async()
            .await
            .expect("list materializations"),
        [operation_id]
    );
    assert!(matches!(
        store.remove_materialization_async(operation_id).await,
        Err(LocalCoreStateStoreError::MaterializationInProgress)
    ));
    let mut terminal = journal;
    terminal.revision = 2;
    terminal.phase = crate::MaterializationPhase::Applied;
    assert!(
        MaterializationJournalStore::compare_and_swap(&store, operation_id, 1, {
            let mut journal = terminal;
            journal.custody = None;
            journal
        })
        .await
        .expect("finish journal")
        .0
    );
    store
        .remove_materialization_async(operation_id)
        .await
        .expect("remove terminal journal");
    assert!(
        store
            .materialization_operations_async()
            .await
            .expect("list cleaned materializations")
            .is_empty()
    );
}

#[tokio::test]
async fn previous_only_materialization_journal_is_discoverable_and_recovered() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().expect("admitted namespace");
    let operation_id = OperationId::from_bytes([0x73; 16]);
    let mut journal = materialization_journal(operation_id);
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
        .record("materialization", &operation_id.into_bytes())
        .expect("record paths");
    std::fs::create_dir_all(&paths.directory).expect("journal directory");
    std::fs::write(
        &paths.previous,
        serde_json::to_vec(&journal).expect("serialize journal"),
    )
    .expect("write previous journal");

    assert_eq!(
        store
            .materialization_operations_async()
            .await
            .expect("discover journal"),
        [operation_id]
    );
    assert_eq!(
        MaterializationJournalStore::load(&store, operation_id)
            .await
            .expect("recover journal")
            .map(|record| record),
        Some(journal)
    );
    assert!(paths.current.exists());
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut value =
            serde_json::to_value(crate::WorkBudget::UNBOUNDED).expect("canonical budget fixture");
        for field in value.as_object_mut().expect("budget fields").values_mut() {
            *field = serde_json::Value::from(100_000_000_u64);
        }
        let mut budget: crate::WorkBudget = serde_json::from_value(value).expect("finite budget");
        budget.source_bytes_read = std::fs::metadata(&paths.current)
            .expect("actual durable record")
            .len();
        let cancellation = crate::CancellationToken::new();
        let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation)
            .expect("original finite meter");
        let baseline = work.live_allocation_bytes();
        let record =
            MaterializationJournalStore::load(&store.materialization(work.clone()), operation_id)
                .await
                .expect("bounded original durable decoder")
                .expect("original journal");
        let allocation = record
            .custody
            .as_ref()
            .expect("actual decoded payload custody");
        assert!(
            allocation.bytes() > 0,
            "actual path and image buffers retain decoder custody"
        );
        assert!(allocation.work().shares_ledger(&work));
        assert_eq!(work.live_allocation_bytes(), baseline + allocation.bytes());
        assert_eq!(work.receipt().source_bytes_read, budget.source_bytes_read);
        work.receipt()
            .verify(budget)
            .expect("exact record read and all finite work");
        cancellation.cancel();
        let prefix = work.receipt();
        drop(record);
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(
            work.receipt(),
            prefix,
            "original decoded guard retires without a second admission or cancellation check"
        );
    }
}

#[tokio::test]
async fn accepted_materialization_completion_survives_cancellation_without_new_allocation() {
    let directory = tempfile::tempdir().expect("real journal namespace");
    let root = directory.path().join("checkout");
    std::fs::create_dir(&root).expect("original checkout allocation");
    let effect = root.join("file.txt");
    std::fs::write(&effect, b"before").expect("actual staged action fixture");
    let state_root = directory.path().join("state");
    let store = LocalCoreStateStore::new(&state_root);
    let cancellation = crate::CancellationToken::new();
    let mut value = serde_json::to_value(crate::WorkBudget::UNBOUNDED).expect("canonical budget");
    for field in value.as_object_mut().expect("budget fields").values_mut() {
        *field = serde_json::Value::from(100_000_000_u64);
    }
    let budget: crate::WorkBudget = serde_json::from_value(value).expect("all finite");
    let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation)
        .expect("original finite work");
    let held = crate::native_host::HostRoot::open(&root, &mut &work).expect("actual root identity");
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
        crate::materializer::MaterializationJournalStore::compare_and_swap(&store, operation, 0, {
            let mut journal = journal;
            journal.custody = None;
            journal
        })
        .await
        .expect("original durable preparing intent")
        .0
    );
    let measured = store.materialization(work.clone());
    let original = measured
        .load(operation)
        .await
        .expect("original decoder")
        .expect("same operation");
    let root_identity = original.root_identity;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join("file.txt"))
        .expect("authorized fixture file");
    let completion = measured
        .prepare_action(original)
        .await
        .expect("accepted exact original completion");
    acyclic_native_runtime::write_all_at(
        &file,
        0,
        b"after",
        Some((work.runtime_meter().as_ref(), false)),
    )
    .expect("actual bounded SDK action");
    cancellation.cancel();
    let known_prefix = work.journal_work().expect("actual post-action prefix");
    let allocations = work.receipt().allocation_operations;
    let completed = completion
        .await
        .expect("only the previously accepted metadata completion");
    assert_eq!(
        work.receipt().allocation_operations,
        allocations,
        "completion allocates no buffer, filename, request or boxed future"
    );
    assert_eq!(completed.revision, 2);
    assert_eq!(completed.root_identity, root_identity);
    assert_eq!(completed.work, known_prefix);
    assert_eq!(completed.phase, crate::MaterializationPhase::Preparing);
    assert!(
        completed
            .custody
            .as_ref()
            .expect("original packet")
            .work()
            .shares_ledger(&work)
    );
    work.receipt()
        .verify(budget)
        .expect("all completion work remains within original finite budget");
    drop(completed);
    drop(measured);
    drop(store);
    let restarted = LocalCoreStateStore::new(&state_root);
    let recovered = crate::materializer::MaterializationJournalStore::load(&restarted, operation)
        .await
        .expect("real process-style restart")
        .expect("same durable operation");
    assert_eq!(recovered.revision, 2);
    assert_eq!(recovered.root_identity, root_identity);
    assert_eq!(
        recovered.work, known_prefix,
        "durable prefix is a lower bound, not the later commit work"
    );
    assert_eq!(
        std::fs::read(effect).expect("real completed source effect"),
        b"aftere"
    );
}
