use super::*;

fn finite_native_budget() -> WorkBudget {
    let mut value = serde_json::to_value(WorkBudget::UNBOUNDED).expect("budget encoding");
    for field in value.as_object_mut().expect("budget object").values_mut() {
        *field = serde_json::Value::from(100_000_000_u64);
    }
    serde_json::from_value(value).expect("finite native budget")
}

async fn native_backend(
    directory: &std::path::Path,
    work: &NativeMaterializationWork,
) -> Arc<NativeTreeMaterializationBackend> {
    let root = directory.join("root");
    let operation = directory.join("operation");
    std::fs::create_dir(&root).expect("fixture source root");
    std::fs::create_dir_all(operation.join("target")).expect("fixture target");
    std::fs::create_dir(operation.join("backup")).expect("fixture backup");
    std::fs::write(root.join("file"), b"before").expect("fixture source bytes");
    std::fs::write(operation.join("target/file"), b"target").expect("fixture target bytes");
    let mut root = work.path(&root, None).expect("funded backend root path");
    let mut operation = work
        .path(&operation, None)
        .expect("funded backend operation path");
    root.1
        .absorb(&mut operation.1)
        .expect("same original allocation context");
    let admission = NativeTreeMaterializationBackend::admission()
        .await
        .expect("descriptor admission");
    NativeTreeMaterializationBackend::new(root.0, operation.0, root.1, work.clone(), admission)
        .expect("real original-resource backend")
}

async fn native_prepared_packet(
    store: &CrashJournalStore,
    backend: &Arc<NativeTreeMaterializationBackend>,
    work: &NativeMaterializationWork,
    case: u8,
) -> MaterializationJournal {
    let mut path = work.copy_bytes(b"file").expect("real paid path copy");
    let mut custody = work
        .claim(
            std::mem::size_of::<MaterializationEdit>() as u64,
            u64::from((std::mem::size_of::<MaterializationEdit>() as u64) != 0),
            acyclic_fs::WorkCounters::default(),
        )
        .expect("admit plan vector before allocation");
    let mut edits = Vec::with_capacity(1);
    custody
        .reconcile_capacity((edits.capacity() * std::mem::size_of::<MaterializationEdit>()) as u64)
        .expect("retain actual vector capacity before any late refusal");
    custody
        .absorb(&mut path.1)
        .expect("same original paid plan");
    edits.push(MaterializationEdit::Remove {
        path: String::from_utf8(path.0).expect("static valid UTF-8 without copying"),
    });
    let plan = MaterializationPlan {
        operation_id: OperationId::from_bytes([case; 16]),
        intent: MaterializationIntent::GenerationTransition {
            from: generation(1),
            to: generation(2),
        },
        edits,
    };
    JournaledMaterializer::new(store.clone(), backend.clone())
        .prepare(plan, Some(custody))
        .await
        .expect("production-owned original journal")
}

#[tokio::test]
async fn accepted_completion_crashes_keep_native_custody_and_truthful_durable_prefixes() {
    acyclic_native_runtime::initialize_native_io_services().expect("actual native I/O services");
    for after_write in [false, true] {
        for cancel_after_action in [false, true] {
            let directory = tempfile::tempdir().expect("physical action fixture");
            let cancellation = CancellationToken::new();
            let budget = finite_native_budget();
            let original = NativeMaterializationWork::new(budget, &cancellation)
                .expect("one original finite work ledger");
            let work = original
                .native_volume_work(budget)
                .expect("original native resource");
            let backend = native_backend(directory.path(), &work).await;
            let store = CrashJournalStore::default();
            let journal = native_prepared_packet(&store, &backend, &work, 0x71).await;
            let operation_id = journal.plan.operation_id;
            let last_durable_work = journal.work;

            assert!(
                journal
                    .custody
                    .as_ref()
                    .expect("original custody")
                    .work()
                    .shares_allocation_context(&work)
            );
            let file = directory.path().join("root/file");
            let output = std::fs::OpenOptions::new()
                .write(true)
                .open(&file)
                .expect("fixture output before admission");
            store.fail_at_relative_write(1, after_write);
            let before_admission = original.receipt();
            let completion = store
                .prepare_action(journal)
                .await
                .expect("genuine Memory completion accepted before the physical action");
            assert!(original.receipt().bytes_copied > before_admission.bytes_copied);
            assert_eq!(
                std::fs::read(&file).expect("unchanged before acceptance"),
                b"before"
            );
            acyclic_native_runtime::write_all_at(
                &output,
                0,
                b"after!",
                Some((work.runtime_meter().as_ref(), false)),
            )
            .expect("actual original-resource action");
            if cancel_after_action {
                cancellation.cancel();
            }
            let live_prefix = original.receipt();
            let durable_prefix = work.journal_work().expect("pure original durable prefix");
            let error = completion.await.expect_err("real before/after write seam");
            assert_eq!(
                original.receipt(),
                live_prefix,
                "no allocation, copy, or admission after action"
            );
            let CrashStoreError::InjectedCrash(packet) = error else {
                panic!("expected the injected crash to retain its real owned packet");
            };
            let custody = packet
                .custody
                .as_ref()
                .expect("native fault never becomes ordinary None");
            assert!(custody.work().shares_allocation_context(&work));
            let retained_packet_bytes = custody.bytes();
            assert_eq!(packet.revision, 1 + u64::from(after_write));
            assert_eq!(
                packet.work,
                if after_write {
                    durable_prefix
                } else {
                    last_durable_work
                }
            );
            assert!(live_prefix.output_bytes > last_durable_work.output_bytes);
            assert_eq!(
                std::fs::read(&file).expect("independent physical observation"),
                b"after!"
            );
            live_prefix
                .verify(budget)
                .expect("full observed prefix fits the original finite budget");
            store.clear_failure();
            if cancel_after_action {
                assert!(
                    matches!(
                        store.load(operation_id).await,
                        Err(CrashStoreError::Memory(StoreError::NativeWork(
                            acyclic_fs::NativeTreeMaterializationError::Cancelled,
                        )))
                    ),
                    "cold reads never replace the cancelled original meter with an unbounded one"
                );
            } else {
                // Reopen the consumer, not a replacement journal engine or ledger.
                let reopened = JournaledMaterializer::new(store.clone(), backend.clone());
                let retained = reopened
                    .load(operation_id)
                    .await
                    .expect("real same-store reopen")
                    .expect("original durable journal");
                assert_eq!(&retained, &packet);
                assert!(
                    retained
                        .custody
                        .as_ref()
                        .expect("funded reopened packet")
                        .work()
                        .shares_allocation_context(&work)
                );
            }
            let live_bytes = original.live_allocation_bytes();
            drop(packet);
            assert_eq!(
                original.live_allocation_bytes(),
                live_bytes - retained_packet_bytes
            );
        }
    }
}

#[tokio::test]
async fn accepted_completion_conflict_keeps_the_whole_real_memory_error_and_packet() {
    acyclic_native_runtime::initialize_native_io_services().expect("actual native I/O services");
    let directory = tempfile::tempdir().expect("real native conflict fixture");
    let budget = finite_native_budget();
    let original = NativeMaterializationWork::new(budget, &CancellationToken::new())
        .expect("original finite meter");
    let work = original
        .native_volume_work(budget)
        .expect("original native resource");
    let backend = native_backend(directory.path(), &work).await;
    let store = CrashJournalStore::default();
    let journal = native_prepared_packet(&store, &backend, &work, 0x72).await;
    let operation_id = journal.plan.operation_id;

    let completion = store
        .prepare_action(journal)
        .await
        .expect("actual accepted Memory completion");
    // An independent actor reaches the same real Memory store, not fixture metadata.
    let mut competitor = store
        .inner
        .load(operation_id)
        .await
        .expect("actual competing load")
        .expect("original record");
    competitor.revision += 1;
    let (stored, competitor) = store
        .inner
        .compare_and_swap(operation_id, 1, competitor)
        .await
        .expect("genuine competing CAS");
    assert!(stored);
    let before = original.receipt();
    let error = completion
        .await
        .expect_err("actual accepted CAS sees the winner");
    assert_eq!(
        original.receipt(),
        before,
        "conflict performs no late copy or allocation"
    );
    let CrashStoreError::Memory(StoreError::Conflict(packet)) = error else {
        panic!("expected the intact real Memory conflict error");
    };
    assert!(
        packet
            .custody
            .as_ref()
            .expect("intact original error custody")
            .work()
            .shares_allocation_context(&work)
    );
    let retained = store
        .load(operation_id)
        .await
        .expect("real winning record query")
        .expect("winner");
    assert_eq!(&retained, &competitor);
    original
        .receipt()
        .verify(budget)
        .expect("actual whole conflict prefix");
}
