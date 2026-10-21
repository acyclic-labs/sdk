use super::*;

async fn native_write_count(image: Vec<u8>) -> u64 {
    let directory = tempfile::tempdir().unwrap();
    let work = crate::NativeMaterializationWork::new(
        native::finite_budget(),
        &crate::CancellationToken::new(),
    )
    .unwrap();
    let operation = OperationId::new();
    let mut journal = materialization_journal(operation);
    journal.phase = crate::MaterializationPhase::Preparing;
    journal
        .plan
        .edits
        .push(crate::MaterializationEdit::Install {
            path: "file".into(),
            image,
        });
    journal.custody = Some(
        work.claim(
            crate::materializer::journal_owned_bytes(&journal).unwrap(),
            0,
            crate::WorkCounters::UNCHARGED,
        )
        .unwrap(),
    );
    let store = LocalCoreStateStore::new(directory.path()).materialization(work.clone());
    work.state.lock().write_probe = Some(crate::demand::WriteProbe {
        writes: 0,
        stop_before: None,
    });
    let (stored, journal) =
        MaterializationJournalStore::compare_and_swap(&store, operation, 0, journal)
            .await
            .unwrap();
    assert!(stored);
    let writes = work.state.lock().write_probe.take().unwrap().writes;
    assert_eq!(
        MaterializationJournalStore::load(&store, operation)
            .await
            .unwrap(),
        Some(journal)
    );
    writes
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(4))]
    // The wire value round-trips exactly; JSON token count never becomes an
    // extra native write or crash boundary. All buffer capacity stays funded.
    #[test]
    fn native_journal_writes_are_independent_of_json_token_count(
        image in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let before = runtime.block_on(native_write_count(Vec::new()));
        let after = runtime.block_on(native_write_count(image));
        proptest::prop_assert_eq!(before, after);
    }
}

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
    std::fs::create_dir_all(paths.directory()).unwrap();
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
async fn accepted_materialization_completion_survives_cancellation_and_restart() {
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
    assert!(journal.is_valid_for(operation));
    assert!(
        MaterializationJournalStore::compare_and_swap(&store, operation, 0, journal)
            .await
            .unwrap()
            .0
    );
    let measured = store.materialization(work.clone());
    let original = crate::MaterializationJournalStore::load(&measured, operation)
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
    let completed = completion.await.unwrap();
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

#[derive(Default)]
struct CountWrites {
    writes: usize,
    bytes: Vec<u8>,
}
impl Write for CountWrites {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.writes += 1;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn one_record_write(writes: usize) -> proptest::test_runner::TestCaseResult {
    proptest::prop_assert_eq!(writes, 1, "JSON tokens escaped record buffering");
    Ok(())
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // A record smaller than the fixed buffer makes one backend write, with
    // byte-for-byte canonical JSON, independently of its number of tokens.
    #[test]
    fn ordinary_record_writes_are_independent_of_json_token_count(
        value in proptest::collection::vec(proptest::prelude::any::<u64>(), 1..256),
    ) {
        let mut buffered = CountWrites::default();
        write_buffered_json(&mut buffered, &value).unwrap();
        let expected = serde_json::to_vec(&value).unwrap();
        proptest::prop_assert!(expected.len() <= 8192);
        proptest::prop_assert_eq!(&buffered.bytes, &expected);
        proptest::prop_assert_eq!(serde_json::from_slice::<Vec<u64>>(&buffered.bytes).unwrap(), value.clone());
        one_record_write(buffered.writes)?;
        let mut old = CountWrites::default();
        serde_json::to_writer(&mut old, &value).unwrap();
        proptest::prop_assert_eq!(old.bytes, expected);
        proptest::prop_assert!(one_record_write(old.writes).is_err(), "unbuffered control escaped the same write bound");
    }
}
#[test]
fn ordinary_record_flush_failures_are_reported() {
    struct Fail;
    impl Write for Fail {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("record write failed"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    assert!(matches!(
        write_buffered_json(Fail, &42),
        Err(LocalCoreStateStoreError::Io(_))
    ));
}

#[test]
fn checkpoint_records_coalesce_tokens_on_ordinary_and_native_paths() {
    let directory = tempfile::tempdir().unwrap();
    let id = WorkspaceContextId::from_bytes([0x4a; 16]);
    let values = [
        None,
        Some(RecordValue::Context(workspace_context(
            directory.path(),
            id,
            1,
            WorkspaceContextState::Active,
        ))),
        Some(RecordValue::Children(ContextChildren([id].into()))),
        Some(RecordValue::ChildCount(ContextChildCount(1))),
        Some(RecordValue::ChildIndex(ContextChildIndex(1))),
        Some(RecordValue::Lineage(crash_lineage())),
        Some(RecordValue::LazyWorkspace(lazy_state(workspace(), 1))),
    ];
    let cxl = crate::CancellationToken::new();
    let work =
        crate::materializer::NativeMaterializationWork::new(super::native::finite_budget(), &cxl)
            .unwrap();
    for native in [false, true] {
        for value in &values {
            let mut expected = Vec::new();
            RecordValue::write_bare(value.as_ref(), &mut expected).unwrap();
            assert!(expected.len() < 8192);
            let before = work.receipt();
            let mut writer = CountWrites::default();
            RecordValue::write_checkpoint(value.as_ref(), &mut writer, native.then_some(&work))
                .unwrap();
            assert_eq!(writer.bytes, expected);
            assert_eq!(
                writer.writes, 1,
                "checkpoint JSON tokens escaped buffering: native={native}"
            );
            if native {
                assert_eq!(
                    work.receipt().bytes_encoded - before.bytes_encoded,
                    expected.len() as u64
                );
                assert!(work.receipt().bytes_copied - before.bytes_copied >= expected.len() as u64);
            }
        }
    }
}

#[test]
fn checkpoint_record_buffer_failures_are_reported_before_the_barrier() {
    struct Fail;
    impl Write for Fail {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("checkpoint backend write failed"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let cxl = crate::CancellationToken::new();
    let work =
        crate::materializer::NativeMaterializationWork::new(super::native::finite_budget(), &cxl)
            .unwrap();
    for native in [false, true] {
        let error =
            RecordValue::write_checkpoint(None, &mut Fail, native.then_some(&work)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("checkpoint backend write failed")
        );
    }
}

#[test]
fn checkpoint_native_accounting_tracks_encoded_and_written_bytes_once() {
    fn checkpoint(children: ContextChildren, native: bool) -> (u64, u64, u64, usize) {
        let directory = tempfile::tempdir().unwrap();
        let cancellation = crate::CancellationToken::new();
        let work =
            crate::NativeMaterializationWork::new(super::native::finite_budget(), &cancellation)
                .unwrap();
        let store = LocalCoreStateStore::new(directory.path());
        let store = if native {
            store.materialization(work.clone())
        } else {
            store
        };
        let namespace = store.namespace().unwrap();
        let id = WorkspaceContextId::from_bytes([0x63; 16]);
        let expected = serde_json::to_vec(&children).unwrap();
        let mut log = CoreLog::open(&namespace, Arc::default()).unwrap();
        let mut change = if native {
            Change::new(&work).unwrap()
        } else {
            Change::default()
        };
        change
            .put(
                (
                    RecordKey::Children(id),
                    Some(RecordValue::Children(children)),
                )
                    .into(),
            )
            .unwrap();
        log.commit(&namespace, change).unwrap();
        let before = work.receipt();
        log.checkpoint(&namespace).unwrap();
        let after = work.receipt();
        assert_eq!(
            std::fs::read(children_paths(&namespace, id).current).unwrap(),
            expected
        );
        drop(log);
        let mut reopened = CoreLog::open(&namespace, Arc::default()).unwrap();
        let loaded = reopened
            .get::<ContextChildren>(&namespace, id, None, None)
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(serde_json::to_vec(&loaded).unwrap(), expected);
        work.receipt()
            .verify(super::native::finite_budget())
            .unwrap();
        (
            after.bytes_encoded - before.bytes_encoded,
            after.object_bytes_written - before.object_bytes_written,
            after.backend_write_operations - before.backend_write_operations,
            expected.len(),
        )
    }
    for native in [false, true] {
        let empty = checkpoint(ContextChildren::default(), native);
        let populated = checkpoint(
            ContextChildren(
                [1_u8, 2, 3, 4]
                    .map(|byte| WorkspaceContextId::from_bytes([byte; 16]))
                    .into(),
            ),
            native,
        );
        assert!(populated.3 > empty.3);
        if native {
            let growth = (populated.3 - empty.3) as u64;
            // Equal-length random fixture names and identical record paths
            // cancel all fixed path encoding and filesystem operations. Only
            // the independently serialized JSON payload grows.
            assert_eq!(
                populated.0 - empty.0,
                growth,
                "production checkpoint charged payload encoding twice"
            );
            assert_eq!(empty.1, empty.3 as u64);
            assert_eq!(populated.1, populated.3 as u64);
            assert_eq!(
                populated.2, empty.2,
                "production checkpoint backend writes depend on JSON tokens"
            );
        }
    }
}
