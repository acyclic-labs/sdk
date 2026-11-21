use super::*;

#[tokio::test]
async fn materialization_cas_flushes_once_for_a_batch_of_preimages() {
    use acyclic_native_runtime::testing::Schedule;

    crate::initialize_test_native_io();
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let operation = OperationId::new();
    let mut journal = materialization_journal(operation);
    journal.phase = crate::MaterializationPhase::Preparing;
    journal.plan.edits = (0..3)
        .map(|index| crate::MaterializationEdit::Install {
            path: format!("entry-{index}"),
            image: vec![1],
        })
        .collect();
    let (stored, mut journal) =
        MaterializationJournalStore::compare_and_swap(&store, operation, 0, journal)
            .await
            .unwrap();
    assert!(stored);
    journal.preimages = (0..3)
        .map(|_| crate::MaterializationPreimage {
            image: vec![2; 64],
            custody: None,
        })
        .collect();
    let expected = journal.revision;
    journal.revision += 1;
    let (schedule, mut events) = Schedule::new();
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    let started = Instant::now();
    let (stored, journal) = schedule
        .scope(MaterializationJournalStore::compare_and_swap(
            &store, operation, expected, journal,
        ))
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(stored);
    schedule.drained().await;
    driver.abort();
    let calls = schedule.durable_writes();
    let flushes: Vec<_> = calls.iter().filter(|call| call.flushes).collect();
    assert_eq!(flushes.len(), 1, "{calls:?}");
    assert_eq!(flushes[0].operation, "file:sync_data");
    assert!(!calls.iter().any(|call| call.operation.contains("rename")));
    eprintln!(
        "MATERIALIZATION_CAS preimages=3 flushes=1 elapsed_us={}",
        elapsed.as_micros()
    );
    assert_eq!(
        MaterializationJournalStore::load(&store, operation)
            .await
            .unwrap(),
        Some(journal)
    );
}

#[tokio::test]
async fn materialization_recovers_at_each_batch_frame_cut_and_retries() {
    use acyclic_native_runtime::testing::{DurableFault, Schedule, drive};
    crate::initialize_test_native_io();
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let operation = OperationId::new();
    let mut first = materialization_journal(operation);
    first.phase = crate::MaterializationPhase::Preparing;
    first.plan.edits = (0..2)
        .map(|index| crate::MaterializationEdit::Install {
            path: format!("entry-{index}"),
            image: vec![1],
        })
        .collect();
    let (stored, first) =
        MaterializationJournalStore::compare_and_swap(&store, operation, 0, first)
            .await
            .unwrap();
    assert!(stored);
    let namespace = store.namespace().unwrap();
    let paths = namespace
        .record(MATERIALIZATION_FAMILY, &operation.into_bytes())
        .unwrap();
    let accepted = std::fs::metadata(&paths.current).unwrap().len() as usize;
    let mut second = MaterializationJournalStore::load(&store, operation)
        .await
        .unwrap()
        .unwrap();
    second.revision += 1;
    second.preimages = (0..2)
        .map(|_| crate::MaterializationPreimage {
            image: vec![2; 64],
            custody: None,
        })
        .collect();
    let (schedule, mut events) = Schedule::new();
    let (stored, second) = drive(
        &schedule,
        &mut events,
        |_| {},
        MaterializationJournalStore::compare_and_swap(&store, operation, first.revision, second),
    )
    .await
    .unwrap();
    assert!(stored);
    let write = schedule
        .durable_writes()
        .into_iter()
        .find(|write| write.requested > 0)
        .unwrap();
    let full = std::fs::read(&paths.current).unwrap();

    let mut cuts = Vec::new();
    let mut offset = accepted;
    while offset < full.len() {
        let length = FRAME_FORMAT.body_length(&full[offset..]).unwrap();
        let extent = FRAME_FORMAT.frame_bytes(length).unwrap() as usize;
        cuts.extend([offset, offset + extent / 2, offset + extent - 1]);
        offset += extent;
    }
    assert_eq!(cuts.len(), 9, "two preimages and one closing receipt");
    assert_eq!(offset, full.len());

    assert_eq!(write.requested, full.len() - accepted);
    for cut in cuts {
        std::fs::write(&paths.current, &full[..accepted]).unwrap();
        let crashing = LocalCoreStateStore::new(directory.path());
        let mut replacement = MaterializationJournalStore::load(&crashing, operation)
            .await
            .unwrap()
            .unwrap();
        replacement.revision = second.revision;
        for image in &second.preimages {
            replacement.preimages.push(crate::MaterializationPreimage {
                image: image.image.clone(),
                custody: None,
            });
        }
        let (schedule, mut events) = Schedule::new();
        schedule.cut_at(write.ordinal, DurableFault::Torn(cut - accepted));
        let failed = drive(
            &schedule,
            &mut events,
            |_| {},
            MaterializationJournalStore::compare_and_swap(
                &crashing,
                operation,
                first.revision,
                replacement,
            ),
        )
        .await;
        assert!(failed.is_err(), "cut at byte {cut} was acknowledged");
        assert_eq!(std::fs::read(&paths.current).unwrap(), &full[..cut]);
        let reopened = LocalCoreStateStore::new(directory.path());
        let mut recovered = MaterializationJournalStore::load(&reopened, operation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered, first, "recovered at byte {cut}");
        recovered.revision = second.revision;
        for image in &second.preimages {
            recovered.preimages.push(crate::MaterializationPreimage {
                image: image.image.clone(),
                custody: None,
            });
        }
        assert!(
            MaterializationJournalStore::compare_and_swap(
                &reopened,
                operation,
                first.revision,
                recovered
            )
            .await
            .unwrap()
            .0
        );
        let reopened = LocalCoreStateStore::new(directory.path());
        assert_eq!(
            MaterializationJournalStore::load(&reopened, operation)
                .await
                .unwrap()
                .as_ref(),
            Some(&second),
            "retried at byte {cut}"
        );
    }
}

#[tokio::test]
async fn materialization_compacts_when_receipt_history_outgrows_live_data() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let operation = OperationId::new();
    let mut journal = materialization_journal(operation);
    journal
        .plan
        .edits
        .push(crate::MaterializationEdit::Install {
            path: "entry".to_owned(),
            image: vec![1],
        });
    journal.preimages.push(crate::MaterializationPreimage {
        image: vec![2; 64],
        custody: None,
    });
    let namespace = store.namespace().unwrap();
    let paths = namespace
        .record(MATERIALIZATION_FAMILY, &operation.into_bytes())
        .unwrap();
    let mut previous_length = 0;
    let mut compacted = false;
    for expected in 0..8 {
        journal.revision = expected + 1;
        let (stored, next) =
            MaterializationJournalStore::compare_and_swap(&store, operation, expected, journal)
                .await
                .unwrap();
        assert!(stored);
        journal = next;
        let length = std::fs::metadata(&paths.current).unwrap().len();
        if length < previous_length {
            compacted = true;
            let reopened = LocalCoreStateStore::new(directory.path());
            assert_eq!(
                MaterializationJournalStore::load(&reopened, operation)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&journal)
            );
        }
        previous_length = length;
    }
    assert!(
        compacted,
        "obsolete receipts kept growing past the live state"
    );
}

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
    journal
        .fund(work.claim(0, 0, crate::WorkCounters::UNCHARGED).unwrap())
        .unwrap();
    let store = LocalCoreStateStore::new(directory.path()).materialization(work.clone());
    work.state.lock().write_probe = Some(crate::ledger::WriteProbe {
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
async fn materialization_journal_with_a_torn_tail_is_discoverable_and_recovered() {
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
    let (stored, journal) =
        MaterializationJournalStore::compare_and_swap(&store, operation, 0, journal)
            .await
            .unwrap();
    assert!(stored);
    OpenOptions::new()
        .append(true)
        .open(&paths.current)
        .unwrap()
        .write_all(b"torn")
        .unwrap();
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
        let baseline = work.live_allocation_bytes().unwrap();
        let record =
            MaterializationJournalStore::load(&store.materialization(work.clone()), operation)
                .await
                .unwrap()
                .unwrap();
        let allocation = record.custody().unwrap();
        assert!(allocation.bytes() > 0);
        assert!(allocation.work().shares_ledger(&work));
        assert_eq!(
            work.live_allocation_bytes().unwrap(),
            baseline + allocation.bytes()
        );
        assert_eq!(
            work.receipt().unwrap().source_bytes_read,
            budget.source_bytes_read
        );
        work.receipt().unwrap().verify(budget).unwrap();
        cxl.cancel();
        let prefix = work.receipt().unwrap();
        drop(record);
        assert_eq!(work.live_allocation_bytes().unwrap(), baseline);
        assert_eq!(work.receipt().unwrap(), prefix);
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
    let written = work.receipt().unwrap().object_bytes_written;
    let durability = work.receipt().unwrap().durability_operations;
    let completion = measured.prepare_action(original).await.unwrap();
    assert_eq!(
        work.receipt().unwrap().object_bytes_written,
        written,
        "preparing an append must not rewrite the durable prefix"
    );
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
    assert_eq!(
        work.receipt().unwrap().durability_operations - durability,
        1
    );
    assert_eq!(completed.revision, 2);
    assert_eq!(completed.root_identity, root_identity);
    assert_eq!(completed.work, prefix);
    assert_eq!(completed.phase, crate::MaterializationPhase::Preparing);
    assert!(completed.custody().unwrap().work().shares_ledger(&work));
    work.receipt().unwrap().verify(budget).unwrap();
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
    let work = crate::ledger::NativeMaterializationWork::new(super::native::finite_budget(), &cxl)
        .unwrap();
    for native in [false, true] {
        for value in &values {
            let mut expected = Vec::new();
            RecordValue::write_bare(value.as_ref(), &mut expected).unwrap();
            assert!(expected.len() < 8192);
            let before = work.receipt().unwrap();
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
                    work.receipt().unwrap().bytes_encoded - before.bytes_encoded,
                    expected.len() as u64
                );
                assert!(
                    work.receipt().unwrap().bytes_copied - before.bytes_copied
                        >= expected.len() as u64
                );
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
    let work = crate::ledger::NativeMaterializationWork::new(super::native::finite_budget(), &cxl)
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
        let before = work.receipt().unwrap();
        log.checkpoint(&namespace).unwrap();
        let after = work.receipt().unwrap();
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
            .unwrap()
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

#[tokio::test]
async fn journal_progress_reads_and_writes_only_the_appended_preimage() {
    let mut first_history = None;
    for count in [8, 64, 512] {
        let directory = tempfile::tempdir().unwrap();
        let cxl = crate::CancellationToken::new();
        let work =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cxl).unwrap();
        let store = LocalCoreStateStore::new(directory.path()).materialization(work.clone());
        let operation = OperationId::new();
        let mut journal = materialization_journal(operation);
        journal.phase = crate::MaterializationPhase::Preparing;
        journal.plan.edits = (0..count)
            .map(|index| crate::MaterializationEdit::Install {
                path: format!("directory/file-{index:04}"),
                image: vec![1],
            })
            .collect();
        journal
            .fund(work.claim(0, 0, crate::WorkCounters::UNCHARGED).unwrap())
            .unwrap();
        let (stored, mut journal) =
            MaterializationJournalStore::compare_and_swap(&store, operation, 0, journal)
                .await
                .unwrap();
        assert!(stored);
        journal.reserve_preimages(count).unwrap();
        // One durable commit per measured step, plus one that seeds the
        // history between them: the step at the end of the history is
        // measured against the step at its start, and across plan sizes.
        let mut first = None;
        let mut stored_preimages = 0;
        for index in [0, count - 1] {
            while journal.preimages.len() < index {
                let (image, custody) = work.copy_bytes(&[255; 64]).unwrap();
                journal
                    .append_funded_preimage(crate::MaterializationPreimage {
                        image,
                        custody: Some(custody),
                    })
                    .unwrap();
            }
            if journal.preimages.len() > stored_preimages {
                let expected = journal.revision;
                journal.revision += 1;
                let (stored, updated) = MaterializationJournalStore::compare_and_swap(
                    &store, operation, expected, journal,
                )
                .await
                .unwrap();
                assert!(stored);
                journal = updated;
            }
            let (image, custody) = work.copy_bytes(&[255; 64]).unwrap();
            let before = work.receipt().unwrap();
            assert!(store.matches(&journal).await.unwrap());
            journal
                .append_funded_preimage(crate::MaterializationPreimage {
                    image,
                    custody: Some(custody),
                })
                .unwrap();
            let expected = journal.revision;
            journal.revision += 1;
            let (stored, updated) =
                MaterializationJournalStore::compare_and_swap(&store, operation, expected, journal)
                    .await
                    .unwrap();
            assert!(stored);
            journal = updated;
            stored_preimages = journal.preimages.len();
            let after = work.receipt().unwrap();
            let delta = (
                after.source_bytes_read - before.source_bytes_read,
                after.object_bytes_written - before.object_bytes_written,
            );
            let baseline = *first.get_or_insert(delta);
            let across_sizes = *first_history.get_or_insert(delta);
            assert!(
                delta.0 <= across_sizes.0 + 256,
                "plan size changed per-step reads: {delta:?} vs {across_sizes:?}"
            );
            assert!(
                delta.1 <= across_sizes.1 + 256,
                "plan size changed per-step writes: {delta:?} vs {across_sizes:?}"
            );
            if index + 1 == count {
                eprintln!(
                    "journal_delta history={count} read={} written={}",
                    delta.0, delta.1
                );
            }
            assert!(
                delta.0 <= baseline.0 + 256,
                "history was reread at {index}/{count}: {delta:?} vs {baseline:?}"
            );
            assert!(
                delta.1 <= baseline.1 + 256,
                "history was rewritten at {index}/{count}: {delta:?} vs {baseline:?}"
            );
        }
        let recovered = MaterializationJournalStore::load(&store, operation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered, journal);
        drop(recovered);
        journal.revision += 1;
        journal.phase = crate::MaterializationPhase::RolledBack;
        let expected = journal.revision - 1;
        let (stored, journal) =
            MaterializationJournalStore::compare_and_swap(&store, operation, expected, journal)
                .await
                .unwrap();
        assert!(stored);
        let before = work.receipt().unwrap();
        assert!(
            !MaterializationJournalStore::compare_and_swap(&store, operation, expected, journal)
                .await
                .unwrap()
                .0
        );
        assert_eq!(
            work.receipt().unwrap().object_bytes_written,
            before.object_bytes_written
        );
        store.remove_materialization_async(operation).await.unwrap();
        assert!(
            MaterializationJournalStore::load(&store, operation)
                .await
                .unwrap()
                .is_none()
        );
        let namespace = store.namespace().unwrap();
        let paths = namespace
            .record(MATERIALIZATION_FAMILY, &operation.into_bytes())
            .unwrap();
        assert!(!paths.current.exists());
    }
}

#[tokio::test]
async fn journal_receipts_refuse_independently_admitted_history() {
    let left = tempfile::tempdir().unwrap();
    let right = tempfile::tempdir().unwrap();
    let stores = [
        LocalCoreStateStore::new(left.path()),
        LocalCoreStateStore::new(right.path()),
    ];
    let operation = OperationId::new();
    for (index, store) in stores.iter().enumerate() {
        let mut journal = materialization_journal(operation);
        journal.phase = crate::MaterializationPhase::Preparing;
        journal
            .plan
            .edits
            .push(crate::MaterializationEdit::Install {
                path: "file".into(),
                image: vec![index as u8],
            });
        journal.preimages.push(crate::MaterializationPreimage {
            image: vec![index as u8 + 2],
            custody: None,
        });
        assert!(MaterializationJournal::empty(journal.plan.clone(), None).is_none());
        assert!(
            MaterializationJournalStore::compare_and_swap(store, operation, 0, journal)
                .await
                .unwrap()
                .0
        );
    }
    // Identical revisions and progress do not admit another store's history.
    // Exercise both physical-action admission and ordinary progress publication.
    let foreign = MaterializationJournalStore::load(&stores[1], operation)
        .await
        .unwrap()
        .unwrap();
    assert!(!stores[0].matches(&foreign).await.unwrap());
    assert!(stores[1].matches(&foreign).await.unwrap());
    assert!(stores[0].prepare_action(foreign).await.is_err());
    let mut foreign = MaterializationJournalStore::load(&stores[1], operation)
        .await
        .unwrap()
        .unwrap();
    let revision = foreign.revision;
    foreign.revision += 1;
    assert!(
        !MaterializationJournalStore::compare_and_swap(&stores[0], operation, revision, foreign)
            .await
            .unwrap()
            .0
    );
    let original = MaterializationJournalStore::load(&stores[0], operation)
        .await
        .unwrap()
        .unwrap();
    for field in ["plan", "preimages"] {
        let mut snapshot = serde_json::to_value(&original).unwrap();
        if field == "plan" {
            snapshot["plan"]["edits"][0]["Install"]["image"] = serde_json::json!([7]);
        } else {
            snapshot["preimages"][0]["image"] = serde_json::json!([7]);
        }
        let forged: MaterializationJournal = serde_json::from_value(snapshot).unwrap();
        assert_eq!(forged.revision, original.revision);
        assert!(!stores[0].matches(&forged).await.unwrap());
        assert!(stores[0].prepare_action(forged).await.is_err());
    }
    assert_eq!(original.revision, revision);
    let crate::MaterializationEdit::Install { image, .. } = &original.plan.edits[0] else {
        panic!("install")
    };
    assert_eq!(image, &[0]);
    assert_eq!(original.preimages[0].image, [2]);
    assert!(stores[0].matches(&original).await.unwrap());
    let copy = original.try_clone().unwrap();
    assert!(stores[0].matches(&copy).await.unwrap());
    drop(copy);
    let completion = stores[0].prepare_action(original).await.unwrap();
    let acknowledged = completion.await.unwrap();
    assert!(stores[0].matches(&acknowledged).await.unwrap());
}
