use super::*;

#[tokio::test]
async fn restart_completes_from_every_forward_journal_boundary() {
    let expected = target_tree();
    // Preparing, one preimage record per edit, Prepared, Applying, one
    // progress record per edit, Applied: the production persist boundaries.
    let boundary_count = 2 * plan(0).edits.len() + 4;
    for after_write in [false, true] {
        for boundary in 1..=boundary_count {
            let store = CrashJournalStore::default();
            store.fail_at_relative_write(boundary, after_write);
            let backend = TreeBackend::source();
            let operation = plan(u8::try_from(boundary).expect("small boundary"));
            let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
            assert!(matches!(
                materializer
                    .apply(operation.clone(), &mut || async {
                        Ok::<(), MaterializationError<_, _>>(())
                    })
                    .await,
                Err(MaterializationError::Store(_))
            ));
            drop(materializer);

            store.clear_failure();
            let restarted = JournaledMaterializer::new(store, backend.clone());
            let retained = restarted
                .load(operation.operation_id)
                .await
                .expect("query original journal after reopen");
            let durable_writes = boundary - usize::from(!after_write);
            if let Some(journal) = &retained {
                assert_eq!(journal.plan, operation);
                if journal.phase == MaterializationPhase::Preparing {
                    assert_eq!(backend.snapshot(), source_tree());
                }
            } else {
                assert_eq!(durable_writes, 0);
                assert_eq!(backend.snapshot(), source_tree());
            }
            let recovered = restarted
                .recover(
                    operation.operation_id,
                    MaterializationRecovery::Complete,
                    &mut || async { Ok::<(), MaterializationError<_, _>>(()) },
                )
                .await
                .expect("restart recovery");
            let recovered = if retained.is_some() {
                recovered.expect("durable journal")
            } else {
                assert!(recovered.is_none());
                restarted
                    .apply(operation.clone(), &mut || async {
                        Ok::<(), MaterializationError<_, _>>(())
                    })
                    .await
                    .expect("retry undurable intent")
            };
            assert_eq!(recovered.plan, operation);
            assert_eq!(recovered.phase, MaterializationPhase::Applied);
            assert_eq!(backend.snapshot(), expected);
            assert_eq!(
                restarted
                    .apply(operation, &mut || async {
                        Ok::<(), MaterializationError<_, _>>(())
                    })
                    .await
                    .expect("duplicate replay")
                    .phase,
                MaterializationPhase::Applied
            );
        }
    }
}

#[tokio::test]
async fn restart_rolls_back_every_partial_forward_boundary() {
    let expected = source_tree();
    // All forward boundaries except terminal Applied, covered below.
    let boundary_count = 2 * plan(0).edits.len() + 3;
    for after_write in [false, true] {
        for boundary in 1..=boundary_count {
            let store = CrashJournalStore::default();
            store.fail_at_relative_write(boundary, after_write);
            let backend = TreeBackend::source();
            let operation = plan(u8::try_from(boundary + 0x10).expect("small boundary"));
            let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
            assert!(matches!(
                materializer
                    .apply(operation.clone(), &mut || async {
                        Ok::<(), MaterializationError<_, _>>(())
                    })
                    .await,
                Err(MaterializationError::Store(_))
            ));
            drop(materializer);

            store.clear_failure();
            let restarted = JournaledMaterializer::new(store, backend.clone());
            let retained = restarted
                .load(operation.operation_id)
                .await
                .expect("query partial original journal");
            let durable_writes = boundary - usize::from(!after_write);
            if let Some(journal) = &retained {
                assert_eq!(journal.plan, operation);
                if journal.phase == MaterializationPhase::Preparing {
                    assert_eq!(backend.snapshot(), expected);
                }
            } else {
                assert_eq!(durable_writes, 0);
            }
            let recovered = restarted
                .recover(
                    operation.operation_id,
                    MaterializationRecovery::RollBack,
                    &mut || async { Ok::<(), MaterializationError<_, _>>(()) },
                )
                .await
                .expect("partial rollback recovery");
            if let Some(journal) = recovered {
                assert!(retained.is_some());
                assert_eq!(journal.plan, operation);
                assert_eq!(journal.phase, MaterializationPhase::RolledBack);
            } else {
                assert!(retained.is_none());
            }
            assert_eq!(backend.snapshot(), expected);
        }
    }
}

#[tokio::test]
async fn restart_rolls_back_from_every_reverse_journal_boundary() {
    let expected = source_tree();
    // RollingBack, one reverse restore-progress record per edit, RolledBack.
    let boundary_count = plan(0).edits.len() + 2;
    for after_write in [false, true] {
        for boundary in 1..=boundary_count {
            let store = CrashJournalStore::default();
            let backend = TreeBackend::source();
            let operation = plan(u8::try_from(boundary + 8).expect("small boundary"));
            JournaledMaterializer::new(store.clone(), backend.clone())
                .apply(operation.clone(), &mut || async {
                    Ok::<(), MaterializationError<_, _>>(())
                })
                .await
                .expect("initial apply");
            assert_eq!(backend.snapshot(), target_tree());
            store.fail_at_relative_write(boundary, after_write);
            let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
            assert!(matches!(
                materializer
                    .recover(
                        operation.operation_id,
                        MaterializationRecovery::RollBack,
                        &mut || async { Ok::<(), MaterializationError<_, _>>(()) }
                    )
                    .await,
                Err(MaterializationError::Store(_))
            ));
            drop(materializer);

            store.clear_failure();
            let restarted = JournaledMaterializer::new(store, backend.clone());
            let retained = restarted
                .load(operation.operation_id)
                .await
                .expect("query original rollback journal")
                .expect("durable journal");
            assert_eq!(retained.plan, operation);
            let recovered = restarted
                .recover(
                    operation.operation_id,
                    MaterializationRecovery::RollBack,
                    &mut || async { Ok::<(), MaterializationError<_, _>>(()) },
                )
                .await
                .expect("restart rollback")
                .expect("durable journal");
            assert_eq!(recovered.plan, operation);
            assert_eq!(recovered.phase, MaterializationPhase::RolledBack);
            assert_eq!(backend.snapshot(), expected);
            assert_eq!(
                restarted
                    .recover(
                        operation.operation_id,
                        MaterializationRecovery::RollBack,
                        &mut || async { Ok::<(), MaterializationError<_, _>>(()) }
                    )
                    .await
                    .expect("duplicate rollback")
                    .expect("terminal journal")
                    .phase,
                MaterializationPhase::RolledBack
            );
        }
    }
}
