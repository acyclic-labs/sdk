use super::*;

#[tokio::test]
async fn capture_failures_reopen_original_partial_intent_without_physical_edits() {
    for recovery in [
        MaterializationRecovery::Complete,
        MaterializationRecovery::RollBack,
    ] {
        // Fail before the first capture and at each later path in this install,
        // remove, rename plan. Only successfully persisted captures may survive.
        for ordinal in 1..=plan(0).edits.len() {
            let store = CrashJournalStore::default();
            let tree = TreeBackend::source();
            let backend = MutationCrashBackend::before_capture(tree.clone(), ordinal);
            let original = plan(u8::try_from(0x60 + ordinal).expect("small capture ordinal"));
            let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
            assert!(matches!(
                materializer
                    .apply(original.clone(), &mut || async {
                        Ok::<(), MaterializationError<_, _>>(())
                    })
                    .await,
                Err(MaterializationError::Backend(_))
            ));
            drop(materializer);

            assert_eq!(tree.snapshot(), source_tree());

            let restarted = JournaledMaterializer::new(store.clone(), backend.clone());
            let retained = restarted
                .load(original.operation_id)
                .await
                .expect("query same store after capture failure")
                .expect("original intent survives before first capture");
            assert_eq!(retained.plan, original);
            assert_eq!(retained.preimages.len(), ordinal - 1);
            assert_eq!(retained.phase, MaterializationPhase::Preparing);

            let mut changed_from = original.clone();
            changed_from.intent = MaterializationIntent::GenerationTransition {
                from: generation(8),
                to: generation(2),
            };
            let mut changed_to = original.clone();
            changed_to.intent = MaterializationIntent::GenerationTransition {
                from: generation(1),
                to: generation(9),
            };
            let mut changed_image = original.clone();
            changed_image.edits[0] = MaterializationEdit::Install {
                path: "a".to_owned(),
                image: b"conflicting-a".to_vec(),
            };
            let mut changed_order = original.clone();
            changed_order.edits.reverse();
            for changed in [changed_from, changed_to, changed_image, changed_order] {
                assert!(matches!(
                    restarted
                        .apply(changed, &mut || async {
                            Ok::<(), MaterializationError<_, _>>(())
                        })
                        .await,
                    Err(MaterializationError::PlanConflict)
                ));
                assert_eq!(
                    restarted
                        .load(original.operation_id)
                        .await
                        .expect("query after conflict")
                        .as_ref(),
                    Some(&retained)
                );
                assert_eq!(tree.snapshot(), source_tree());
            }

            // Recover the original operation, never allocate a substitute plan.
            let recovered = restarted
                .recover(original.operation_id, recovery, &mut || async {
                    Ok::<(), MaterializationError<_, _>>(())
                })
                .await
                .expect("recover partially captured original plan")
                .expect("original journal");
            assert_eq!(recovered.plan, original);
            match recovery {
                MaterializationRecovery::Complete => {
                    assert_eq!(recovered.phase, MaterializationPhase::Applied);
                    assert_eq!(tree.snapshot(), target_tree());
                    assert_eq!(recovered.preimages[..ordinal - 1], retained.preimages);
                    #[cfg(not(target_arch = "wasm32"))]
                    assert!(recovered.custody.is_none(), "ordinary receipt copy only");
                }
                MaterializationRecovery::RollBack => {
                    assert_eq!(recovered.phase, MaterializationPhase::RolledBack);
                    assert_eq!(recovered.preimages, retained.preimages);
                    assert_eq!(recovered.applied, 0);
                    assert_eq!(recovered.restored, 0);
                    assert_eq!(tree.snapshot(), source_tree());
                    assert!(matches!(
                        restarted
                            .apply(original.clone(), &mut || async {
                                Ok::<(), MaterializationError<_, _>>(())
                            })
                            .await,
                        Err(MaterializationError::PlanConflict)
                    ));
                }
            }
            assert_eq!(
                restarted
                    .load(original.operation_id)
                    .await
                    .expect("query recovered original"),
                Some(recovered)
            );
        }
    }
}
