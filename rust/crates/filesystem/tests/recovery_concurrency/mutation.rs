use super::*;

#[tokio::test]
async fn restart_repairs_progress_that_lags_host_mutation() {
    let target = target_tree();
    let source = source_tree();

    for ordinal in 1..=3 {
        let forward_store = CrashJournalStore::default();
        let forward_tree = TreeBackend::source();
        let forward_plan = plan(u8::try_from(0x40 + ordinal).expect("small ordinal"));
        let forward_backend = MutationCrashBackend::after_apply(forward_tree.clone(), ordinal);
        assert!(matches!(
            JournaledMaterializer::new(forward_store.clone(), forward_backend.clone(),)
                .apply(forward_plan.clone(), &mut || async {
                    Ok::<(), MaterializationError<_, _>>(())
                })
                .await,
            Err(MaterializationError::Backend(_))
        ));
        let restarted = JournaledMaterializer::new(forward_store, forward_tree.clone());
        let retained = restarted
            .load(forward_plan.operation_id)
            .await
            .expect("query original operation after unrecorded apply")
            .expect("forward journal");
        assert_eq!(retained.plan, forward_plan);
        assert_eq!(retained.phase, MaterializationPhase::Applying);
        assert_eq!(
            retained.applied,
            u32::try_from(ordinal - 1).expect("small ordinal")
        );
        #[cfg(not(target_arch = "wasm32"))]
        assert!(retained.custody.is_none(), "ordinary receipt copy only");
        let completed = restarted
            .recover(
                forward_plan.operation_id,
                MaterializationRecovery::Complete,
                &mut || async { Ok::<(), MaterializationError<_, _>>(()) },
            )
            .await
            .expect("complete after unrecorded mutation")
            .expect("forward journal");
        assert_eq!(completed.phase, MaterializationPhase::Applied);
        assert_eq!(completed.plan, forward_plan);
        assert_eq!(completed.preimages, retained.preimages);
        assert_eq!(forward_tree.snapshot(), target);

        let rollback_store = CrashJournalStore::default();
        let rollback_tree = TreeBackend::source();
        let rollback_plan = plan(u8::try_from(0x50 + ordinal).expect("small ordinal"));
        JournaledMaterializer::new(rollback_store.clone(), rollback_tree.clone())
            .apply(rollback_plan.clone(), &mut || async {
                Ok::<(), MaterializationError<_, _>>(())
            })
            .await
            .expect("prepare rollback fixture");
        let rollback_backend = MutationCrashBackend::after_restore(rollback_tree.clone(), ordinal);
        assert!(matches!(
            JournaledMaterializer::new(rollback_store.clone(), rollback_backend.clone(),)
                .recover(
                    rollback_plan.operation_id,
                    MaterializationRecovery::RollBack,
                    &mut || async { Ok::<(), MaterializationError<_, _>>(()) },
                )
                .await,
            Err(MaterializationError::Backend(_))
        ));
        let restarted = JournaledMaterializer::new(rollback_store, rollback_tree.clone());
        let retained = restarted
            .load(rollback_plan.operation_id)
            .await
            .expect("query original operation after unrecorded restore")
            .expect("rollback journal");
        assert_eq!(retained.plan, rollback_plan);
        assert_eq!(retained.phase, MaterializationPhase::RollingBack);
        assert_eq!(
            retained.restored,
            u32::try_from(ordinal - 1).expect("small ordinal")
        );
        #[cfg(not(target_arch = "wasm32"))]
        assert!(retained.custody.is_none(), "ordinary receipt copy only");
        let rolled_back = restarted
            .recover(
                rollback_plan.operation_id,
                MaterializationRecovery::RollBack,
                &mut || async { Ok::<(), MaterializationError<_, _>>(()) },
            )
            .await
            .expect("rollback after unrecorded restoration")
            .expect("rollback journal");
        assert_eq!(rolled_back.phase, MaterializationPhase::RolledBack);
        assert_eq!(rolled_back.plan, rollback_plan);
        assert_eq!(rolled_back.preimages, retained.preimages);
        assert_eq!(rollback_tree.snapshot(), source);
    }
}

#[tokio::test]
async fn immutable_materialization_plan_rejects_conflicting_replay() {
    let store = CrashJournalStore::default();
    let backend = TreeBackend::source();
    let materializer = JournaledMaterializer::new(store, backend);
    let original = plan(0x30);
    materializer
        .apply(original.clone(), &mut || async {
            Ok::<(), MaterializationError<_, _>>(())
        })
        .await
        .expect("initial apply");
    let mut altered = original;
    altered.intent = MaterializationIntent::GenerationTransition {
        from: generation(1),
        to: generation(9),
    };
    assert!(matches!(
        materializer
            .apply(altered, &mut || async {
                Ok::<(), MaterializationError<_, _>>(())
            })
            .await,
        Err(MaterializationError::PlanConflict)
    ));
}
