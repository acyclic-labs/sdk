use super::*;
use acyclic_fs::WorkCounters;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Only an Applied record for this exact root and generation transition
    // suppresses native replay. Every other phase must continue recovery.
    #[test]
    fn completion_requires_exact_original_root_and_transition(
        root in any::<[u8; 16]>(), from in any::<[u8; 32]>(), to in any::<[u8; 32]>(),
        phase in 0usize..6, change in 0usize..4,
    ) {
        use acyclic_fs::{Digest, GenerationId, MaterializationIntent, MaterializationJournal, MaterializationPhase, MaterializationPlan, NativeRootIdentity};
        let from = GenerationId::new(Digest::from_bytes(from));
        let to = GenerationId::new(Digest::from_bytes(to));
        let phases = [MaterializationPhase::Preparing, MaterializationPhase::Prepared, MaterializationPhase::Applying, MaterializationPhase::Applied, MaterializationPhase::RollingBack, MaterializationPhase::RolledBack];
        let journal = MaterializationJournal {
            version: 1, revision: 1,
            plan: MaterializationPlan { operation_id: OperationId::from_bytes([1; 16]), intent: MaterializationIntent::GenerationTransition { from, to }, edits: Vec::new() },
            root_identity: Some(root), preparation_identity: None, preparation_progress: None,
            preimages: Vec::new(), phase: phases[phase], applied: 0, restored: 0, work: WorkCounters::UNCHARGED, custody: None,
        };
        let mut actual_root = root;
        let mutate = |id: GenerationId| { let mut bytes = id.digest().into_bytes(); bytes[0] ^= 1; GenerationId::new(Digest::from_bytes(bytes)) };
        if change == 1 { actual_root[0] ^= 1; }
        let result = completed(&journal, NativeRootIdentity::from_bytes(actual_root), if change == 2 { mutate(from) } else { from }, if change == 3 { mutate(to) } else { to });
        prop_assert_eq!(result.is_ok(), change == 0);
        if let Ok(applied) = result { prop_assert_eq!(applied, phase == 3); }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // For every ownership-loss point during preparation, no prepared value
    // reaches publication unless the owner still holds its fence afterward.
    #[test]
    fn merge_preparation_rechecks_ownership_with_negative_control(
        lose_owner in any::<bool>(), phase in 0usize..4,
    ) {
        let run = |broken| {
            let owned = std::cell::Cell::new(true);
            let mut published = false;
            let result = futures::executor::block_on(prepared(async {
                for step in 0..4 {
                    if lose_owner && phase == step { owned.set(false); }
                    std::future::ready(()).await;
                }
                Ok(())
            }, &|| std::future::ready(if broken || owned.get() { Ok(()) }
                else { Err(Error::Unauthorized("execution owner lost during merge preparation".into())) })));
            if result.is_ok() { published = true; }
            (result.is_ok(), published && !owned.get())
        };
        let (accepted, stale_publication) = run(false);
        prop_assert_eq!(accepted, !lose_owner);
        prop_assert!(!stale_publication);
        // Removing the post-preparation check exposes precisely the stale
        // publication that this property forbids.
        prop_assert_eq!(run(true).1, lose_owner);
    }
}
