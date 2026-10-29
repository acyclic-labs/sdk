use super::*;

#[test]
fn transaction_construction_rejects_every_contradictory_sparse_seed()
-> Result<(), Box<dyn std::error::Error>> {
    let metadata = ObjectId {
        storage: None,
        kind: ObjectKind::Metadata,
        digest: Digest::ZERO,
    };
    let first_id = FileId::from_bytes([101; 16]);
    let second_id = FileId::from_bytes([102; 16]);
    let first = regular(first_id, metadata)?;
    let second = regular(second_id, metadata)?;
    let limits = config().limits;

    let one = MutationPlan::compile(
        vec![Mutation::Create {
            path: path(&["one"])?,
            record: first,
        }],
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    fn fixture_state(
        plan: &MutationPlan,
        lookups: &InitialLookups,
    ) -> Result<TransactionState, crate::FsError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut allocation = KernelOwnedAllocation::Standalone;
        TransactionState::new(
            plan,
            lookups,
            config(),
            &mut MutationLeaf {
                accounting: &mut accounting,
                allocation: &mut allocation,
            },
        )
    }
    let failure = fixture_state(
        &one,
        &InitialLookups {
            paths: Owned {
                value: Vec::new(),
                allocation: KernelOwnedAllocation::Standalone,
            },
            identities: Owned {
                value: Vec::new(),
                allocation: KernelOwnedAllocation::Standalone,
            },
        },
    )
    .err()
    .ok_or("lookup cardinality mismatch unexpectedly succeeded")?;
    assert!(matches!(
        generation_error(failure),
        GenerationMutationError::InconsistentState
    ));

    let duplicate_path = MutationPlan::compile(
        vec![
            Mutation::ValidateRegular {
                path: path(&["same"])?,
            },
            Mutation::ValidateRegular {
                path: path(&["same"])?,
            },
        ],
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    let failure = fixture_state(
        &duplicate_path,
        &InitialLookups {
            paths: Owned {
                value: vec![
                    PathBatchEntry {
                        record: Some(first),
                        parent: None,
                        resolved_components: 1,
                    },
                    PathBatchEntry {
                        record: Some(second),
                        parent: None,
                        resolved_components: 1,
                    },
                ],
                allocation: KernelOwnedAllocation::Standalone,
            },
            identities: Owned {
                value: Vec::new(),
                allocation: KernelOwnedAllocation::Standalone,
            },
        },
    )
    .err()
    .ok_or("contradictory duplicate path lookup unexpectedly succeeded")?;
    assert!(matches!(
        generation_error(failure),
        GenerationMutationError::InconsistentState
    ));

    let duplicate_created = MutationPlan::compile(
        vec![
            Mutation::Create {
                path: path(&["first"])?,
                record: first,
            },
            Mutation::Create {
                path: path(&["second"])?,
                record: first,
            },
        ],
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    let absent = PathBatchEntry {
        record: None,
        parent: None,
        resolved_components: 0,
    };
    let failure = fixture_state(
        &duplicate_created,
        &InitialLookups {
            paths: Owned {
                value: vec![absent, absent],
                allocation: KernelOwnedAllocation::Standalone,
            },
            identities: Owned {
                value: Vec::new(),
                allocation: KernelOwnedAllocation::Standalone,
            },
        },
    )
    .err()
    .ok_or("duplicate created identity unexpectedly succeeded")?;
    assert!(matches!(
        generation_error(failure),
        GenerationMutationError::FileIdentityAlreadyExists
    ));

    let distinct_paths = MutationPlan::compile(
        vec![
            Mutation::ValidateRegular {
                path: path(&["left"])?,
            },
            Mutation::ValidateRegular {
                path: path(&["right"])?,
            },
        ],
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    let conflicting_first = FileRecord {
        metadata: ObjectId {
            storage: None,
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([1; 32]),
        },
        ..first
    };
    let failure = fixture_state(
        &distinct_paths,
        &InitialLookups {
            paths: Owned {
                value: vec![
                    PathBatchEntry {
                        record: Some(first),
                        parent: None,
                        resolved_components: 1,
                    },
                    PathBatchEntry {
                        record: Some(conflicting_first),
                        parent: None,
                        resolved_components: 1,
                    },
                ],
                allocation: KernelOwnedAllocation::Standalone,
            },
            identities: Owned {
                value: Vec::new(),
                allocation: KernelOwnedAllocation::Standalone,
            },
        },
    )
    .err()
    .ok_or("contradictory base records unexpectedly succeeded")?;
    assert!(matches!(
        generation_error(failure),
        GenerationMutationError::FileIdentityAlreadyExists
    ));

    let failure = fixture_state(
        &one,
        &InitialLookups {
            paths: Owned {
                value: vec![PathBatchEntry {
                    record: Some(first),
                    parent: None,
                    resolved_components: 1,
                }],
                allocation: KernelOwnedAllocation::Standalone,
            },
            identities: Owned {
                value: Vec::new(),
                allocation: KernelOwnedAllocation::Standalone,
            },
        },
    )
    .err()
    .ok_or("base and created identity collision unexpectedly succeeded")?;
    assert!(matches!(
        generation_error(failure),
        GenerationMutationError::FileIdentityAlreadyExists
    ));
    Ok(())
}

#[test]
#[cfg(target_pointer_width = "64")]
fn impossible_transaction_vector_allocation_releases_its_complete_claim()
-> Result<(), Box<dyn std::error::Error>> {
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::borrowed_standalone(
        &mut allocations,
        &mut work,
        WorkBudget::UNBOUNDED,
        &cancellation,
    );
    let failure = reserve_owned::<u8>(usize::MAX, &mut accounting)
        .err()
        .ok_or("impossible transaction vector allocation succeeded")?;
    assert!(matches!(
        generation_error(failure),
        GenerationMutationError::AllocationFailed
    ));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}
