use super::*;

#[tokio::test]
async fn identity_copy_refusal_preserves_prior_planning_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = 0;
    for mutation in [
        Mutation::File {
            file_id: FileId::from_bytes([2; 16]),
            mutation: FileMutation::Resize { logical_bytes: 128 },
        },
        Mutation::CloneFileRange {
            source_file_id: FileId::from_bytes([2; 16]),
            source_offset: 0,
            destination_file_id: FileId::from_bytes([2; 16]),
            destination_offset: 0,
            length: 1,
        },
    ] {
        let failure = apply_generation_mutations(
            &store,
            &fixture.generation,
            vec![mutation],
            config(),
            budget,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("copy-limited regular mutation unexpectedly succeeded")?;
        assert!(
            matches!(
                &failure.error,
                GenerationMutationError::Regular(
                    RegularMutationError::Work(WorkError::BudgetExceeded {
                        counter: "bytes_copied",
                        observed: 1..,
                        maximum: 0,
                    }) | RegularMutationError::Extent(ExtentMutationError::Work(
                        WorkError::BudgetExceeded {
                            counter: "bytes_copied",
                            observed: 1..,
                            maximum: 0,
                        }
                    ))
                ) | GenerationMutationError::FileRecord(super::super::FileRecordReadError::Work(
                    WorkError::BudgetExceeded {
                        counter: "bytes_copied",
                        observed: 1..,
                        maximum: 0,
                    }
                ))
            ),
            "unexpected nested regular failure: {:?}",
            failure.error
        );
        assert!(failure.work.items_examined > 0);
        assert_eq!(failure.work.bytes_copied, 0);
    }
    Ok(())
}

#[tokio::test]
async fn ordered_path_and_identity_regular_mutations_share_one_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let file_id = FileId::from_bytes([2; 16]);
    let receipt = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::ValidateRegular {
                path: path(&["a"])?,
            },
            Mutation::Resize {
                path: path(&["a"])?,
                logical_bytes: 128,
            },
            Mutation::Preallocate {
                path: path(&["a"])?,
                offset: 8,
                length: 56,
                keep_size: true,
            },
            Mutation::ZeroRange {
                path: path(&["a"])?,
                offset: 0,
                length: 1,
                allocated: true,
                extend: false,
            },
            Mutation::File {
                file_id,
                mutation: FileMutation::SetMetadata {
                    metadata: fixture.changed_metadata,
                },
            },
            Mutation::File {
                file_id,
                mutation: FileMutation::ValidateRegular,
            },
            Mutation::File {
                file_id,
                mutation: FileMutation::Preallocate {
                    offset: 64,
                    length: 64,
                    keep_size: true,
                },
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let record = lookup_file_record(
        &store,
        receipt.root.file_table,
        file_id,
        DecodeLimits::for_volume(config()),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .value
    .ok_or("mutated file identity disappeared")?;
    assert_eq!(record.metadata, fixture.changed_metadata);
    let FilePayload::Regular { logical_bytes, .. } = record.payload else {
        return Err("ordered sparse operations did not preserve sparse payload".into());
    };
    assert_eq!(logical_bytes, 128);
    assert!(receipt.work.backend_write_operations > 0);
    assert_eq!(
        inline_at(&store, &fixture.generation, "a").await?,
        b"payload"
    );
    Ok(())
}

#[tokio::test]
async fn every_sparse_operation_variant_rejects_before_work_when_sparse_files_are_disabled()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let file_id = FileId::from_bytes([2; 16]);
    let operations = [
        Mutation::Preallocate {
            path: path(&["a"])?,
            offset: 0,
            length: 1,
            keep_size: true,
        },
        Mutation::CloneRange {
            source: path(&["a"])?,
            source_offset: 0,
            destination: path(&["a"])?,
            destination_offset: 0,
            length: 1,
        },
        Mutation::File {
            file_id,
            mutation: FileMutation::ZeroRange {
                offset: 0,
                length: 1,
                allocated: false,
                extend: false,
            },
        },
        Mutation::File {
            file_id,
            mutation: FileMutation::Preallocate {
                offset: 0,
                length: 1,
                keep_size: true,
            },
        },
        Mutation::CloneFileRange {
            source_file_id: file_id,
            source_offset: 0,
            destination_file_id: file_id,
            destination_offset: 0,
            length: 1,
        },
    ];
    let mut dense = config();
    dense.sparse_files = false;
    for operation in operations {
        let failure = apply_generation_mutations(
            &store,
            &fixture.generation,
            vec![operation],
            dense,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("disabled sparse operation unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            GenerationMutationError::UnsupportedSparseMutation
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    Ok(())
}

#[tokio::test]
async fn empty_generation_batch_and_precancellation_reject_before_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let empty = apply_generation_mutations(
        &store,
        &fixture.generation,
        Vec::new(),
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("empty generation batch unexpectedly succeeded")?;
    assert!(matches!(
        empty.error,
        GenerationMutationError::Plan(MutationPlanError::Empty)
    ));
    assert_eq!(*empty.work, WorkCounters::default());

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::ValidateRegular {
            path: path(&["a"])?,
        }],
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .err()
    .ok_or("pre-cancelled generation mutation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::Path(PathLookupError::Tree(TreeReadError::Cancelled))
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}
