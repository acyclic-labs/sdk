#[tokio::test]
async fn rejection_precedes_storage_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let failure = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"a")?),
        RegularMutation::Write {
            offset: 0,
            length: 1,
            content: ObjectId {
                kind: ObjectKind::Metadata,
                digest: object_digest(b"wrong"),
            },
            content_offset: 0,
        },
    )
    .await
    .err()
    .ok_or("wrong content kind unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::WrongContentKind
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    let clone_failure = clone_range(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"a")?),
        0,
        FilePayload::InlineRegular(InlineFileData::new(b"b")?),
        0,
        2,
    )
    .await
    .err()
    .ok_or("out-of-range clone unexpectedly succeeded")?;
    assert!(matches!(
        clone_failure.error,
        RegularMutationError::SourceRangeOutOfBounds
    ));
    assert_eq!(*clone_failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn dense_volume_rejects_sparse_semantics_before_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = blob(&store, b"XY").await?;
    let mut dense = config();
    dense.sparse_files = false;
    let cancellation = CancellationToken::new();
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);

    let replaced = (apply_regular_mutation_async(
        &store,
        inline,
        RegularMutation::Write {
            offset: 1,
            length: 2,
            content,
            content_offset: 0,
        },
        dense,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let FilePayload::InlineRegular(replaced) = replaced.payload else {
        return Err("in-range dense write did not remain inline".into());
    };
    assert_eq!(replaced.as_bytes(), b"aXYd");

    for mutation in [
        RegularMutation::Write {
            offset: 5,
            length: 1,
            content,
            content_offset: 0,
        },
        RegularMutation::Resize { logical_bytes: 5 },
        RegularMutation::ZeroRange {
            offset: 0,
            length: 1,
            allocated: false,
            extend: false,
        },
        RegularMutation::Preallocate {
            offset: 0,
            length: 1,
            keep_size: true,
        },
    ] {
        let failure = (apply_regular_mutation_async(
            &store,
            inline,
            mutation,
            dense,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("sparse semantics unexpectedly succeeded in a dense volume")?;
        assert!(matches!(
            failure.error,
            RegularMutationError::SparseSemanticsDisabled
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let clone_failure = (async {
        let cancellation = &cancellation;
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
        apply_regular_clone_with_accounting(
            &store,
            inline,
            0,
            inline,
            0,
            1,
            dense,
            &mut accounting,
            cancellation,
        )
        .await
        .map_err(|error| OperationFailure::new(project_regular_error(error), accounting.spent()))
    })
    .await
    .err()
    .ok_or("clone unexpectedly succeeded in a dense volume")?;
    assert!(matches!(
        clone_failure.error,
        RegularMutationError::SparseSemanticsDisabled
    ));
    assert_eq!(*clone_failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn malformed_ranges_nonregular_payloads_and_semantic_noops_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let content = blob(&store, b"x").await?;
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);
    for mutation in [
        RegularMutation::Write {
            offset: 0,
            length: 0,
            content,
            content_offset: 0,
        },
        RegularMutation::Write {
            offset: u64::MAX,
            length: 1,
            content,
            content_offset: 0,
        },
        RegularMutation::Write {
            offset: 0,
            length: 1,
            content,
            content_offset: u64::MAX,
        },
        RegularMutation::ZeroRange {
            offset: 0,
            length: 0,
            allocated: false,
            extend: false,
        },
        RegularMutation::ZeroRange {
            offset: u64::MAX,
            length: 1,
            allocated: false,
            extend: false,
        },
        RegularMutation::Preallocate {
            offset: 0,
            length: 0,
            keep_size: true,
        },
        RegularMutation::Preallocate {
            offset: u64::MAX,
            length: 1,
            keep_size: false,
        },
    ] {
        let failure = (apply_regular_mutation_async(
            &store,
            inline,
            mutation,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("malformed range unexpectedly succeeded")?;
        assert!(matches!(failure.error, RegularMutationError::RangeOverflow));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let nonregular = (apply_regular_mutation_async(
        &store,
        FilePayload::Empty,
        RegularMutation::Resize { logical_bytes: 0 },
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("nonregular mutation unexpectedly succeeded")?;
    assert!(matches!(nonregular.error, RegularMutationError::NotRegular));
    assert_eq!(*nonregular.work, WorkCounters::default());
    for (source, destination) in [(FilePayload::Empty, inline), (inline, FilePayload::Empty)] {
        let failure = (async {
            let cancellation = &cancellation;
            let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
            apply_regular_clone_with_accounting(
                &store,
                source,
                0,
                destination,
                0,
                1,
                config(),
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(|error| {
                OperationFailure::new(project_regular_error(error), accounting.spent())
            })
        })
        .await
        .err()
        .ok_or("nonregular clone unexpectedly succeeded")?;
        assert!(matches!(failure.error, RegularMutationError::NotRegular));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    for (source_offset, destination_offset, length) in
        [(u64::MAX, 0, 1), (0, u64::MAX, 1), (0, 0, 0)]
    {
        let failure = (async {
            let cancellation = &cancellation;
            let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
            apply_regular_clone_with_accounting(
                &store,
                inline,
                source_offset,
                inline,
                destination_offset,
                length,
                config(),
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(|error| {
                OperationFailure::new(project_regular_error(error), accounting.spent())
            })
        })
        .await
        .err()
        .ok_or("invalid clone unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            RegularMutationError::RangeOverflow | RegularMutationError::SourceRangeOutOfBounds
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let zero_noop = apply(
        &store,
        inline,
        RegularMutation::ZeroRange {
            offset: 4,
            length: 2,
            allocated: true,
            extend: false,
        },
    )
    .await?;
    assert_eq!(zero_noop.payload, inline);
    assert_eq!(zero_noop.work, WorkCounters::default());
    let preallocate_noop = apply(
        &store,
        inline,
        RegularMutation::Preallocate {
            offset: 1,
            length: 2,
            keep_size: true,
        },
    )
    .await?;
    assert_eq!(preallocate_noop.payload, inline);
    assert_eq!(preallocate_noop.work, WorkCounters::default());

    let promoted = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'p'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let contained = apply(
        &store,
        promoted.payload,
        RegularMutation::Preallocate {
            offset: 0,
            length: 32,
            keep_size: true,
        },
    )
    .await?;
    assert_eq!(contained.payload, promoted.payload);
    assert_eq!(contained.work.backend_write_operations, 0);
    let empty = apply(
        &store,
        promoted.payload,
        RegularMutation::Resize { logical_bytes: 0 },
    )
    .await?;
    let FilePayload::InlineRegular(data) = empty.payload else {
        return Err("zero-length sparse resize did not demote".into());
    };
    assert!(data.as_bytes().is_empty());
    Ok(())
}
