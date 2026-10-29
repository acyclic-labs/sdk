#[tokio::test]
async fn clone_promotion_matrix_preserves_dense_and_sparse_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();

    let extended_inline = clone_range(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"abcd")?),
        0,
        FilePayload::InlineRegular(InlineFileData::new(b"z")?),
        1,
        4,
    )
    .await?;
    let FilePayload::InlineRegular(extended_inline) = extended_inline.destination else {
        return Err("small extending clone unexpectedly promoted".into());
    };
    assert_eq!(extended_inline.as_bytes(), b"zabcd");

    let sparse_source = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b's'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let promoted_destination = clone_range(
        &store,
        sparse_source.payload,
        0,
        FilePayload::InlineRegular(InlineFileData::new(b"zzzz")?),
        4,
        4,
    )
    .await?;
    let FilePayload::InlineRegular(promoted_destination) = promoted_destination.destination else {
        return Err("dense small clone did not demote after sparse execution".into());
    };
    assert_eq!(promoted_destination.as_bytes(), b"zzzzssss");

    let sparse_destination = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'd'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let promoted_source = clone_range(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"abcd")?),
        0,
        sparse_destination.payload,
        80,
        4,
    )
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = promoted_source.destination
    else {
        return Err("clone into sparse destination unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 128);
    let encoded = AsyncObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
    )
    .await?;
    let ExtentPage::Leaf(extents) =
        decode_extent_page(&encoded, DecodeLimits::for_volume(config()), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("clone destination root was not a leaf".into());
    };
    assert!(extents.iter().any(|extent| {
        extent.offset == 80
            && extent.length == 4
            && matches!(extent.kind, ExtentKind::Content { .. })
    }));

    let same = FilePayload::InlineRegular(InlineFileData::new(b"same")?);
    let overlapping_identity = clone_range(&store, same, 0, same, 8, 4).await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = overlapping_identity.destination
    else {
        return Err("hole-bearing identity clone unexpectedly remained inline".into());
    };
    assert_eq!(logical_bytes, 12);
    let encoded = AsyncObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
    )
    .await?;
    let ExtentPage::Leaf(extents) =
        decode_extent_page(&encoded, DecodeLimits::for_volume(config()), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("identity clone destination root was not a leaf".into());
    };
    assert!(extents.iter().any(|extent| {
        extent.offset == 4 && extent.length == 4 && matches!(extent.kind, ExtentKind::Hole)
    }));
    Ok(())
}

#[tokio::test]
async fn clone_budget_and_precancellation_fail_before_visible_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let source = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);
    let destination = FilePayload::InlineRegular(InlineFileData::new(b"0000")?);
    let budget = WorkBudget {
        bytes_copied: 3,
        ..WorkBudget::UNBOUNDED
    };
    let failure = (async {
        let cancellation = &CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        apply_regular_clone_with_accounting(
            &store,
            source,
            0,
            destination,
            0,
            4,
            config(),
            &mut accounting,
            cancellation,
        )
        .await
        .map_err(|error| OperationFailure::new(project_regular_error(error), accounting.spent()))
    })
    .await
    .err()
    .ok_or("over-budget clone unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            observed: 4,
            maximum: 3,
        })
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = (async {
        let cancellation = &cancelled;
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
        .map_err(|error| OperationFailure::new(project_regular_error(error), accounting.spent()))
    })
    .await
    .err()
    .ok_or("cancelled clone unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn regular_mutation_cancellation_nonregular_clone_and_inline_budget_matrix_is_total()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = blob(&store, b"xy").await?;
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    for mutation in [
        RegularMutation::Write {
            offset: 0,
            length: 1,
            content,
            content_offset: 0,
        },
        RegularMutation::Resize { logical_bytes: 2 },
        RegularMutation::ZeroRange {
            offset: 0,
            length: 1,
            allocated: true,
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
            config(),
            WorkBudget::UNBOUNDED,
            &cancelled,
        ))
        .await
        .err()
        .ok_or("pre-cancelled regular mutation succeeded")?;
        assert!(matches!(
            failure.error,
            RegularMutationError::Storage(ObjectStoreError::Cancelled)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let typed = |kind| ObjectId {
        storage: None,
        kind,
        digest: Digest::from_bytes([91; 32]),
    };
    for payload in [
        FilePayload::Empty,
        FilePayload::Directory {
            entries: typed(ObjectKind::TreePage),
        },
        FilePayload::SymbolicLink {
            target_bytes: 1,
            target: typed(ObjectKind::Blob),
        },
        FilePayload::Device { major: 1, minor: 3 },
        FilePayload::ReparsePoint {
            payload_bytes: 1,
            payload: typed(ObjectKind::Blob),
        },
    ] {
        let failure = apply(
            &store,
            payload,
            RegularMutation::Resize { logical_bytes: 0 },
        )
        .await
        .err()
        .ok_or("non-regular payload mutation succeeded")?;
        assert!(matches!(failure.error, RegularMutationError::NotRegular));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let source_failure = clone_range(&store, FilePayload::Empty, 0, inline, 0, 1)
        .await
        .err()
        .ok_or("non-regular clone source succeeded")?;
    assert!(matches!(
        source_failure.error,
        RegularMutationError::NotRegular
    ));
    assert_eq!(*source_failure.work, WorkCounters::default());
    let destination_failure = clone_range(&store, inline, 0, FilePayload::Empty, 0, 1)
        .await
        .err()
        .ok_or("non-regular clone destination succeeded")?;
    assert!(matches!(
        destination_failure.error,
        RegularMutationError::NotRegular
    ));
    assert_eq!(*destination_failure.work, WorkCounters::default());

    let range_failure = apply(
        &store,
        inline,
        RegularMutation::Write {
            offset: 0,
            length: 2,
            content,
            content_offset: 1,
        },
    )
    .await
    .err()
    .ok_or("out-of-range blob write succeeded")?;
    assert!(matches!(
        range_failure.error,
        RegularMutationError::BlobRead(BlobReadError::InvalidRange)
    ));
    assert!(range_failure.work.backend_read_operations > 0);

    let baseline = apply(
        &store,
        inline,
        RegularMutation::Write {
            offset: 1,
            length: 1,
            content,
            content_offset: 0,
        },
    )
    .await?;
    for (counter, maximum) in [
        ("bytes_copied", baseline.work.bytes_copied - 1),
        ("object_bytes_read", baseline.work.object_bytes_read - 1),
    ] {
        let mut budget = WorkBudget::UNBOUNDED;
        match counter {
            "bytes_copied" => budget.bytes_copied = maximum,
            "object_bytes_read" => budget.object_bytes_read = maximum,
            _ => return Err("unknown budget counter".into()),
        }
        let failure = (apply_regular_mutation_async(
            &store,
            inline,
            RegularMutation::Write {
                offset: 1,
                length: 1,
                content,
                content_offset: 0,
            },
            config(),
            budget,
            &CancellationToken::new(),
        ))
        .await
        .err()
        .ok_or("under-budget inline write succeeded")?;
        match counter {
            "bytes_copied" => assert!(matches!(
                failure.error,
                RegularMutationError::Work(WorkError::BudgetExceeded {
                    counter: "bytes_copied",
                    maximum: actual_maximum,
                    ..
                }) if actual_maximum == maximum
            )),
            "object_bytes_read" => match failure.error {
                RegularMutationError::Work(WorkError::BudgetExceeded {
                    counter,
                    observed,
                    maximum: actual_maximum,
                }) => {
                    assert_eq!(counter, "object_bytes_read");
                    assert!(observed > actual_maximum);
                    assert!(actual_maximum < baseline.work.object_bytes_read);
                }
                error => {
                    return Err(format!("unexpected object-read budget failure: {error:#?}").into());
                }
            },
            _ => return Err("unknown budget counter".into()),
        }
        assert!(failure.work.bytes_copied <= budget.bytes_copied);
        assert!(failure.work.object_bytes_read <= budget.object_bytes_read);
    }
    Ok(())
}

#[tokio::test]
async fn sparse_clone_preserves_holes_and_destination_size()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let source = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b's'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let destination = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'd'; 64])?),
        RegularMutation::Resize { logical_bytes: 160 },
    )
    .await?;
    let cloned = clone_range(&store, source.payload, 0, destination.payload, 16, 128).await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = cloned.destination
    else {
        return Err("large sparse clone unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 160);
    assert!(cloned.work.backend_write_operations > 0);
    let encoded = AsyncObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
    )
    .await?;
    let ExtentPage::Leaf(values) =
        decode_extent_page(&encoded, DecodeLimits::for_volume(config()), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("sparse clone root was not a leaf".into());
    };
    assert_eq!(values.len(), 3);
    assert_eq!((values[0].offset, values[0].length), (0, 16));
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert_eq!((values[1].offset, values[1].length), (16, 64));
    assert!(matches!(values[1].kind, ExtentKind::Content { .. }));
    assert_eq!((values[2].offset, values[2].length), (80, 80));
    assert!(matches!(values[2].kind, ExtentKind::Hole));
    Ok(())
}
