#[tokio::test]
async fn preallocation_preserves_content_allocates_only_holes_and_handles_eof_explicitly()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let allocated = apply(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 32,
            length: 64,
            keep_size: true,
        },
    )
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = allocated.payload
    else {
        return Err("preallocation unexpectedly produced inline data".into());
    };
    assert_eq!(logical_bytes, 128);
    let encoded = {
        let __object_args = (
            extents,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::read(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    let ExtentPage::Leaf(values) =
        decode_extent_page(&encoded, DecodeLimits::for_volume(config()), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("preallocated extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 3);
    assert_eq!((values[0].offset, values[0].length), (0, 64));
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert_eq!((values[1].offset, values[1].length), (64, 32));
    assert!(matches!(values[1].kind, ExtentKind::AllocatedZero));
    assert_eq!((values[2].offset, values[2].length), (96, 32));
    assert!(matches!(values[2].kind, ExtentKind::Hole));

    let extended = apply(
        &store,
        allocated.payload,
        RegularMutation::Preallocate {
            offset: 120,
            length: 40,
            keep_size: false,
        },
    )
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = extended.payload
    else {
        return Err("extending preallocation unexpectedly produced inline data".into());
    };
    assert_eq!(logical_bytes, 160);
    let encoded = {
        let __object_args = (
            extents,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::read(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    let ExtentPage::Leaf(values) =
        decode_extent_page(&encoded, DecodeLimits::for_volume(config()), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("extended extent root was not a leaf".into());
    };
    assert!(values.iter().any(|extent| {
        extent.offset == 120
            && extent.length == 40
            && matches!(extent.kind, ExtentKind::AllocatedZero)
    }));

    let failure = apply(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 120,
            length: 40,
            keep_size: true,
        },
    )
    .await
    .err()
    .ok_or("unrepresentable keep-size allocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::KeepSizeBeyondEofUnsupported
    ));
    Ok(())
}

#[tokio::test]
async fn extending_preallocation_merges_the_eof_hole_and_extension_into_one_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let mut constrained = config();
    constrained.limits.maximum_mutations_per_batch = 1;
    let allocated = (apply_regular_mutation_async(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 64,
            length: 128,
            keep_size: false,
        },
        constrained,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = allocated.payload
    else {
        return Err("extending preallocation unexpectedly produced inline data".into());
    };
    assert_eq!(logical_bytes, 192);
    let encoded = {
        let __object_args = (
            extents,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::read(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    let ExtentPage::Leaf(values) =
        decode_extent_page(&encoded, DecodeLimits::for_volume(config()), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("allocated extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 2);
    assert_eq!((values[1].offset, values[1].length), (64, 128));
    assert!(matches!(values[1].kind, ExtentKind::AllocatedZero));
    Ok(())
}

#[tokio::test]
async fn preallocation_budget_rejections_report_only_completed_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let mut allocation_budget = WorkBudget::UNBOUNDED;
    allocation_budget.allocation_operations = 0;
    let failure = (apply_regular_mutation_async(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 128,
            length: 64,
            keep_size: false,
        },
        config(),
        allocation_budget,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("allocation-limited preallocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Work(WorkError::BudgetExceeded {
            counter: "allocation_operations",
            observed: 1,
            maximum: 0,
        })
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'b'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let mut scan_budget = WorkBudget::UNBOUNDED;
    scan_budget.items_examined = 5;
    let failure = (apply_regular_mutation_async(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 64,
            length: 32,
            keep_size: true,
        },
        config(),
        scan_budget,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("scan-limited preallocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Work(WorkError::BudgetExceeded {
            counter: "items_examined",
            observed: 6,
            maximum: 5,
        })
    ));
    assert_eq!(failure.work.items_examined, 5);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn inline_preallocation_handles_keep_size_and_growth()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);
    let failure = apply(
        &store,
        inline,
        RegularMutation::Preallocate {
            offset: 4,
            length: 1,
            keep_size: true,
        },
    )
    .await
    .err()
    .ok_or("keep-size preallocation beyond EOF unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::KeepSizeBeyondEofUnsupported
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let grown = apply(
        &store,
        inline,
        RegularMutation::Preallocate {
            offset: 4,
            length: 2,
            keep_size: false,
        },
    )
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = grown.payload
    else {
        return Err("inline extending preallocation did not promote".into());
    };
    assert_eq!(logical_bytes, 6);
    let encoded = {
        let __object_args = (
            extents,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::read(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    let ExtentPage::Leaf(values) =
        decode_extent_page(&encoded, DecodeLimits::for_volume(config()), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("preallocation root was not a leaf".into());
    };
    assert!(matches!(
        values.last().map(|extent| extent.kind.clone()),
        Some(ExtentKind::AllocatedZero)
    ));
    assert_eq!(
        values.last().map(|extent| (extent.offset, extent.length)),
        Some((4, 2))
    );
    Ok(())
}

#[tokio::test]
async fn preallocation_rejects_fragmented_ranges_at_the_configured_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 256 },
    )
    .await?;
    let content = blob(&store, b"x").await?;
    let fragmented = apply(
        &store,
        sparse.payload,
        RegularMutation::Write {
            offset: 128,
            length: 1,
            content,
            content_offset: 0,
        },
    )
    .await?;
    let mut constrained = config();
    constrained.limits.maximum_mutations_per_batch = 1;
    let failure = (apply_regular_mutation_async(
        &store,
        fragmented.payload,
        RegularMutation::Preallocate {
            offset: 0,
            length: 256,
            keep_size: false,
        },
        constrained,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("over-limit preallocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Range(ExtentReadError::TooManySpans)
    ));
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn preallocation_allocates_multiple_holes_and_a_nonadjacent_extension_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 129 },
    )
    .await?;
    let tail = blob(&store, b"z").await?;
    let fragmented = apply(
        &store,
        sparse.payload,
        RegularMutation::Write {
            offset: 128,
            length: 1,
            content: tail,
            content_offset: 0,
        },
    )
    .await?;
    let allocated = apply(
        &store,
        fragmented.payload,
        RegularMutation::Preallocate {
            offset: 64,
            length: 128,
            keep_size: false,
        },
    )
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = allocated.payload
    else {
        return Err("multi-region preallocation unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 192);
    let encoded = {
        let __object_args = (
            extents,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::read(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    let ExtentPage::Leaf(values) =
        decode_extent_page(&encoded, DecodeLimits::for_volume(config()), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("multi-region preallocation root was not a leaf".into());
    };
    assert_eq!(values.len(), 4);
    assert_eq!(
        values
            .iter()
            .map(|extent| (extent.offset, extent.length))
            .collect::<Vec<_>>(),
        vec![(0, 64), (64, 64), (128, 1), (129, 63)]
    );
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert!(matches!(values[1].kind, ExtentKind::AllocatedZero));
    assert!(matches!(values[2].kind, ExtentKind::Content { .. }));
    assert!(matches!(values[3].kind, ExtentKind::AllocatedZero));
    assert!(allocated.work.backend_write_operations > 0);
    Ok(())
}
