#[tokio::test]
async fn writes_at_sixty_three_and_sixty_four_bytes_remain_inline()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = blob(&store, b"xy").await?;
    for (initial, offset, expected_last) in [(63_usize, 62_u64, b'x'), (63, 63, b'y')] {
        let receipt = apply(
            &store,
            FilePayload::InlineRegular(InlineFileData::new(&vec![b'a'; initial])?),
            RegularMutation::Write {
                offset,
                length: 1,
                content,
                content_offset: u64::from(expected_last == b'y'),
            },
        )
        .await?;
        let FilePayload::InlineRegular(data) = receipt.payload else {
            return Err("small write unexpectedly promoted inline data".into());
        };
        assert_eq!(
            data.as_bytes().len(),
            usize::try_from(offset + 1)?.max(initial)
        );
        assert_eq!(data.as_bytes()[usize::try_from(offset)?], expected_last);
    }
    Ok(())
}

#[tokio::test]
async fn sixty_fifth_byte_promotes_to_sparse_without_materializing_a_hole()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = blob(&store, b"z").await?;
    let receipt = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Write {
            offset: 64,
            length: 1,
            content,
            content_offset: 0,
        },
    )
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = receipt.payload
    else {
        return Err("sixty-fifth byte did not promote inline data".into());
    };
    assert_eq!(logical_bytes, 65);
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
        return Err("promoted extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 2);
    assert_eq!((values[0].offset, values[0].length), (0, 64));
    assert_eq!((values[1].offset, values[1].length), (64, 1));
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert!(matches!(values[1].kind, ExtentKind::Content { .. }));

    let demoted = apply(
        &store,
        receipt.payload,
        RegularMutation::Resize { logical_bytes: 64 },
    )
    .await?;
    let FilePayload::InlineRegular(data) = demoted.payload else {
        return Err("dense sixty-four-byte shrink did not demote inline".into());
    };
    assert_eq!(data.as_bytes(), &[b'a'; 64]);

    let sparse_gap = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 63])?),
        RegularMutation::Write {
            offset: 64,
            length: 1,
            content,
            content_offset: 0,
        },
    )
    .await?;
    let still_sparse = apply(
        &store,
        sparse_gap.payload,
        RegularMutation::Resize { logical_bytes: 64 },
    )
    .await?;
    assert!(matches!(still_sparse.payload, FilePayload::Regular { .. }));

    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let promoted = (promote_inline(
        &store,
        InlineFileData::new(&[b'q'; 64])?,
        config(),
        &mut accounting,
        &cancellation,
    ))
    .await?;
    let promoted_payload = FilePayload::Regular {
        logical_bytes: promoted.logical_bytes,
        extents: promoted.extents,
    };
    let clone_demoted = clone_range(&store, promoted_payload, 0, promoted_payload, 4, 4).await?;
    let FilePayload::InlineRegular(clone_data) = clone_demoted.destination else {
        return Err("dense small sparse clone did not demote inline".into());
    };
    assert_eq!(clone_data.as_bytes(), &[b'q'; 64]);
    Ok(())
}

#[tokio::test]
async fn empty_inline_growth_promotes_directly_to_one_sparse_hole()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let receipt = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = receipt.payload
    else {
        return Err("empty inline growth unexpectedly remained inline".into());
    };
    assert_eq!(logical_bytes, 128);
    assert_eq!(receipt.work.source_bytes_read, 0);
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
        return Err("empty growth extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 1);
    assert_eq!((values[0].offset, values[0].length), (0, 128));
    assert!(matches!(values[0].kind, ExtentKind::Hole));
    Ok(())
}

#[tokio::test]
async fn inline_growth_within_threshold_preserves_trailing_hole()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let receipt = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"a")?),
        RegularMutation::Resize { logical_bytes: 64 },
    )
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = receipt.payload
    else {
        return Err("inline growth with a trailing hole demoted incorrectly".into());
    };
    assert_eq!(logical_bytes, 64);
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
        return Err("grown extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 2);
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert!(matches!(values[1].kind, ExtentKind::Hole));
    assert_eq!((values[1].offset, values[1].length), (1, 63));
    Ok(())
}

#[tokio::test]
async fn inline_shrink_and_allocated_zero_are_allocation_free()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let shrunk = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[7; 64])?),
        RegularMutation::Resize { logical_bytes: 63 },
    )
    .await?;
    assert_eq!(shrunk.work, WorkCounters::default());
    let zeroed = apply(
        &store,
        shrunk.payload,
        RegularMutation::ZeroRange {
            offset: 2,
            length: 4,
            allocated: true,
            extend: false,
        },
    )
    .await?;
    assert_eq!(zeroed.work.allocation_operations, 0);
    assert_eq!(zeroed.work.bytes_copied, 4);
    let FilePayload::InlineRegular(data) = zeroed.payload else {
        return Err("allocated zero unexpectedly promoted inline data".into());
    };
    assert_eq!(&data.as_bytes()[2..6], &[0; 4]);
    let cloned = clone_range(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"abcdef")?),
        0,
        FilePayload::InlineRegular(InlineFileData::new(b"000000")?),
        1,
        4,
    )
    .await?;
    assert_eq!(cloned.work.bytes_copied, 4);
    assert_eq!(cloned.work.allocation_operations, 0);
    let FilePayload::InlineRegular(cloned_data) = cloned.destination else {
        return Err("small clone unexpectedly promoted inline data".into());
    };
    assert_eq!(cloned_data.as_bytes(), b"0abcd0");
    Ok(())
}

#[tokio::test]
async fn inline_zero_extension_preserves_exact_sparse_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"a")?);
    let allocated = apply(
        &store,
        inline,
        RegularMutation::ZeroRange {
            offset: 1,
            length: 3,
            allocated: true,
            extend: true,
        },
    )
    .await?;
    let FilePayload::InlineRegular(data) = allocated.payload else {
        return Err("contiguous allocated-zero extension did not remain inline".into());
    };
    assert_eq!(data.as_bytes(), b"a\0\0\0");
    assert_eq!(allocated.work.bytes_copied, 3);

    for (offset, allocated) in [(1_u64, false), (3, true)] {
        let promoted = apply(
            &store,
            inline,
            RegularMutation::ZeroRange {
                offset,
                length: 2,
                allocated,
                extend: true,
            },
        )
        .await?;
        let FilePayload::Regular {
            logical_bytes,
            extents,
        } = promoted.payload
        else {
            return Err("hole-bearing zero extension remained inline".into());
        };
        assert_eq!(logical_bytes, offset + 2);
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
            return Err("promoted extent root was not a leaf".into());
        };
        assert!(
            values
                .iter()
                .any(|extent| matches!(extent.kind, ExtentKind::Hole))
        );
        if allocated {
            assert!(
                values
                    .iter()
                    .any(|extent| matches!(extent.kind, ExtentKind::AllocatedZero))
            );
        }
    }

    let oversized = apply(
        &store,
        inline,
        RegularMutation::ZeroRange {
            offset: 1,
            length: 64,
            allocated: true,
            extend: true,
        },
    )
    .await?;
    assert!(matches!(oversized.payload, FilePayload::Regular { .. }));
    Ok(())
}

#[tokio::test]
async fn hole_bearing_sparse_shrink_attempts_inline_demotion_once()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let with_hole = apply(
        &store,
        sparse.payload,
        RegularMutation::ZeroRange {
            offset: 16,
            length: 16,
            allocated: false,
            extend: false,
        },
    )
    .await?;
    let shrunk = apply(
        &store,
        with_hole.payload,
        RegularMutation::Resize { logical_bytes: 64 },
    )
    .await?;
    let FilePayload::Regular { logical_bytes, .. } = shrunk.payload else {
        return Err("hole-bearing sparse file unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 64);
    assert_eq!(shrunk.work.page_reads, 2);
    assert_eq!(shrunk.work.backend_read_operations, 2);
    Ok(())
}
