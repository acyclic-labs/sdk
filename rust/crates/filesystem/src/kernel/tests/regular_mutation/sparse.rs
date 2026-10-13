#[tokio::test]
async fn sparse_nonextending_zero_clips_at_eof_and_is_inert_beyond_eof()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let clipped = apply(
        &store,
        sparse.payload,
        RegularMutation::ZeroRange {
            offset: 120,
            length: 16,
            allocated: true,
            extend: false,
        },
    )
    .await?;
    assert_eq!(payload_size(clipped.payload)?, 128);
    assert_eq!(
        &payload_kinds(&store, clipped.payload).await?[120..],
        &[PhysicalByteKind::AllocatedZero; 8]
    );

    let outside = apply(
        &store,
        clipped.payload,
        RegularMutation::ZeroRange {
            offset: 128,
            length: 8,
            allocated: true,
            extend: false,
        },
    )
    .await?;
    assert_eq!(outside.payload, clipped.payload);
    assert_eq!(outside.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn sparse_write_rewrites_only_the_target_and_preserves_the_trailing_hole()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )
    .await?;
    let replacement = blob(&store, b"WXYZWXYZ").await?;
    let written = apply(
        &store,
        sparse.payload,
        RegularMutation::Write {
            offset: 60,
            length: 8,
            content: replacement,
            content_offset: 0,
        },
    )
    .await?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = written.payload
    else {
        return Err("sparse write unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 128);
    assert!(written.work.backend_write_operations > 0);
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
        return Err("sparse write root was not a leaf".into());
    };
    assert_eq!(values.len(), 3);
    assert_eq!((values[0].offset, values[0].length), (0, 60));
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert_eq!((values[1].offset, values[1].length), (60, 8));
    assert!(matches!(
        values[1].kind,
        ExtentKind::Content { object, object_offset: 0 } if object == replacement
    ));
    assert_eq!((values[2].offset, values[2].length), (68, 60));
    assert!(matches!(values[2].kind, ExtentKind::Hole));
    Ok(())
}
