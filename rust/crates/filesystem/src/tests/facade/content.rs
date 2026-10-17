use super::*;

#[tokio::test]
async fn stamped_content_change_equals_the_change_then_its_stamp_in_one_mutation() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let id = create(&mut seed, "timed", vec![b'a'; 128].into(), &cancel).await?;
    create(&mut seed, "untimed", vec![b'b'; 128].into(), &cancel).await?;
    let times = FileMetadata {
        modified_ns: MetadataField::Value(1),
        accessed_ns: MetadataField::Value(2),
        changed_ns: MetadataField::Value(3),
        ..FileMetadata::default()
    };
    seed.set_attributes(id, times, None, B, &cancel).await?;
    commit(&mut seed, &cancel).await?;
    let mut stamped = volume
        .checkout(GenerationSelector::Head, writable_pinned(), B, &cancel)
        .await?
        .value;
    let mut separate = volume
        .checkout(GenerationSelector::Head, writable_pinned(), B, &cancel)
        .await?
        .value;
    for (checkout, stamp) in [
        (&mut stamped, ContentTimes::Stamp(99)),
        (&mut separate, ContentTimes::Preserve),
    ] {
        checkout
            .change_content(
                id,
                ContentChange::Write {
                    offset: 4,
                    bytes: OwnedBytes::from_static(b"STAMP"),
                },
                stamp,
                B,
                &cancel,
            )
            .await?;
    }
    let mut expected = separate.read_metadata(id, B, &cancel).await?.value;
    assert_eq!(expected, times);
    assert!(expected.stamp_content_change(99));
    separate
        .set_attributes(id, expected, None, B, &cancel)
        .await?;
    assert_eq!(stamped.root(), separate.root());
    assert_eq!(
        read(&mut stamped, id, 0, 128, &cancel).await?,
        read(&mut separate, id, 0, 128, &cancel).await?
    );
    assert_eq!(stamped.read_metadata(id, B, &cancel).await?.value, expected);
    let before = read(&mut stamped, id, 0, 128, &cancel).await?;
    assert!(
        stamped
            .change_content(
                id,
                ContentChange::CloneFrom {
                    source: FileId::from_bytes([200; 16]),
                    source_offset: 0,
                    offset: 0,
                    length: 1
                },
                ContentTimes::Stamp(100),
                B,
                &cancel
            )
            .await
            .is_err()
    );
    assert_eq!(read(&mut stamped, id, 0, 128, &cancel).await?, before);
    assert_eq!(stamped.read_metadata(id, B, &cancel).await?.value, expected);
    let untimed = path("untimed")?;
    let before = stamped.read_metadata(&untimed, B, &cancel).await?.value;
    stamped
        .change_content(
            untimed.clone(),
            ContentChange::Resize { logical_bytes: 7 },
            ContentTimes::Stamp(101),
            B,
            &cancel,
        )
        .await?;
    assert_eq!(
        stamped.read_metadata(&untimed, B, &cancel).await?.value,
        before
    );
    Ok(())
}

async fn stage(
    checkout: &TestCheckout,
    body: &'static [u8],
    cancel: &CancellationToken,
) -> TestResult<StagedContent> {
    let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
    Ok(checkout
        .stage_content(&mut source, 64, B, cancel)
        .await?
        .value)
}
async fn rewrite(
    checkout: &mut TestCheckout,
    body: &'static [u8],
    cancel: &CancellationToken,
) -> TestResult {
    let content = stage(checkout, body, cancel).await?;
    checkout
        .apply_authored_transaction(
            vec![
                AuthoredMutation::Resize {
                    path: path("large")?,
                    logical_bytes: 0,
                },
                AuthoredMutation::Resize {
                    path: path("large")?,
                    logical_bytes: 32,
                },
                AuthoredMutation::WriteFromContent {
                    path: path("large")?,
                    offset: 0,
                    content,
                },
            ],
            B,
            cancel,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn whole_file_rewrites_exceed_read_bounds_but_remain_generation_fenced() -> TestResult {
    let cancel = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 8;
    let (volume, mut seed) = fixture(limited, &cancel).await?;
    let content = stage(&seed, b"0123456789abcdef0123456789abcdef", &cancel).await?;
    seed.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: path("large")?,
            content,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        B,
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let mut replacement = volume
        .checkout(GenerationSelector::Head, writable_pinned(), B, &cancel)
        .await?
        .value;
    rewrite(
        &mut replacement,
        b"ZYXWVUTSRQPONMLKzyxwvutsrqponmlk",
        &cancel,
    )
    .await?;
    commit(&mut replacement, &cancel).await?;
    assert_eq!(
        replacement
            .read_file_range(
                &path("large")?,
                ByteRange {
                    offset: 24,
                    length: 8
                },
                B,
                &cancel
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"rqponmlk"
    );
    let mut first = volume
        .checkout(GenerationSelector::Head, writable_pinned(), B, &cancel)
        .await?
        .value;
    let mut stale = volume
        .checkout(GenerationSelector::Head, writable_pinned(), B, &cancel)
        .await?
        .value;
    rewrite(&mut first, b"FIRSTFIRSTFIRSTFIRSTFIRSTFIRST12", &cancel).await?;
    assert!(matches!(
        first
            .commit(
                OperationId::new(),
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<FsError>
            )
            .await?
            .value,
        LivePublicationObservation::Committed { .. }
    ));
    rewrite(&mut stale, b"STALESTALESTALESTALESTALESTALE12", &cancel).await?;
    assert!(matches!(
        stale
            .commit(
                OperationId::new(),
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<FsError>
            )
            .await?
            .value,
        LivePublicationObservation::Conflict { .. }
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_content_stream_and_clone_keep_real_identity_and_atomic_stamps()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("original-content-stream", B, &cancel)
        .await?
        .value;
    workspace
        .write_text("/source", "authenticated source bytes")
        .await?;
    workspace
        .write_text("/destination", "old destination")
        .await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            B,
            &cancellation,
        )
        .await?
        .value;
    let source = checkout
        .lookup_no_follow(&path("source")?, B, &cancellation)
        .await?
        .value
        .record
        .ok_or("actual source absent")?;
    let destination = checkout
        .lookup_no_follow(&path("destination")?, B, &cancellation)
        .await?
        .value
        .record
        .ok_or("actual destination absent")?;
    let metadata = FileMetadata {
        modified_ns: MetadataField::Value(1),
        changed_ns: MetadataField::Value(2),
        accessed_ns: MetadataField::Value(3),
        ..FileMetadata::default()
    };
    checkout
        .set_attributes(destination.file_id, metadata, None, B, &cancellation)
        .await?;
    let original =
        crate::NativeMaterializationWork::new(B, &cancellation).map_err(|(cause, _)| cause)?;
    let reader = checkout
        .private_candidate(
            &mut crate::kernel::KernelAccounting::native(&original, &cancellation),
            &cancellation,
        )?
        .into_pinned_reader_with_native_work(&original, &cancellation)?;
    let bytes = reader
        .read_file_range(
            &path("source")?,
            ByteRange {
                offset: 0,
                length: 26,
            },
            &original,
            &cancellation,
        )
        .await?
        .bytes;
    assert_eq!(bytes.as_ref(), b"authenticated source bytes");
    let mut candidate = checkout.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation),
        &cancellation,
    )?;
    candidate
        .change_content(
            destination.file_id,
            ContentChange::Write { offset: 0, bytes },
            ContentTimes::Stamp(40),
            &original,
            &cancellation,
        )
        .await?;
    candidate
        .change_content(
            destination.file_id,
            ContentChange::CloneFrom {
                source: source.file_id,
                source_offset: 14,
                offset: 0,
                length: 6,
            },
            ContentTimes::Stamp(41),
            &original,
            &cancellation,
        )
        .await?;
    let actual = candidate
        .read_file_record_by_id(destination.file_id, &original, &cancellation)
        .await?;
    assert_eq!(actual.file_id, destination.file_id);
    let content = candidate
        .read_file_range(
            destination.file_id,
            ByteRange {
                offset: 0,
                length: 26,
            },
            &original,
            &cancellation,
        )
        .await?;
    assert_eq!(content.bytes.as_ref(), b"sourceticated source bytes");
    let stamped = candidate
        .read_metadata(destination.file_id, &original, &cancellation)
        .await?;
    assert_eq!(stamped.modified_ns, MetadataField::Value(41));
    assert_eq!(stamped.changed_ns, MetadataField::Value(41));
    assert_eq!(stamped.accessed_ns, MetadataField::Value(3));
    assert!(original.receipt().source_bytes_read >= 26);
    assert!(original.receipt().bytes_encoded > 0);
    assert_eq!(
        checkout
            .read_metadata(destination.file_id, B, &cancellation,)
            .await?
            .value,
        metadata,
        "private native content cannot stamp its source checkout"
    );
    Ok(())
}
