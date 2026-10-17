use super::*;

#[tokio::test]
async fn detached_open_file_remains_sparse_and_mutable_after_last_binding_removal() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    let id = create(&mut checkout, "open-file", vec![b'a'; 128].into(), &cancel).await?;
    let file = path("open-file")?;
    let mut detached = checkout.detach_regular_file(&file, B, &cancel).await?.value;
    assert_eq!(detached.file_id(), id);
    for (offset, target, expected) in [
        (0, ExtentSeekTarget::Data, Some(0)),
        (0, ExtentSeekTarget::Hole, Some(128)),
        (128, ExtentSeekTarget::Data, None),
        (128, ExtentSeekTarget::Hole, Some(128)),
        (129, ExtentSeekTarget::Data, None),
        (129, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            detached.seek(offset, target, B, &cancel).await?.value,
            expected
        );
    }
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.detached".to_vec(),
        128,
    )?;
    assert!(
        detached
            .read_named_attribute(&attribute, B, &cancel)
            .await?
            .value
            .is_none()
    );
    assert!(
        detached
            .list_named_attributes(None, 8, B, &cancel)
            .await?
            .value
            .entries
            .is_empty()
    );
    assert!(matches!(
        detached
            .change_named_attribute(&attribute, None, B, &cancel)
            .await,
        Err(OperationFailure {
            error: FsError::NotFound,
            ..
        })
    ));
    assert!(matches!(
        detached
            .change_named_attribute(
                &attribute,
                Some((
                    &OwnedBytes::from_static(b"replacement"),
                    NamedAttributeWriteMode::Replace
                )),
                B,
                &cancel
            )
            .await,
        Err(OperationFailure {
            error: FsError::CreationRejected,
            ..
        })
    ));
    for (bytes, mode) in [
        (b"first".as_slice(), NamedAttributeWriteMode::Create),
        (b"second".as_slice(), NamedAttributeWriteMode::Replace),
    ] {
        detached
            .change_named_attribute(
                &attribute,
                Some((&OwnedBytes::copy_from_slice(bytes), mode)),
                B,
                &cancel,
            )
            .await?;
        assert_eq!(
            detached
                .read_named_attribute(&attribute, B, &cancel)
                .await?
                .value
                .unwrap()
                .bytes
                .as_ref(),
            bytes
        );
        assert_eq!(
            detached
                .list_named_attributes(None, 8, B, &cancel)
                .await?
                .value
                .entries
                .len(),
            1
        );
        assert!(
            detached
                .change_named_attribute(
                    &attribute,
                    Some((
                        &OwnedBytes::from_static(b"duplicate"),
                        NamedAttributeWriteMode::Create
                    )),
                    B,
                    &cancel
                )
                .await
                .is_err()
        );
    }
    let mut metadata = detached.read_metadata(B, &cancel).await?.value;
    for mode in [0o100_640, 0o100_600] {
        metadata.posix_mode = MetadataField::Value(mode);
        detached.set_attributes(metadata, None, B, &cancel).await?;
    }
    assert_eq!(detached.logical_bytes(), 128);
    detached
        .set_attributes(metadata, Some(160), B, &cancel)
        .await?;
    assert_eq!(detached.logical_bytes(), 160);
    checkout
        .mutate(
            vec![Mutation::Remove {
                path: file.clone(),
                expected_file_id: MetadataField::Value(id),
            }],
            B,
            &cancel,
        )
        .await?;
    assert!(
        checkout
            .lookup_no_follow(&file, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    for change in [
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"open"),
        },
        ContentChange::Write {
            offset: 4,
            bytes: OwnedBytes::new(),
        },
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 128,
                length: 64,
            },
            keep_size: false,
        },
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 64,
                length: 16,
            },
            allocated: false,
            extend: false,
        },
    ] {
        detached.change_content(change, None, B, &cancel).await?;
    }
    let read = detached
        .read_range(
            ByteRange {
                offset: 0,
                length: 80,
            },
            B,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(&read.bytes[..4], b"open");
    assert_eq!(&read.bytes[64..80], &[0; 16]);
    detached
        .change_content(
            ContentChange::Resize { logical_bytes: 192 },
            None,
            B,
            &cancel,
        )
        .await?;
    assert_eq!(detached.logical_bytes(), 192);
    detached
        .change_named_attribute(&attribute, None, B, &cancel)
        .await?;
    assert!(
        detached
            .change_named_attribute(&attribute, None, B, &cancel)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn detached_inline_file_seek_and_attribute_absence_are_exact() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    create(
        &mut checkout,
        "inline-detached",
        OwnedBytes::from_static(b"abc"),
        &cancel,
    )
    .await?;
    let mut detached = checkout
        .detach_regular_file(&path("inline-detached")?, B, &cancel)
        .await?
        .value;
    assert_eq!(detached.logical_bytes(), 3);
    for (offset, target, expected) in [
        (0, ExtentSeekTarget::Data, Some(0)),
        (3, ExtentSeekTarget::Data, None),
        (0, ExtentSeekTarget::Hole, Some(3)),
        (3, ExtentSeekTarget::Hole, Some(3)),
        (4, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            detached.seek(offset, target, B, &cancel).await?.value,
            expected
        );
    }
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.inline".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    detached
        .change_named_attribute(
            &attribute,
            Some((
                &OwnedBytes::from_static(b"value"),
                NamedAttributeWriteMode::Create,
            )),
            B,
            &cancel,
        )
        .await?;
    detached
        .change_named_attribute(&attribute, None, B, &cancel)
        .await?;
    assert!(
        detached
            .read_named_attribute(&attribute, B, &cancel)
            .await?
            .value
            .is_none()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_detached_content_stream_keeps_atomic_metadata_transition() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    create(
        &mut checkout,
        "source",
        OwnedBytes::from_static(b"real detached stream"),
        &cancel,
    )
    .await?;
    create(
        &mut checkout,
        "destination",
        OwnedBytes::from_static(b"before"),
        &cancel,
    )
    .await?;
    commit(&mut checkout, &cancel).await?;
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(cause, _)| cause)?;
    let reader = checkout
        .private_candidate(
            &mut crate::kernel::KernelAccounting::native(&original, &cancel),
            &cancel,
        )?
        .into_pinned_reader_with_native_work(&original, &cancel)?;
    let bytes = reader
        .read_file_range(
            &path("source")?,
            ByteRange {
                offset: 0,
                length: 20,
            },
            &original,
            &cancel,
        )
        .await?
        .bytes;
    let metadata = FileMetadata {
        modified_ns: MetadataField::Value(90),
        changed_ns: MetadataField::Value(90),
        ..FileMetadata::default()
    };
    let mut detached = checkout
        .detach_regular_file(&path("destination")?, B, &cancel)
        .await?
        .value;
    let id = detached.file_id();
    detached
        .change_content(
            ContentChange::Write { offset: 0, bytes },
            Some(metadata),
            &original,
            &cancel,
        )
        .await?;
    assert_eq!(detached.file_id(), id);
    assert_eq!(
        detached
            .read_range(
                ByteRange {
                    offset: 0,
                    length: 20
                },
                &original,
                &cancel
            )
            .await?
            .bytes
            .as_ref(),
        b"real detached stream"
    );
    let before = detached.read_metadata(B, &cancel).await?.value;
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_encoded: 0,
            ..B
        },
        &cancel,
    )
    .map_err(|(cause, _)| cause)?;
    let failure = detached
        .change_content(
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::new(),
            },
            Some(FileMetadata {
                modified_ns: MetadataField::Value(91),
                ..metadata
            }),
            &denied,
            &cancel,
        )
        .await
        .err()
        .ok_or("metadata encoding ignored original denial")?;
    assert!(matches!(
        failure,
        FsError::Work(_) | FsError::NativeKernel(_)
    ));
    assert_eq!(detached.read_metadata(B, &cancel).await?.value, before);
    assert_eq!(
        detached
            .read_range(
                ByteRange {
                    offset: 0,
                    length: 20
                },
                &original,
                &cancel
            )
            .await?
            .bytes
            .as_ref(),
        b"real detached stream"
    );
    assert_eq!(denied.receipt().bytes_encoded, 0);
    assert_eq!(denied.receipt().backend_write_operations, 0);
    Ok(())
}
