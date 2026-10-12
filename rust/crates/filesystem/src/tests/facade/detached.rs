use super::*;

#[tokio::test]
async fn detached_open_file_remains_sparse_and_mutable_after_last_binding_removal()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), B, &cancellation)).await?.value;
    let file = path("open-file")?;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (checkout.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let detached = (checkout.detach_regular_file(&file, B, &cancellation)).await?;
    let mut detached = detached.value;
    let file_id = detached.file_id();
    assert_eq!(
        (detached.seek(0, ExtentSeekTarget::Data, B, &cancellation,))
            .await?
            .value,
        Some(0)
    );
    assert_eq!(
        (detached.seek(0, ExtentSeekTarget::Hole, B, &cancellation,))
            .await?
            .value,
        Some(128)
    );
    for (offset, target, expected) in [
        (128, ExtentSeekTarget::Data, None),
        (128, ExtentSeekTarget::Hole, Some(128)),
        (129, ExtentSeekTarget::Data, None),
        (129, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            (detached.seek(offset, target, B, &cancellation,))
                .await?
                .value,
            expected
        );
    }

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.detached".to_vec(),
        128,
    )?;
    assert!(
        (detached.read_named_attribute(&attribute, B, &cancellation,))
            .await?
            .value
            .is_none()
    );
    assert!(
        (detached.list_named_attributes(None, 8, B, &cancellation,))
            .await?
            .value
            .entries
            .is_empty()
    );
    let absent_remove =
        (detached.change_named_attribute(&attribute.clone(), None, B, &cancellation))
            .await
            .err()
            .ok_or("attribute-free detached removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = (detached.change_named_attribute(
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"replacement"),
            NamedAttributeWriteMode::Replace,
        )),
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("absent detached replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    (detached.change_named_attribute(
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"first"),
            NamedAttributeWriteMode::Create,
        )),
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(
        (detached.read_named_attribute(&attribute, B, &cancellation,))
            .await?
            .value
            .map(|chunk| chunk.bytes)
            .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        (detached.list_named_attributes(None, 8, B, &cancellation,))
            .await?
            .value
            .entries
            .len(),
        1
    );
    assert!(
        (detached.change_named_attribute(
            &attribute.clone(),
            Some((
                &OwnedBytes::from_static(b"duplicate"),
                NamedAttributeWriteMode::Create
            )),
            B,
            &cancellation
        ))
        .await
        .is_err()
    );
    (detached.change_named_attribute(
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"second"),
            NamedAttributeWriteMode::Replace,
        )),
        B,
        &cancellation,
    ))
    .await?;

    let mut metadata = (detached.read_metadata(B, &cancellation)).await?.value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    (detached.set_attributes(metadata, None, B, &cancellation)).await?;
    metadata.posix_mode = MetadataField::Value(0o100_600);
    (detached.set_attributes(metadata, None, B, &cancellation)).await?;
    assert_eq!(detached.logical_bytes(), 128);
    (detached.set_attributes(metadata, Some(160), B, &cancellation)).await?;
    assert_eq!(detached.logical_bytes(), 160);
    (checkout.mutate(
        vec![Mutation::Remove {
            path: file.clone(),
            expected_file_id: Some(file_id)
                .map_or(MetadataField::Unavailable, MetadataField::Value),
        }],
        B,
        &cancellation,
    ))
    .await?;
    let absent = (checkout.lookup_no_follow(&file, B, &cancellation)).await?;
    assert!(absent.value.record.is_none());

    (detached.change_content(
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"open"),
        },
        None,
        B,
        &cancellation,
    ))
    .await?;
    (detached.change_content(
        ContentChange::Write {
            offset: 4,
            bytes: OwnedBytes::new(),
        },
        None,
        B,
        &cancellation,
    ))
    .await?;
    (detached.change_content(
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 128,
                length: 64,
            },
            keep_size: false,
        },
        None,
        B,
        &cancellation,
    ))
    .await?;
    (detached.change_content(
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 64,
                length: 16,
            },
            allocated: false,
            extend: false,
        },
        None,
        B,
        &cancellation,
    ))
    .await?;
    let read = (detached.read_range(
        ByteRange {
            offset: 0,
            length: 80,
        },
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(&read.value.bytes[..4], b"open");
    assert_eq!(&read.value.bytes[64..80], &[0; 16]);
    (detached.change_content(
        ContentChange::Resize { logical_bytes: 192 },
        None,
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(detached.logical_bytes(), 192);
    (detached.change_named_attribute(&attribute.clone(), None, B, &cancellation)).await?;
    assert!(
        (detached.change_named_attribute(&attribute, None, B, &cancellation))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn detached_inline_file_seek_and_attribute_absence_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), B, &cancellation)).await?.value;
    let file = path("inline-detached")?;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (checkout.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from_static(b"abc"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let mut detached = (checkout.detach_regular_file(&file, B, &cancellation))
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
        let receipt = (detached.seek(offset, target, B, &cancellation)).await?;
        assert_eq!(receipt.value, expected);
        assert_eq!(receipt.work, WorkCounters::default());
    }

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.inline".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    (detached.change_named_attribute(
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"value"),
            NamedAttributeWriteMode::Create,
        )),
        B,
        &cancellation,
    ))
    .await?;
    (detached.change_named_attribute(&attribute.clone(), None, B, &cancellation)).await?;
    let absent = (detached.read_named_attribute(&attribute, B, &cancellation)).await?;
    assert!(absent.value.is_none());

    detached.record.kind = FileKind::Fifo;
    detached.record.payload = FilePayload::Empty;
    assert_eq!(detached.logical_bytes(), 0);
    let rejected = (detached.seek(0, ExtentSeekTarget::Data, B, &cancellation))
        .await
        .err()
        .ok_or("non-regular detached seek succeeded")?;
    assert!(matches!(
        rejected.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert_eq!(*rejected.work, WorkCounters::default());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_detached_content_stream_keeps_atomic_metadata_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("original-detached-content", B, &cancel)
        .await?
        .value;
    workspace
        .write_text("/source", "real detached stream")
        .await?;
    workspace.write_text("/destination", "before").await?;
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
        .ok_or("real detached source absent")?;
    let destination = checkout
        .lookup_no_follow(&path("destination")?, B, &cancellation)
        .await?
        .value
        .record
        .ok_or("real detached destination absent")?;
    let reader = checkout.pinned_reader()?;
    let original =
        crate::NativeMaterializationWork::new(B, &cancellation).map_err(|(cause, _)| cause)?;
    let source = reader
        .read_record_range(
            source,
            ByteRange {
                offset: 0,
                length: 20,
            },
            &original,
            &cancellation,
        )
        .await?;
    let metadata = FileMetadata {
        modified_ns: MetadataField::Value(90),
        changed_ns: MetadataField::Value(90),
        ..FileMetadata::default()
    };
    let mut detached =
        DetachedFile::from_record(checkout.volume.clone(), destination, B, &cancellation)?.value;
    detached
        .change_content(
            ContentChange::Write {
                offset: 0,
                bytes: source.bytes,
            },
            Some(metadata),
            &original,
            &cancellation,
        )
        .await?;
    assert_eq!(detached.record.file_id, destination.file_id);
    assert_eq!(
        reader
            .read_record_range(
                detached.record,
                ByteRange {
                    offset: 0,
                    length: 20
                },
                &original,
                &cancellation,
            )
            .await?
            .bytes
            .as_ref(),
        b"real detached stream"
    );
    let before = detached.record;
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_encoded: 0,
            ..B
        },
        &cancellation,
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
            &cancellation,
        )
        .await
        .err()
        .ok_or("detached metadata encoding ignored original denial")?;
    assert!(matches!(
        failure,
        FsError::Work(_) | FsError::NativeKernel(_)
    ));
    assert_eq!(detached.record, before);
    assert_eq!(denied.receipt().bytes_encoded, 0);
    assert_eq!(denied.receipt().backend_write_operations, 0);
    Ok(())
}
