use super::*;

#[tokio::test]
async fn stamped_content_change_equals_the_change_then_its_stamp_in_one_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([131; 16]), config(), B, &cancellation))
            .await?
            .value;
    let timed = path("timed")?;
    let untimed = path("untimed")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let file_id = (seed.create(
        AuthoredMutation::CreateFile {
            path: timed.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    (seed.create(
        AuthoredMutation::CreateFile {
            path: untimed.clone(),
            bytes: OwnedBytes::from(vec![b'b'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let times = FileMetadata {
        modified_ns: MetadataField::Value(1),
        accessed_ns: MetadataField::Value(2),
        changed_ns: MetadataField::Value(3),
        ..FileMetadata::default()
    };
    (seed.set_attributes_by_id(file_id, times, None, B, &cancellation)).await?;
    (seed.commit(
        OperationId::from_bytes([132; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut stamped = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let mut separate = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (stamped.change_content(
        file_id,
        ContentChange::Write {
            offset: 4,
            bytes: OwnedBytes::from_static(b"STAMP"),
        },
        ContentTimes::Stamp(99),
        B,
        &cancellation,
    ))
    .await?;
    (separate.change_content(
        file_id,
        ContentChange::Write {
            offset: 4,
            bytes: OwnedBytes::from_static(b"STAMP"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    let mut expected = (separate.read_metadata_by_id(file_id, B, &cancellation))
        .await?
        .value;
    assert_eq!(expected, times, "a preserving write leaves metadata intact");
    assert!(expected.stamp_content_change(99));
    (separate.set_attributes_by_id(file_id, expected, None, B, &cancellation)).await?;
    assert_eq!(stamped.root(), separate.root());
    let metadata = (stamped.read_metadata_by_id(file_id, B, &cancellation))
        .await?
        .value;
    assert_eq!(metadata.modified_ns, MetadataField::Value(99));
    assert_eq!(metadata.changed_ns, MetadataField::Value(99));
    assert_eq!(metadata.accessed_ns, MetadataField::Value(2));

    let before = stamped.root().clone();
    assert!(
        (stamped.change_content(
            file_id,
            ContentChange::CloneFrom {
                source: FileId::from_bytes([200; 16]),
                source_offset: 0,
                offset: 0,
                length: 1,
            },
            ContentTimes::Stamp(100),
            B,
            &cancellation
        ))
        .await
        .is_err()
    );
    assert_eq!(stamped.root(), &before);

    let untimed_before = (stamped.read_metadata(&untimed, B, &cancellation))
        .await?
        .value;
    (stamped.change_content(
        untimed.clone(),
        ContentChange::Resize { logical_bytes: 7 },
        ContentTimes::Stamp(101),
        B,
        &cancellation,
    ))
    .await?;
    let untimed_after = (stamped.read_metadata(&untimed, B, &cancellation))
        .await?
        .value;
    assert_eq!(untimed_after, untimed_before);
    Ok(())
}

#[tokio::test]
async fn grouped_changes_land_together_and_each_keeps_its_own_result()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([141; 16]), config(), B, &cancellation))
            .await?
            .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let existing = (checkout.create(
        AuthoredMutation::CreateFile {
            path: path("existing")?,
            bytes: OwnedBytes::from(vec![b'a'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    let content = |bytes: &'static [u8]| GroupedChange::Content {
        file_id: existing,
        change: ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(bytes),
        },
        times: ContentTimes::Stamp(7),
    };
    let create = |name: &str| -> Result<GroupedChange, Box<dyn std::error::Error>> {
        Ok(GroupedChange::CreateFile {
            path: path(name)?,
            metadata: Box::new(FileMetadata::default()),
            file_id: FileId::new(),
            bytes: OwnedBytes::new(),
        })
    };

    let applied = (checkout.apply_group(
        vec![create("first")?, content(b"ONE"), create("second")?],
        B,
        &cancellation,
    ))
    .await?
    .value;
    let [
        Ok(GroupedOutcome::Created(first)),
        Ok(GroupedOutcome::Changed),
        Ok(GroupedOutcome::Created(second)),
    ] = applied.as_slice()
    else {
        return Err(format!("unexpected grouped outcomes {applied:?}").into());
    };
    assert_ne!(first, second);

    let mixed = (checkout.apply_group(
        vec![
            create("third")?,
            create("first")?,
            GroupedChange::Content {
                file_id: FileId::from_bytes([142; 16]),
                change: ContentChange::Resize { logical_bytes: 1 },
                times: ContentTimes::Stamp(8),
            },
            content(b"TWO"),
        ],
        B,
        &cancellation,
    ))
    .await?
    .value;
    assert!(matches!(mixed[0], Ok(GroupedOutcome::Created(_))));
    assert!(
        mixed[1].is_err(),
        "an existing name cannot be created again"
    );
    assert!(mixed[2].is_err(), "an absent identity cannot change");
    assert!(matches!(mixed[3], Ok(GroupedOutcome::Changed)));

    for name in ["existing", "first", "second", "third"] {
        assert!(
            (checkout.lookup_no_follow(&path(name)?, B, &cancellation))
                .await?
                .value
                .record
                .is_some(),
            "{name} is bound"
        );
    }
    let read = (checkout.read_file_range_by_id(
        existing,
        ByteRange {
            offset: 0,
            length: 4,
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    assert_eq!(read.bytes.as_ref(), b"TWOa");
    Ok(())
}

#[tokio::test]
async fn each_grouped_change_has_its_own_budget() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([161; 16]), config(), B, &cancellation))
            .await?
            .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let mut files = Vec::new();
    for (index, name) in ["one", "two", "three", "four"].into_iter().enumerate() {
        let file_id = (checkout.create(
            AuthoredMutation::CreateFile {
                path: path(name)?,
                bytes: OwnedBytes::from(vec![u8::try_from(index)?; 16]),
                metadata: FileMetadata::default(),
            },
            B,
            &cancellation,
        ))
        .await?
        .value;
        files.push(file_id);
    }
    let write = |file_id: FileId, fill: u8| GroupedChange::Content {
        file_id,
        change: ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from(vec![fill; 4_096]),
        },
        times: ContentTimes::Preserve,
    };
    let single = (checkout.apply_group(vec![write(files[0], 1)], B, &cancellation)).await?;
    assert!(single.value.iter().all(Result::is_ok));

    let mut budget = B;
    budget.bytes_hashed = single.work.bytes_hashed * 3 / 2;
    let group = (checkout.apply_group(
        vec![write(files[1], 2), write(files[2], 3), write(files[3], 4)],
        budget,
        &cancellation,
    ))
    .await?;
    assert!(
        group.value.iter().all(Result::is_ok),
        "a change failed on its neighbours' work: {:?}",
        group.value
    );
    assert!(group.work.bytes_hashed > budget.bytes_hashed);
    Ok(())
}

#[tokio::test]
async fn a_whole_file_rewrite_larger_than_the_read_bound_is_admitted()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 8;
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([231; 16]), limited, B, &cancellation))
            .await?
            .value;
    let file = path("large")?;
    let stage = async |checkout: &Checkout<_, _>, body: &'static [u8]| {
        let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
        (checkout.stage_content(&mut source, 64, B, &cancellation))
            .await
            .map(|staged| staged.value)
            .map_err(|failure| format!("staging failed: {:?}", failure.error))
    };

    let mut first = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let original = stage(&first, b"0123456789abcdef0123456789abcdef").await?;
    (first.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: file.clone(),
            content: original,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        B,
        &cancellation,
    ))
    .await?;
    (first.commit(
        OperationId::from_bytes([231; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut second = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let replacement = stage(&second, b"ZYXWVUTSRQPONMLKzyxwvutsrqponmlk").await?;
    (second.apply_authored_transaction(
        vec![
            AuthoredMutation::Resize {
                path: file.clone(),
                logical_bytes: 0,
            },
            AuthoredMutation::Resize {
                path: file.clone(),
                logical_bytes: 32,
            },
            AuthoredMutation::WriteFromContent {
                path: file.clone(),
                offset: 0,
                content: replacement,
            },
        ],
        B,
        &cancellation,
    ))
    .await
    .map_err(|failure| format!("whole-file rewrite refused: {:?}", failure.error))?;
    (second.commit(
        OperationId::from_bytes([232; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let head = (second.read_file_range(
        &file,
        ByteRange {
            offset: 24,
            length: 8,
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    assert_eq!(head.bytes, OwnedBytes::from_static(b"rqponmlk"));
    Ok(())
}

#[tokio::test]
async fn a_stale_whole_file_rewrite_still_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 8;
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([233; 16]), limited, B, &cancellation))
            .await?
            .value;
    let file = path("large")?;
    let checkout = async || {
        (volume.checkout(
            GenerationSelector::Head,
            writable_pinned(),
            B,
            &cancellation,
        ))
        .await
        .map(|checkout| checkout.value)
        .map_err(|failure| format!("checkout failed: {:?}", failure.error))
    };
    let stage = async |checkout: &Checkout<_, _>, body: &'static [u8]| {
        let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
        (checkout.stage_content(&mut source, 64, B, &cancellation))
            .await
            .map(|staged| staged.value)
            .map_err(|failure| format!("staging failed: {:?}", failure.error))
    };
    let rewrite = async |checkout: &mut Checkout<_, _>, body: &'static [u8]| {
        let content = stage(checkout, body).await?;
        (checkout.apply_authored_transaction(
            vec![
                AuthoredMutation::Resize {
                    path: file.clone(),
                    logical_bytes: 0,
                },
                AuthoredMutation::Resize {
                    path: file.clone(),
                    logical_bytes: 32,
                },
                AuthoredMutation::WriteFromContent {
                    path: file.clone(),
                    offset: 0,
                    content,
                },
            ],
            B,
            &cancellation,
        ))
        .await
        .map(|_| ())
        .map_err(|failure| format!("rewrite refused: {:?}", failure.error))
    };

    let mut seed = checkout().await?;
    let original = stage(&seed, b"0123456789abcdef0123456789abcdef").await?;
    (seed.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: file.clone(),
            content: original,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        B,
        &cancellation,
    ))
    .await?;
    (seed.commit(
        OperationId::from_bytes([233; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut first = checkout().await?;
    let mut stale = checkout().await?;
    rewrite(&mut first, b"FIRSTFIRSTFIRSTFIRSTFIRSTFIRST12").await?;
    let committed = (first.commit(
        OperationId::from_bytes([234; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?
    .value;
    assert!(
        matches!(committed, LivePublicationObservation::Committed { .. }),
        "{committed:?}"
    );
    rewrite(&mut stale, b"STALESTALESTALESTALESTALESTALE12").await?;
    let refused = (stale.commit(
        OperationId::from_bytes([235; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?
    .value;
    assert!(
        matches!(refused, LivePublicationObservation::Conflict { .. }),
        "{refused:?}"
    );
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
        .set_attributes_by_id(destination.file_id, metadata, None, B, &cancellation)
        .await?;
    let original =
        crate::NativeMaterializationWork::new(B, &cancellation).map_err(|(cause, _)| cause)?;
    let reader = checkout.pinned_reader()?;
    let bytes = reader
        .read_record_range(
            source,
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
    let mut candidate = checkout.private_candidate(&original, &cancellation)?;
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
    let content = reader
        .read_record_range(
            actual,
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
        .read_metadata_by_id(destination.file_id, &original, &cancellation)
        .await?;
    assert_eq!(stamped.modified_ns, MetadataField::Value(41));
    assert_eq!(stamped.changed_ns, MetadataField::Value(41));
    assert_eq!(stamped.accessed_ns, MetadataField::Value(3));
    assert!(original.receipt().source_bytes_read >= 26);
    assert!(original.receipt().bytes_encoded > 0);
    assert_eq!(
        checkout
            .read_metadata_by_id(destination.file_id, B, &cancellation,)
            .await?
            .value,
        metadata,
        "private native content cannot stamp its source checkout"
    );
    let mut accounting =
        crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let destination_path = path("destination")?;
    let source_path = path("source")?;
    let mut destination_owned = clone_authored_path(&destination_path, &mut accounting)?;
    let mut source_owned = clone_authored_path(&source_path, &mut accounting)?;
    let input_bytes = destination_owned
        .bytes
        .checked_add(source_owned.bytes)
        .ok_or(WorkError::Overflow)?;
    destination_owned
        .allocation
        .absorb(&mut source_owned.allocation)?;
    let compiled = candidate
        .compile_content_with_accounting(
            crate::kernel::Owned::new(
                (
                    destination_owned.value,
                    ContentChange::CloneFrom {
                        source: source_owned.value,
                        source_offset: 0,
                        offset: 0,
                        length: 6,
                    },
                ),
                destination_owned.allocation,
                input_bytes,
            ),
            ContentTimes::Stamp(42),
            &mut accounting,
            &cancellation,
        )
        .await?;
    assert!(
        matches!(&compiled.value[0], Mutation::CloneRange { source, destination, .. }
        if source == &source_path && destination == &destination_path)
    );
    assert_eq!(
        compiled.bytes,
        compiled.value.capacity() as u64 * size_of::<Mutation>() as u64
            + source_path.retained_bytes()
            + 2 * destination_path.retained_bytes()
    );
    compiled.retire(&mut accounting)?;
    Ok(())
}
