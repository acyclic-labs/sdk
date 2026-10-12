use super::*;

#[tokio::test]
async fn pinned_reader_batches_ranges_in_request_order() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), B, &cancellation)).await?.value;
    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (writer.create(
        AuthoredMutation::CreateFile {
            path: path("first")?,
            bytes: OwnedBytes::from_static(b"abcdefgh"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (writer.create(
        AuthoredMutation::CreateFile {
            path: path("second")?,
            bytes: OwnedBytes::from_static(b"01234567"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (writer.create(
        AuthoredMutation::CreateSymbolicLink {
            path: path("link")?,
            target: OwnedBytes::from_static(b"first"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let first_directory = path("first-directory")?;
    let second_directory = path("second-directory")?;
    (writer.create(
        AuthoredMutation::CreateDirectory {
            path: first_directory.clone(),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (writer.create(
        AuthoredMutation::CreateDirectory {
            path: second_directory.clone(),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    for name in ["a", "b"] {
        (writer.create(
            AuthoredMutation::CreateFile {
                path: path_parts(&["first-directory", name])?,
                bytes: OwnedBytes::from_static(b"x"),
                metadata: FileMetadata::default(),
            },
            B,
            &cancellation,
        ))
        .await?;
    }
    (writer.create(
        AuthoredMutation::CreateFile {
            path: path_parts(&["second-directory", "c"])?,
            bytes: OwnedBytes::from_static(b"y"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let reader = writer.pinned_reader()?;
    let request_paths = [path("second")?, path("first")?];
    let resolved_batch = (reader.resolve_files(&request_paths, B, &cancellation)).await?;
    let second_file = resolved_batch.value[0]
        .as_ref()
        .ok_or("second resolved file absent")?;
    let first_file = resolved_batch.value[1]
        .as_ref()
        .ok_or("first resolved file absent")?;
    let requests = [
        ResolvedFileRangeReadRequest {
            file: second_file,
            range: ByteRange {
                offset: 2,
                length: 3,
            },
        },
        ResolvedFileRangeReadRequest {
            file: first_file,
            range: ByteRange {
                offset: 1,
                length: 4,
            },
        },
    ];
    let batch = (reader.read_resolved_ranges(&requests, 2, B, &cancellation)).await?;
    assert_eq!(&batch.value[0].bytes[..], b"234");
    assert_eq!(&batch.value[1].bytes[..], b"bcde");

    let descriptions = (reader.describe_files(
        &[path("first")?, path("second")?, path("first")?],
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(descriptions.value.len(), 3);
    assert_eq!(descriptions.value[0], descriptions.value[2]);
    assert!(descriptions.value.iter().all(Option::is_some));
    let resolved = (reader.resolve_files(&[path("first")?], B, &cancellation)).await?;
    let resolved_file = resolved.value[0].as_ref().ok_or("resolved file absent")?;
    let resolved_read = (resolved_file.read_range(
        ByteRange {
            offset: 1,
            length: 4,
        },
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(&resolved_read.value.bytes[..], b"bcde");
    assert_eq!(resolved_read.work.page_reads, 0);
    let other_volume = (fs.create_volume(config(), B, &cancellation)).await?.value;
    let mut other_writer = (other_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (other_writer.create(
        AuthoredMutation::CreateFile {
            path: path("foreign")?,
            bytes: OwnedBytes::from_static(b"WXYZ"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let other_reader = other_writer.pinned_reader()?;
    let other_resolved =
        (other_reader.resolve_files(&[path("foreign")?], B, &cancellation)).await?;
    let other_file = other_resolved.value[0]
        .as_ref()
        .ok_or("other resolved file absent")?;
    let mixed_requests = [
        ResolvedFileRangeReadRequest {
            file: resolved_file,
            range: ByteRange {
                offset: 0,
                length: 2,
            },
        },
        ResolvedFileRangeReadRequest {
            file: other_file,
            range: ByteRange {
                offset: 1,
                length: 2,
            },
        },
    ];
    let mixed = (reader.read_resolved_ranges(&mixed_requests, 2, B, &cancellation)).await?;
    assert_eq!(&mixed.value[0].bytes[..], b"ab");
    assert_eq!(&mixed.value[1].bytes[..], b"XY");
    let bounded_mixed =
        (reader.read_resolved_ranges(&mixed_requests, 2, mixed.work, &cancellation)).await?;
    assert_eq!(bounded_mixed.value, mixed.value);
    let target = (reader.read_symbolic_link(&path("link")?, B, &cancellation)).await?;
    assert_eq!(&target.value[..], b"first");
    assert!(matches!(
        (reader.read_symbolic_link(&path("first")?, B, &cancellation,)).await,
        Err(OperationFailure {
            error: FsError::NotSymbolicLink,
            ..
        })
    ));
    let pages = (reader.list_directory_pages(
        &[
            DirectoryPageRequest {
                path: second_directory,
                after: None,
                maximum_entries: 8,
            },
            DirectoryPageRequest {
                path: first_directory.clone(),
                after: Some(LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?),
                maximum_entries: 1,
            },
        ],
        2,
        B,
        &cancellation,
    ))
    .await?;
    let second_page = pages.value.first().ok_or("second directory page missing")?;
    let first_page = pages.value.get(1).ok_or("first directory page missing")?;
    assert_eq!(
        second_page
            .entries
            .first()
            .ok_or("second entry missing")?
            .name
            .as_bytes(),
        b"c"
    );
    assert_eq!(
        first_page
            .entries
            .first()
            .ok_or("first entry missing")?
            .name
            .as_bytes(),
        b"b"
    );
    assert!(!second_page.has_more);
    assert!(!first_page.has_more);
    let resolved_page =
        (reader.resolve_directory_page(&first_directory, None, 1, B, &cancellation)).await?;
    assert_eq!(resolved_page.value.entries.len(), 1);
    assert!(resolved_page.value.has_more);
    let resolved_entry = resolved_page
        .value
        .entries
        .first()
        .ok_or("resolved directory entry missing")?;
    assert_eq!(resolved_entry.name.as_bytes(), b"a");
    assert_eq!(resolved_entry.file.description().kind, FileKind::Regular);
    let resolved_child = (resolved_entry.file.read_range(
        ByteRange {
            offset: 0,
            length: 1,
        },
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(&resolved_child.value.bytes[..], b"x");
    assert_eq!(resolved_child.work.page_reads, 0);
    assert!(
        (reader.list_directory_pages(&[], 0, B, &cancellation,))
            .await
            .is_err()
    );

    let first =
        (reader.read_file_range(&request_paths[0], requests[0].range, B, &cancellation)).await?;
    let second =
        (reader.read_file_range(&request_paths[1], requests[1].range, B, &cancellation)).await?;
    let independent = first.work.checked_add(second.work)?;
    assert!(batch.work.object_probes < independent.object_probes);
    assert!(batch.work.backend_read_operations < independent.backend_read_operations);
    assert!(batch.work.page_reads < independent.page_reads);

    let Err(bounded_failure) =
        (reader.read_resolved_ranges(&requests, 2, first.work, &cancellation)).await
    else {
        return Err("the second range received the first range's full budget".into());
    };
    bounded_failure.work.verify(first.work)?;

    let concurrent_failure_requests = [
        ResolvedFileRangeReadRequest {
            file: second_file,
            range: ByteRange {
                offset: 0,
                length: u64::MAX,
            },
        },
        ResolvedFileRangeReadRequest {
            file: first_file,
            range: requests[1].range,
        },
    ];
    let Err(concurrent_failure) =
        (reader.read_resolved_ranges(&concurrent_failure_requests, 2, B, &cancellation)).await
    else {
        return Err("the invalid concurrent range unexpectedly succeeded".into());
    };
    assert_eq!(
        concurrent_failure.work.backend_read_operations,
        batch.work.backend_read_operations
    );
    assert_eq!(concurrent_failure.work.page_reads, batch.work.page_reads);
    assert_eq!(
        concurrent_failure.work.output_bytes,
        second.work.output_bytes
    );

    let tracking = (volume.checkout(GenerationSelector::Head, tracking(), B, &cancellation))
        .await?
        .value;
    assert!(matches!(
        tracking.pinned_reader(),
        Err(FsError::MutationNotAllowed)
    ));
    Ok(())
}

#[tokio::test]
async fn checkout_snapshot_reader_is_private_and_stable() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), B, &cancellation)).await?.value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let file_path = path("private")?;
    let file_id = (checkout.create(
        AuthoredMutation::CreateFile {
            path: file_path.clone(),
            bytes: OwnedBytes::from_static(b"first"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    let snapshot = checkout.snapshot_reader();
    let resolved = (snapshot.resolve_files(std::slice::from_ref(&file_path), B, &cancellation))
        .await?
        .value
        .into_iter()
        .next()
        .flatten()
        .ok_or("private file missing from snapshot")?;
    assert_eq!(resolved.file_id(), file_id);

    (checkout.change_content(
        file_path,
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"later"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    let bytes = (resolved.read_range(
        ByteRange {
            offset: 0,
            length: 5,
        },
        B,
        &cancellation,
    ))
    .await?
    .value
    .bytes;
    assert_eq!(&bytes[..], b"first");
    Ok(())
}

#[tokio::test]
async fn opaque_payload_reads_enforce_the_volume_output_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.profile = FilesystemProfile::Windows;
    limited.limits.maximum_read_bytes = 1;
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([126; 16]), limited, B, &cancellation))
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
    let mut source = std::io::Cursor::new(OwnedBytes::from_static(b"xx"));
    let staged = (checkout.stage_content(&mut source, 2, B, &cancellation))
        .await?
        .value;
    let metadata = checkout
        .lookup_no_follow(&root_path()?, B, &cancellation)
        .await?
        .value
        .record
        .ok_or("root metadata missing")?
        .metadata;
    let symbolic = path("symbolic")?;
    let reparse = path("reparse")?;
    (checkout.mutate(
        vec![
            Mutation::Create {
                path: symbolic.clone(),
                record: FileRecord {
                    file_id: FileId::from_bytes([127; 16]),
                    kind: FileKind::SymbolicLink,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::SymbolicLink {
                        target_bytes: staged.logical_bytes(),
                        target: staged.root(),
                    },
                },
            },
            Mutation::Create {
                path: reparse.clone(),
                record: FileRecord {
                    file_id: FileId::from_bytes([128; 16]),
                    kind: FileKind::ReparsePoint,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::ReparsePoint {
                        payload_bytes: staged.logical_bytes(),
                        payload: staged.root(),
                    },
                },
            },
        ],
        B,
        &cancellation,
    ))
    .await?;
    for failure in [
        (checkout.read_symbolic_link(&symbolic, B, &cancellation))
            .await
            .err()
            .ok_or("oversized symlink read unexpectedly succeeded")?,
        (checkout.read_reparse_point(&reparse, B, &cancellation))
            .await
            .err()
            .ok_or("oversized reparse read unexpectedly succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert!(failure.work.object_probes > 0);
    }
    Ok(())
}

#[tokio::test]
async fn public_query_and_identity_failures_preserve_the_checkout_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([118; 16]), config(), B, &cancellation))
            .await?
            .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let file = path("regular")?;
    let directory = path("directory")?;
    let missing = path("missing")?;
    let file_id = (checkout.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from_static(b"data"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    let directory_id = (checkout.create(
        AuthoredMutation::CreateDirectory {
            path: directory.clone(),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    let empty_directory =
        (checkout.list_directory_records(&directory, None, 8, B, &cancellation)).await?;
    assert!(empty_directory.value.entries.is_empty());
    assert!(!empty_directory.value.has_more);
    let unknown_id = FileId::from_bytes([250; 16]);
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.missing".to_vec(),
        128,
    )?;

    let oversized_identity_read = (checkout.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: config().limits.maximum_read_bytes + 1,
        },
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("oversized identity read unexpectedly succeeded")?;
    assert!(matches!(
        oversized_identity_read.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*oversized_identity_read.work, WorkCounters::default());
    let zero_span_plan = (checkout.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 1,
        },
        0,
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("zero-span identity plan unexpectedly succeeded")?;
    assert!(matches!(
        zero_span_plan.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*zero_span_plan.work, WorkCounters::default());
    assert_eq!(
        (checkout.seek_file_extent_by_id(file_id, 0, ExtentSeekTarget::Data, B, &cancellation,))
            .await?
            .value,
        Some(0)
    );
    assert_eq!(
        (checkout.seek_file_extent_by_id(file_id, 0, ExtentSeekTarget::Hole, B, &cancellation,))
            .await?
            .value,
        Some(4)
    );
    for (offset, target, expected) in [
        (4, ExtentSeekTarget::Data, None),
        (4, ExtentSeekTarget::Hole, Some(4)),
        (5, ExtentSeekTarget::Data, None),
        (5, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            (checkout.seek_file_extent_by_id(file_id, offset, target, B, &cancellation,))
                .await?
                .value,
            expected
        );
        assert_eq!(
            (checkout.seek_file_extent(&file, offset, target, B, &cancellation,))
                .await?
                .value,
            expected
        );
    }
    let wrong_kind_seek = (checkout.seek_file_extent_by_id(
        directory_id,
        0,
        ExtentSeekTarget::Data,
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("wrong-kind identity seek unexpectedly succeeded")?;
    assert!(matches!(
        wrong_kind_seek.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert!(
        (checkout.read_named_attribute_by_id(file_id, &attribute, B, &cancellation,))
            .await?
            .value
            .is_none()
    );
    assert!(
        (checkout.list_named_attributes_by_id(file_id, None, 8, B, &cancellation,))
            .await?
            .value
            .entries
            .is_empty()
    );
    let missing_remove = (checkout.change_named_attribute_by_id(
        file_id,
        &attribute.clone(),
        None,
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("attribute-free identity removal unexpectedly succeeded")?;
    assert!(matches!(missing_remove.error, FsError::NotFound));
    let missing_replace = (checkout.change_named_attribute_by_id(
        file_id,
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"first"),
            NamedAttributeWriteMode::Replace,
        )),
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("missing identity attribute replacement unexpectedly succeeded")?;
    assert!(matches!(missing_replace.error, FsError::CreationRejected));
    (checkout.change_named_attribute_by_id(
        file_id,
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"first"),
            NamedAttributeWriteMode::Create,
        )),
        B,
        &cancellation,
    ))
    .await?;
    let duplicate_create = (checkout.change_named_attribute_by_id(
        file_id,
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"second"),
            NamedAttributeWriteMode::Create,
        )),
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("duplicate identity attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        (checkout.read_named_attribute_by_id(file_id, &attribute, B, &cancellation,))
            .await?
            .value
            .as_deref(),
        Some(b"first".as_slice())
    );
    (checkout.change_named_attribute_by_id(file_id, &attribute.clone(), None, B, &cancellation))
        .await?;
    let repeated_remove = (checkout.change_named_attribute_by_id(
        file_id,
        &attribute.clone(),
        None,
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("repeated identity attribute removal unexpectedly succeeded")?;
    assert!(matches!(repeated_remove.error, FsError::NotFound));

    assert!(
        (checkout.list_directory(&file, None, 8, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.list_directory_records(&file, None, 8, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.read_file_range(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (checkout.read_file_range(
            &file,
            ByteRange {
                offset: 5,
                length: 1,
            },
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (checkout.plan_file_extents(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (checkout.seek_file_extent(&directory, 0, ExtentSeekTarget::Data, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.read_symbolic_link(&file, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.read_reparse_point(&file, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.read_metadata(&missing, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.read_named_attribute(&missing, &attribute, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.list_named_attributes(&missing, None, 8, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.change_named_attribute(
            missing.clone(),
            &attribute.clone(),
            None,
            B,
            &cancellation
        ))
        .await
        .is_err()
    );

    assert!(
        (checkout.read_file_record_by_id(unknown_id, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.read_file_range_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 5,
                length: 1,
            },
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (checkout.plan_file_extents_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (checkout.seek_file_extent_by_id(
            directory_id,
            0,
            ExtentSeekTarget::Data,
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (checkout.read_metadata_by_id(unknown_id, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.read_named_attribute_by_id(unknown_id, &attribute, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.list_named_attributes_by_id(unknown_id, None, 8, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.change_content(
            directory_id,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"x")
            },
            ContentTimes::Preserve,
            B,
            &cancellation
        ))
        .await
        .is_err()
    );
    assert!(
        (checkout.change_named_attribute_by_id(unknown_id, &attribute, None, B, &cancellation))
            .await
            .is_err()
    );
    assert!(checkout.has_pending_mutations());
    assert_eq!(
        (checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            B,
            &cancellation,
        ))
        .await?
        .value
        .bytes
        .as_ref(),
        b"data"
    );
    Ok(())
}
