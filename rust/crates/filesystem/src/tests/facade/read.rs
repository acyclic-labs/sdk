use super::*;

#[tokio::test]
async fn pinned_reader_batches_ranges_in_request_order() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut writer) = fixture(config(), &cancel).await?;
    for (name, bytes) in [("first", b"abcdefgh".as_slice()), ("second", b"01234567")] {
        create(
            &mut writer,
            name,
            OwnedBytes::copy_from_slice(bytes),
            &cancel,
        )
        .await?;
    }
    writer
        .create(
            AuthoredMutation::CreateSymbolicLink {
                path: path("link")?,
                target: OwnedBytes::from_static(b"first"),
                metadata: FileMetadata::default(),
            },
            B,
            &cancel,
        )
        .await?;
    for name in ["first-directory", "second-directory"] {
        writer
            .create(
                AuthoredMutation::CreateDirectory {
                    path: path(name)?,
                    metadata: FileMetadata::default(),
                },
                B,
                &cancel,
            )
            .await?;
    }
    for (directory, name, bytes) in [
        ("first-directory", "a", b"x".as_slice()),
        ("first-directory", "b", b"x"),
        ("second-directory", "c", b"y"),
    ] {
        writer
            .create(
                AuthoredMutation::CreateFile {
                    path: path_parts(&[directory, name])?,
                    bytes: OwnedBytes::copy_from_slice(bytes),
                    metadata: FileMetadata::default(),
                },
                B,
                &cancel,
            )
            .await?;
    }
    let reader = writer.pinned_reader()?;
    let resolved = reader
        .resolve_files(&[path("second")?, path("first")?], B, &cancel)
        .await?
        .value;
    let second = resolved[0].as_ref().unwrap();
    let first = resolved[1].as_ref().unwrap();
    let requests = [
        ResolvedFileRangeReadRequest {
            file: second,
            range: ByteRange {
                offset: 2,
                length: 3,
            },
        },
        ResolvedFileRangeReadRequest {
            file: first,
            range: ByteRange {
                offset: 1,
                length: 4,
            },
        },
    ];
    let batch = reader
        .read_resolved_ranges(&requests, 2, B, &cancel)
        .await?;
    assert_eq!(
        batch
            .value
            .iter()
            .map(|read| read.bytes.as_ref())
            .collect::<Vec<_>>(),
        vec![b"234".as_slice(), b"bcde"]
    );
    let descriptions = reader
        .describe_files(
            &[path("first")?, path("second")?, path("first")?],
            B,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(descriptions.len(), 3);
    assert!(descriptions.iter().all(Option::is_some));
    assert_eq!(descriptions[0], descriptions[2]);
    assert_eq!(
        first
            .read_range(requests[1].range, B, &cancel)
            .await?
            .value
            .bytes
            .as_ref(),
        b"bcde"
    );
    let (_, mut foreign) = fixture(config(), &cancel).await?;
    create(
        &mut foreign,
        "foreign",
        OwnedBytes::from_static(b"WXYZ"),
        &cancel,
    )
    .await?;
    let foreign_reader = foreign.pinned_reader()?;
    let foreign_files = foreign_reader
        .resolve_files(&[path("foreign")?], B, &cancel)
        .await?
        .value;
    let mixed = [
        ResolvedFileRangeReadRequest {
            file: first,
            range: ByteRange {
                offset: 0,
                length: 2,
            },
        },
        ResolvedFileRangeReadRequest {
            file: foreign_files[0].as_ref().unwrap(),
            range: ByteRange {
                offset: 1,
                length: 2,
            },
        },
    ];
    let result = reader.read_resolved_ranges(&mixed, 2, B, &cancel).await?;
    assert_eq!(
        result
            .value
            .iter()
            .map(|read| read.bytes.as_ref())
            .collect::<Vec<_>>(),
        vec![b"ab".as_slice(), b"XY"]
    );
    assert_eq!(
        reader
            .read_resolved_ranges(&mixed, 2, result.work, &cancel)
            .await?
            .value,
        result.value
    );
    assert_eq!(
        reader
            .read_symbolic_link(&path("link")?, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"first"
    );
    assert!(matches!(
        reader.read_symbolic_link(&path("first")?, B, &cancel).await,
        Err(OperationFailure {
            error: FsError::NotSymbolicLink,
            ..
        })
    ));
    let pages = reader
        .list_directory_pages(
            &[
                DirectoryPageRequest {
                    path: path("second-directory")?,
                    after: None,
                    maximum_entries: 8,
                },
                DirectoryPageRequest {
                    path: path("first-directory")?,
                    after: Some(LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?),
                    maximum_entries: 1,
                },
            ],
            2,
            B,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(pages[0].entries[0].name.as_bytes(), b"c");
    assert_eq!(pages[1].entries[0].name.as_bytes(), b"b");
    assert!(pages.iter().all(|page| !page.has_more));
    let page = reader
        .resolve_directory_page(&path("first-directory")?, None, 1, B, &cancel)
        .await?
        .value;
    assert_eq!(page.entries.len(), 1);
    assert!(page.has_more);
    let entry = &page.entries[0];
    assert_eq!(entry.name.as_bytes(), b"a");
    assert_eq!(entry.file.description().kind, FileKind::Regular);
    assert_eq!(
        entry
            .file
            .read_range(
                ByteRange {
                    offset: 0,
                    length: 1
                },
                B,
                &cancel
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"x"
    );
    assert!(
        reader
            .list_directory_pages(&[], 0, B, &cancel)
            .await
            .is_err()
    );
    let one = reader
        .read_file_range(&path("second")?, requests[0].range, B, &cancel)
        .await?;
    let failure = reader
        .read_resolved_ranges(&requests, 2, one.work, &cancel)
        .await
        .unwrap_err();
    failure.work.verify(one.work)?;
    let invalid = [
        ResolvedFileRangeReadRequest {
            file: second,
            range: ByteRange {
                offset: 0,
                length: u64::MAX,
            },
        },
        ResolvedFileRangeReadRequest {
            file: first,
            range: requests[1].range,
        },
    ];
    assert!(
        reader
            .read_resolved_ranges(&invalid, 2, B, &cancel)
            .await
            .is_err()
    );
    assert!(matches!(
        volume
            .checkout(GenerationSelector::Head, tracking(), B, &cancel)
            .await?
            .value
            .pinned_reader(),
        Err(FsError::MutationNotAllowed)
    ));
    Ok(())
}

#[tokio::test]
async fn checkout_snapshot_reader_is_private_and_stable() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, _) = fixture(config(), &cancel).await?;
    let mut checkout = volume
        .checkout(GenerationSelector::Head, writable_tracking(), B, &cancel)
        .await?
        .value;
    let id = create(
        &mut checkout,
        "private",
        OwnedBytes::from_static(b"first"),
        &cancel,
    )
    .await?;
    let snapshot = checkout.snapshot_reader();
    let resolved = snapshot
        .resolve_files(&[path("private")?], B, &cancel)
        .await?
        .value
        .pop()
        .flatten()
        .unwrap();
    assert_eq!(resolved.file_id(), id);
    checkout
        .change_content(
            path("private")?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"later"),
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    assert_eq!(
        resolved
            .read_range(
                ByteRange {
                    offset: 0,
                    length: 5
                },
                B,
                &cancel
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"first"
    );
    Ok(())
}

#[tokio::test]
async fn opaque_payload_reads_enforce_the_volume_output_bound() -> TestResult {
    let cancel = CancellationToken::new();
    let mut limited = config();
    limited.profile = FilesystemProfile::Windows;
    limited.limits.maximum_read_bytes = 1;
    let (_, mut checkout) = fixture(limited, &cancel).await?;
    let mut source = std::io::Cursor::new(OwnedBytes::from_static(b"xx"));
    let staged = checkout
        .stage_content(&mut source, 2, B, &cancel)
        .await?
        .value;
    let metadata = checkout
        .lookup_no_follow(&root_path()?, B, &cancel)
        .await?
        .value
        .record
        .unwrap()
        .metadata;
    checkout
        .mutate(
            vec![
                Mutation::Create {
                    path: path("symbolic")?,
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
                    path: path("reparse")?,
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
            &cancel,
        )
        .await?;
    for result in [
        checkout
            .read_symbolic_link(&path("symbolic")?, B, &cancel)
            .await
            .map(|_| ()),
        checkout
            .read_reparse_point(&path("reparse")?, B, &cancel)
            .await
            .map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(OperationFailure {
                error: FsError::FileRead(FileRangeReadError::InvalidRange),
                ..
            })
        ));
    }
    Ok(())
}

#[tokio::test]
async fn public_query_and_identity_failures_preserve_the_checkout_candidate() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, _) = fixture(config(), &cancel).await?;
    let mut checkout = volume
        .checkout(GenerationSelector::Head, writable_tracking(), B, &cancel)
        .await?
        .value;
    let id = create(
        &mut checkout,
        "regular",
        OwnedBytes::from_static(b"data"),
        &cancel,
    )
    .await?;
    let directory = path("directory")?;
    let directory_id = checkout
        .create(
            AuthoredMutation::CreateDirectory {
                path: directory.clone(),
                metadata: FileMetadata::default(),
            },
            B,
            &cancel,
        )
        .await?
        .value;
    let page = checkout
        .list_directory_records(&directory, std::ops::Bound::Unbounded, 8, B, &cancel)
        .await?
        .value;
    assert!(page.entries.is_empty() && !page.has_more);
    let unknown = FileId::from_bytes([250; 16]);
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.missing".to_vec(),
        128,
    )?;
    assert!(matches!(
        checkout
            .read_file_range_by_id(
                id,
                ByteRange {
                    offset: 0,
                    length: config().limits.maximum_read_bytes + 1
                },
                B,
                &cancel
            )
            .await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    assert!(matches!(
        checkout
            .plan_file_extents_by_id(
                id,
                ByteRange {
                    offset: 0,
                    length: 1
                },
                0,
                B,
                &cancel
            )
            .await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    for (offset, target, expected) in [
        (0, ExtentSeekTarget::Data, Some(0)),
        (0, ExtentSeekTarget::Hole, Some(4)),
        (4, ExtentSeekTarget::Data, None),
        (4, ExtentSeekTarget::Hole, Some(4)),
        (5, ExtentSeekTarget::Data, None),
        (5, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            checkout
                .seek_file_extent_by_id(id, offset, target, B, &cancel)
                .await?
                .value,
            expected
        );
        assert_eq!(
            checkout
                .seek_file_extent(&path("regular")?, offset, target, B, &cancel)
                .await?
                .value,
            expected
        );
    }
    assert!(matches!(
        checkout
            .seek_file_extent_by_id(directory_id, 0, ExtentSeekTarget::Data, B, &cancel)
            .await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::NotRegular),
            ..
        })
    ));
    assert!(
        checkout
            .read_named_attribute_by_id(id, &attribute, B, &cancel)
            .await?
            .value
            .is_none()
    );
    assert!(
        checkout
            .list_named_attributes_by_id(id, None, 8, B, &cancel)
            .await?
            .value
            .entries
            .is_empty()
    );
    for mode in [
        NamedAttributeWriteMode::Replace,
        NamedAttributeWriteMode::Create,
    ] {
        if mode == NamedAttributeWriteMode::Create {
            checkout
                .change_named_attribute_by_id(
                    id,
                    &attribute,
                    Some((&OwnedBytes::from_static(b"first"), mode)),
                    B,
                    &cancel,
                )
                .await?;
        }
        assert!(matches!(
            checkout
                .change_named_attribute_by_id(
                    id,
                    &attribute,
                    Some((&OwnedBytes::from_static(b"second"), mode)),
                    B,
                    &cancel
                )
                .await,
            Err(OperationFailure {
                error: FsError::CreationRejected,
                ..
            })
        ));
    }
    assert_eq!(
        checkout
            .read_named_attribute_by_id(id, &attribute, B, &cancel)
            .await?
            .value
            .unwrap()
            .bytes
            .as_ref(),
        b"first"
    );
    checkout
        .change_named_attribute_by_id(id, &attribute, None, B, &cancel)
        .await?;
    assert!(matches!(
        checkout
            .change_named_attribute_by_id(id, &attribute, None, B, &cancel)
            .await,
        Err(OperationFailure {
            error: FsError::NotFound,
            ..
        })
    ));
    let file = path("regular")?;
    let missing = path("missing")?;
    assert!(
        checkout
            .list_directory(&file, None, 8, B, &cancel)
            .await
            .is_err()
    );
    assert!(
        checkout
            .list_directory_records(&file, std::ops::Bound::Unbounded, 8, B, &cancel)
            .await
            .is_err()
    );
    for (file, offset) in [(&directory, 0), (&file, 5)] {
        assert!(
            checkout
                .read_file_range(file, ByteRange { offset, length: 1 }, B, &cancel)
                .await
                .is_err()
        );
    }
    assert!(
        checkout
            .plan_file_extents(
                &directory,
                ByteRange {
                    offset: 0,
                    length: 1
                },
                8,
                B,
                &cancel
            )
            .await
            .is_err()
    );
    assert!(
        checkout
            .seek_file_extent(&directory, 0, ExtentSeekTarget::Data, B, &cancel)
            .await
            .is_err()
    );
    assert!(
        checkout
            .read_symbolic_link(&file, B, &cancel)
            .await
            .is_err()
    );
    assert!(
        checkout
            .read_reparse_point(&file, B, &cancel)
            .await
            .is_err()
    );
    assert!(checkout.read_metadata(&missing, B, &cancel).await.is_err());
    assert!(
        checkout
            .read_named_attribute(&missing, &attribute, B, &cancel)
            .await
            .is_err()
    );
    assert!(
        checkout
            .list_named_attributes(&missing, None, 8, B, &cancel)
            .await
            .is_err()
    );
    assert!(
        checkout
            .change_named_attribute(missing, &attribute, None, B, &cancel)
            .await
            .is_err()
    );
    assert!(
        checkout
            .read_file_record_by_id(unknown, B, &cancel)
            .await
            .is_err()
    );
    for (id, offset) in [(directory_id, 0), (id, 5)] {
        assert!(
            checkout
                .read_file_range_by_id(id, ByteRange { offset, length: 1 }, B, &cancel)
                .await
                .is_err()
        );
    }
    assert!(
        checkout
            .plan_file_extents_by_id(
                directory_id,
                ByteRange {
                    offset: 0,
                    length: 1
                },
                8,
                B,
                &cancel
            )
            .await
            .is_err()
    );
    assert!(
        checkout
            .read_metadata_by_id(unknown, B, &cancel)
            .await
            .is_err()
    );
    assert!(
        checkout
            .read_named_attribute_by_id(unknown, &attribute, B, &cancel)
            .await
            .is_err()
    );
    assert!(
        checkout
            .list_named_attributes_by_id(unknown, None, 8, B, &cancel)
            .await
            .is_err()
    );
    assert!(
        checkout
            .change_content(
                directory_id,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"x")
                },
                ContentTimes::Preserve,
                B,
                &cancel
            )
            .await
            .is_err()
    );
    assert!(
        checkout
            .change_named_attribute_by_id(unknown, &attribute, None, B, &cancel)
            .await
            .is_err()
    );
    assert!(checkout.has_pending_mutations());
    assert_eq!(
        read(&mut checkout, id, 0, 4, &cancel).await?.as_ref(),
        b"data"
    );
    Ok(())
}
