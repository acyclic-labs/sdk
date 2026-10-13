use super::*;

#[tokio::test]
async fn authored_transaction_is_atomic_and_preserves_result_positions() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    let transaction = checkout
        .apply_authored_transaction(
            vec![
                AuthoredMutation::CreateDirectory {
                    path: path("workspace")?,
                    metadata: FileMetadata::default(),
                },
                AuthoredMutation::CreateFile {
                    path: path("file.txt")?,
                    bytes: OwnedBytes::from_static(b"initial"),
                    metadata: FileMetadata::default(),
                },
                AuthoredMutation::Write {
                    path: path("file.txt")?,
                    offset: 7,
                    bytes: OwnedBytes::from_static(b"-atomic"),
                },
            ],
            B,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(transaction.created_file_ids.len(), 3);
    assert!(transaction.created_file_ids[0].is_some() && transaction.created_file_ids[1].is_some());
    assert!(transaction.created_file_ids[2].is_none());
    assert_eq!(
        read(
            &mut checkout,
            transaction.created_file_ids[1].unwrap(),
            0,
            14,
            &cancel
        )
        .await?
        .as_ref(),
        b"initial-atomic"
    );
    assert!(
        checkout
            .apply_authored_transaction(
                vec![
                    AuthoredMutation::CreateFile {
                        path: path("orphan")?,
                        bytes: OwnedBytes::from_static(b"never-visible"),
                        metadata: FileMetadata::default()
                    },
                    AuthoredMutation::Remove {
                        path: path("absent")?,
                        expected_file_id: None
                    },
                ],
                B,
                &cancel
            )
            .await
            .is_err()
    );
    assert!(
        checkout
            .lookup_no_follow(&path("orphan")?, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authored_transaction_keeps_true_ids_and_original_result_custody() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    create(
        &mut checkout,
        "source",
        OwnedBytes::from_static(b"transaction source"),
        &cancel,
    )
    .await?;
    commit(&mut checkout, &cancel).await?;
    let metadata = checkout
        .read_metadata(&path("source")?, B, &cancel)
        .await?
        .value;
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(&original, &cancel)?;
    candidate.bind_authored_operation(OperationId::new());
    let first = path("transaction-first")?;
    let second = path("transaction-second")?;
    let input = original_authored_creates(
        &[
            (&first, OwnedBytes::from_static(b"first actual source")),
            (&second, OwnedBytes::from_static(b"second actual source")),
        ],
        metadata,
        &original,
        &cancel,
    )?;
    let result = candidate
        .apply_authored_transaction_with_accounting(
            crate::kernel::Owned {
                value: input.0,
                bytes: input.1.bytes(),
                allocation: KernelOwnedAllocation::NativeOriginal(input.1),
            },
            &mut KernelAccounting::native(&original, &cancel),
            &cancel,
        )
        .await?;
    let expected = &result.value.created_file_ids;
    assert_eq!(expected.len(), 2);
    assert!(expected.iter().all(Option::is_some));
    assert_ne!(expected[0], expected[1]);
    let KernelOwnedAllocation::NativeOriginal(allocation) = &result.allocation else {
        return Err("native result lost original custody".into());
    };
    assert!(allocation.work().shares_ledger(&original));
    for (path, id) in [(&first, expected[0]), (&second, expected[1])] {
        assert_eq!(
            Some(
                candidate
                    .lookup_no_follow(path, &original, &cancel)
                    .await?
                    .record
                    .unwrap()
                    .file_id
            ),
            id
        );
    }
    let live = original.live_allocation_bytes();
    let result_bytes = allocation.bytes();
    drop(result);
    assert_eq!(original.live_allocation_bytes(), live - result_bytes);
    assert!(
        checkout
            .lookup_no_follow(&first, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn inverse_private_mutations_restore_the_base_and_clear_replay_state() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    let base = checkout.generation_id();
    create(
        &mut checkout,
        "transient",
        OwnedBytes::from_static(b"temporary"),
        &cancel,
    )
    .await?;
    assert!(checkout.has_pending_mutations());
    checkout
        .mutate(
            vec![Mutation::Remove {
                path: path("transient")?,
                expected_file_id: MetadataField::Unavailable,
            }],
            B,
            &cancel,
        )
        .await?;
    assert!(!checkout.has_pending_mutations());
    assert_eq!(checkout.generation_id(), base);
    Ok(())
}

#[tokio::test]
async fn authored_transactions_preflight_expansion_noops_and_byte_bounds() -> TestResult {
    let cancel = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_mutations_per_batch = 1;
    let (_, mut checkout) = fixture(limited, &cancel).await?;
    assert!(matches!(
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::Write {
                    path: path("absent")?,
                    offset: 0,
                    bytes: OwnedBytes::new()
                }],
                B,
                &cancel
            )
            .await,
        Err(OperationFailure {
            error: FsError::Mutation(GenerationMutationError::MissingSource),
            ..
        })
    ));
    let (_, mut noop) = fixture(config(), &cancel).await?;
    assert!(matches!(
        noop.apply_authored_transaction(
            vec![
                AuthoredMutation::Write {
                    path: path("ordered")?,
                    offset: 0,
                    bytes: OwnedBytes::new()
                },
                AuthoredMutation::CreateFile {
                    path: path("ordered")?,
                    bytes: OwnedBytes::new(),
                    metadata: FileMetadata::default()
                }
            ],
            B,
            &cancel
        )
        .await,
        Err(OperationFailure {
            error: FsError::Mutation(GenerationMutationError::MissingSource),
            ..
        })
    ));
    assert!(
        noop.lookup_no_follow(&path("ordered")?, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    assert!(matches!(
        noop.change_content(
            path("missing")?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::new()
            },
            ContentTimes::Preserve,
            B,
            &cancel
        )
        .await,
        Err(OperationFailure {
            error: FsError::Mutation(GenerationMutationError::MissingSource),
            ..
        })
    ));
    let id = create(&mut noop, "present", OwnedBytes::new(), &cancel).await?;
    let generation = noop.generation_id();
    assert!(
        noop.apply_authored_transaction(Vec::new(), B, &cancel)
            .await?
            .value
            .created_file_ids
            .is_empty()
    );
    assert_eq!(
        noop.apply_authored_transaction(
            vec![AuthoredMutation::Write {
                path: path("present")?,
                offset: 0,
                bytes: OwnedBytes::new()
            }],
            B,
            &cancel
        )
        .await?
        .value
        .created_file_ids,
        vec![None]
    );
    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    let content = noop.stage_content(&mut empty, 1, B, &cancel).await?.value;
    assert_eq!(content.logical_bytes(), 0);
    assert_eq!(
        noop.apply_authored_transaction(
            vec![AuthoredMutation::WriteFromContent {
                path: path("present")?,
                offset: u64::MAX,
                content
            }],
            B,
            &cancel
        )
        .await?
        .value
        .created_file_ids,
        vec![None]
    );
    noop.change_content(
        path("present")?,
        ContentChange::Write {
            offset: u64::MAX,
            bytes: OwnedBytes::new(),
        },
        ContentTimes::Preserve,
        B,
        &cancel,
    )
    .await?;
    noop.change_content(
        id,
        ContentChange::Write {
            offset: u64::MAX,
            bytes: OwnedBytes::new(),
        },
        ContentTimes::Preserve,
        B,
        &cancel,
    )
    .await?;
    assert_eq!(noop.generation_id(), generation);
    assert!(matches!(
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: path("large")?,
                    bytes: vec![1; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1].into(),
                    metadata: FileMetadata::default()
                }],
                B,
                &cancel
            )
            .await,
        Err(OperationFailure {
            error: FsError::TooManyPendingMutations,
            ..
        })
    ));
    assert!(
        checkout
            .lookup_no_follow(&path("large")?, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    limited = config();
    limited.limits.maximum_read_bytes = 1;
    let (_, mut checkout) = fixture(limited, &cancel).await?;
    for mutation in [
        AuthoredMutation::CreateFile {
            path: path("created-file")?,
            bytes: OwnedBytes::from_static(b"xx"),
            metadata: FileMetadata::default(),
        },
        AuthoredMutation::Write {
            path: path("file")?,
            offset: 0,
            bytes: OwnedBytes::from_static(b"xx"),
        },
        AuthoredMutation::CreateSymbolicLink {
            path: path("link")?,
            target: OwnedBytes::from_static(b"xx"),
            metadata: FileMetadata::default(),
        },
    ] {
        assert!(matches!(
            checkout
                .apply_authored_transaction(vec![mutation], B, &cancel)
                .await,
            Err(OperationFailure {
                error: FsError::FileRead(FileRangeReadError::InvalidRange),
                ..
            })
        ));
    }
    assert!(matches!(
        checkout
            .create(
                AuthoredMutation::CreateSymbolicLink {
                    path: path("direct-link")?,
                    target: OwnedBytes::from_static(b"xx"),
                    metadata: FileMetadata::default()
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
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn borrowed_authored_second_staged_write_failure_keeps_candidate_source_and_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let control = Arc::new(FaultControl::disabled());
    let objects = Arc::new(crate::memory::MemoryObjectStore::default());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        PostPutObjectStore {
            inner: Arc::clone(&objects),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let volume = fs
        .create_volume_with_id(VolumeId::from_bytes([122; 16]), config(), B, &cancellation)
        .await?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            B,
            &cancellation,
        )
        .await?
        .value;
    let source = path("source")?;
    let destination = path("destination")?;
    let removed = path("removed")?;
    for (requested, body) in [
        (&source, b"source body".as_slice()),
        (&destination, b"destination body".as_slice()),
        (&removed, b"removed body".as_slice()),
    ] {
        checkout
            .create(
                crate::AuthoredMutation::CreateFile {
                    path: requested.clone(),
                    bytes: (bytes::Bytes::copy_from_slice(body)).into(),
                    metadata: crate::kernel::FileMetadata::default(),
                },
                B,
                &cancellation,
            )
            .await?;
    }
    let mut metadata = checkout
        .lookup_no_follow_with_metadata(&source, B, &cancellation)
        .await?
        .value
        .ok_or("real source missing")?
        .metadata;
    metadata.modified_ns = MetadataField::Value(99);
    let original =
        crate::NativeMaterializationWork::new(B, &cancellation).map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(&original, &cancellation)?;
    let input = original_borrowed_path_changes(
        &source,
        &destination,
        &removed,
        metadata,
        &original,
        &cancellation,
    )?;
    let live = original.live_allocation_bytes();
    control.arm(2);
    let failure = candidate
        .apply_borrowed_authored_transaction_with_native_work(
            &input.0,
            &input.1,
            &original,
            &cancellation,
        )
        .await
        .err()
        .ok_or("second staged write unexpectedly succeeded")?;
    assert!(matches!(
        failure,
        FsError::Object(ObjectStoreError::Corrupt)
    ));
    assert_eq!(original.live_allocation_bytes(), live);
    assert!(matches!(&input.0[2], AuthoredMutation::Rename {
        source: old_source, destination: old_destination, ..
    } if old_source == &source && old_destination == &destination));
    control.disable();
    for requested in [&source, &destination, &removed] {
        let retained = candidate
            .lookup_no_follow(requested, B, &cancellation)
            .await?
            .value
            .record
            .ok_or("failed candidate lost existing record")?;
        let installed = checkout
            .lookup_no_follow(requested, B, &cancellation)
            .await?
            .value
            .record
            .ok_or("installed source record disappeared")?;
        assert_eq!(retained, installed);
    }
    Ok(())
}

#[tokio::test]
async fn authored_transactions_cover_every_portable_operation_without_hidden_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancellation).await?;
    let mut create_source = std::io::Cursor::new(OwnedBytes::from_static(b"streamed"));
    let staged_create = (checkout.stage_content(&mut create_source, 64, B, &cancellation))
        .await?
        .value;
    let mut write_source = std::io::Cursor::new(OwnedBytes::from_static(b"TAIL"));
    let staged_write = (checkout.stage_content(&mut write_source, 64, B, &cancellation))
        .await?
        .value;
    assert_eq!(staged_create.logical_bytes(), 8);
    assert_eq!(staged_create.root().kind, ObjectKind::Blob);
    assert_eq!(staged_write.logical_bytes(), 4);
    assert_eq!(staged_write.root().kind, ObjectKind::Blob);

    let source = path("source")?;
    let destination = path("destination")?;
    let streamed = path("streamed")?;
    let streamed_identity = FileId::from_bytes([199; 16]);
    let hard_link = path("hard-link")?;
    let renamed = path("renamed")?;
    let mut replacement_metadata = FileMetadata::default();
    replacement_metadata.modified_ns = MetadataField::Value(42);
    let large = OwnedBytes::from(vec![0x5a; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1]);
    let transaction = (checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("directory")?,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: source.clone(),
                bytes: OwnedBytes::from_static(b"abcdefgh"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: destination.clone(),
                bytes: OwnedBytes::from_static(b"................"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path("large")?,
                bytes: large.clone(),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFileFromContent {
                path: streamed.clone(),
                content: staged_create,
                metadata: FileMetadata::default(),
                file_id: Some(streamed_identity),
            },
            AuthoredMutation::CreateSymbolicLink {
                path: path("symbolic")?,
                target: OwnedBytes::from_static(b"../source"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 0,
                bytes: OwnedBytes::new(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 6,
                bytes: OwnedBytes::from_static(b"XY"),
            },
            AuthoredMutation::WriteFromContent {
                path: streamed.clone(),
                offset: 8,
                content: staged_write,
            },
            AuthoredMutation::SetMetadata {
                path: source.clone(),
                metadata: replacement_metadata,
            },
            AuthoredMutation::Resize {
                path: destination.clone(),
                logical_bytes: 24,
            },
            AuthoredMutation::ZeroRange {
                path: source.clone(),
                range: ByteRange {
                    offset: 2,
                    length: 2,
                },
                allocated: false,
                extend: false,
            },
            AuthoredMutation::Preallocate {
                path: destination.clone(),
                range: ByteRange {
                    offset: 16,
                    length: 8,
                },
                keep_size: false,
            },
            AuthoredMutation::CloneRange(FileCloneRequest {
                source: source.clone(),
                source_offset: 0,
                destination: destination.clone(),
                destination_offset: 4,
                length: 8,
            }),
            AuthoredMutation::HardLink {
                source: source.clone(),
                destination: hard_link.clone(),
            },
            AuthoredMutation::Rename {
                source: hard_link,
                destination: renamed.clone(),
                replace: false,
            },
            AuthoredMutation::Remove {
                path: renamed,
                expected_file_id: None,
            },
        ],
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(transaction.value.created_file_ids.len(), 17);
    assert_eq!(
        transaction.value.created_file_ids.get(4),
        Some(&Some(streamed_identity))
    );
    assert_eq!(
        transaction
            .value
            .created_file_ids
            .iter()
            .filter(|created| created.is_some())
            .count(),
        6
    );
    assert_eq!(
        (checkout.read_file_range(
            &streamed,
            ByteRange {
                offset: 0,
                length: 12,
            },
            B,
            &cancellation,
        ))
        .await?
        .value
        .bytes
        .as_ref(),
        b"streamedTAIL"
    );
    assert!(
        (checkout.apply_authored_transaction(
            vec![AuthoredMutation::CreateFileFromContent {
                path: path("duplicate-source-identity")?,
                content: staged_create,
                metadata: FileMetadata::default(),
                file_id: Some(streamed_identity),
            }],
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert_eq!(
        (checkout.read_file_range(
            &path("large")?,
            ByteRange {
                offset: 0,
                length: u64::try_from(large.len())?,
            },
            B,
            &cancellation,
        ))
        .await?
        .value
        .bytes,
        large
    );
    assert_eq!(
        (checkout.read_symbolic_link(&path("symbolic")?, B, &cancellation,))
            .await?
            .value
            .bytes
            .as_ref(),
        b"../source"
    );
    assert_eq!(
        (checkout.read_metadata(&source, B, &cancellation,))
            .await?
            .value
            .modified_ns,
        MetadataField::Value(42)
    );
    Ok(())
}

mod limits;
