use super::*;

#[tokio::test]
async fn authored_transaction_is_atomic_and_preserves_result_positions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([113; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let directory = path("workspace")?;
    let file = path("file.txt")?;
    let transaction = (checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: directory,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: OwnedBytes::from_static(b"initial"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Write {
                path: file.clone(),
                offset: 7,
                bytes: OwnedBytes::from_static(b"-atomic"),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(transaction.value.created_file_ids.len(), 3);
    assert!(transaction.value.created_file_ids[0].is_some());
    assert!(transaction.value.created_file_ids[1].is_some());
    assert!(transaction.value.created_file_ids[2].is_none());
    let read = (checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 14,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(read.value.bytes.as_ref(), b"initial-atomic");

    let failed = (checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("orphan")?,
                bytes: OwnedBytes::from_static(b"never-visible"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Remove {
                path: path("absent")?,
                expected_file_id: None,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await;
    assert!(failed.is_err());
    let absent =
        (checkout.lookup_no_follow(&path("orphan")?, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(absent.value.record.is_none());
    Ok(())
}

#[tokio::test]
async fn authored_bulk_transaction_is_bounded_and_atomic() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_mutations_per_batch = 2;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([244; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let created = (0..5)
        .map(|index| {
            Ok(AuthoredMutation::CreateFile {
                path: path(&format!("bulk{index}"))?,
                bytes: OwnedBytes::from_static(b"ok"),
                metadata: FileMetadata::default(),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    (checkout.apply_authored_bulk_transaction(created, WorkBudget::UNBOUNDED, &cancellation))
        .await?;
    for index in 0..5 {
        assert!(
            (checkout.lookup_no_follow(
                &path(&format!("bulk{index}"))?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .await?
            .value
            .record
            .is_some()
        );
    }
    let failed = (checkout.apply_authored_bulk_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("should-not-appear")?,
                bytes: OwnedBytes::from_static(b"no"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path("should-not-appear-either")?,
                bytes: OwnedBytes::from_static(b"no"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path("bulk0")?,
                bytes: OwnedBytes::from_static(b"duplicate"),
                metadata: FileMetadata::default(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await;
    assert!(failed.is_err());
    assert!(
        (checkout.lookup_no_follow(
            &path("should-not-appear")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?
        .value
        .record
        .is_none()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authored_transaction_keeps_true_ids_and_original_result_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "original-authored-transaction",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace
        .write_text("/source", "transaction source")
        .await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let metadata = checkout
        .lookup_no_follow_with_metadata(&path("source")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .ok_or("real source metadata absent")?
        .metadata;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(&original, &cancellation)?;
    candidate.bind_authored_operation(OperationId::new());
    let first = path("transaction-first")?;
    let second = path("transaction-second")?;
    let expected = [
        Some(candidate.authored_file_id(FileKind::Regular, &first)),
        Some(candidate.authored_file_id(FileKind::Regular, &second)),
    ];
    let input = original_authored_creates(
        &[
            (&first, OwnedBytes::from_static(b"first actual source")),
            (&second, OwnedBytes::from_static(b"second actual source")),
        ],
        metadata,
        &original,
        &cancellation,
    )?;
    let result = candidate
        .apply_authored_transaction_with_accounting(
            crate::kernel::Owned {
                value: input.0,
                bytes: input.1.bytes(),
                allocation: KernelOwnedAllocation::NativeOriginal(input.1),
            },
            &mut KernelAccounting::native(&original, &cancellation),
            &cancellation,
        )
        .await?;
    assert_eq!(result.value.created_file_ids, expected);
    let KernelOwnedAllocation::NativeOriginal(allocation) = &result.allocation else {
        return Err("native authored result lost its original custody".into());
    };
    assert!(allocation.work().shares_ledger(&original));
    assert_eq!(
        allocation.bytes(),
        (result.value.created_file_ids.capacity() * size_of::<Option<FileId>>()) as u64
    );
    let mut accounting =
        crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    for (requested, expected) in [(&first, expected[0]), (&second, expected[1])] {
        let actual = crate::kernel::lookup_path_async_with_accounting(
            &fs.inner.objects,
            &candidate.root,
            requested,
            candidate.volume.config,
            &mut accounting,
            &cancellation,
            None,
        )
        .await?
        .record
        .ok_or("created real record absent")?;
        assert_eq!(Some(actual.file_id), expected);
    }
    let live = original.live_allocation_bytes();
    let result_bytes = allocation.bytes();
    drop(result);
    assert_eq!(original.live_allocation_bytes(), live - result_bytes);
    assert!(
        checkout
            .lookup_no_follow(&first, WorkBudget::UNBOUNDED, &cancellation,)
            .await?
            .value
            .record
            .is_none(),
        "private transaction cannot alter its source checkout"
    );
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
        .create_volume_with_id(
            VolumeId::from_bytes([122; 16]),
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
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
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
    }
    let mut metadata = checkout
        .lookup_no_follow_with_metadata(&source, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .ok_or("real source missing")?
        .metadata;
    metadata.modified_ns = MetadataField::Value(99);
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(&original, &cancellation)?;
    let input = original_borrowed_path_changes(
        &source,
        &destination,
        &removed,
        metadata,
        &original,
        &cancellation,
    )?;
    let state = checkout_state(&candidate);
    let installed = checkout_state(&checkout);
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
    assert_eq!(control.operation.load(Ordering::Relaxed), 2);
    assert_eq!(original.receipt().backend_write_operations, 2);
    assert_eq!(checkout_state(&candidate), state);
    assert_eq!(checkout_state(&checkout), installed);
    assert_eq!(original.live_allocation_bytes(), live);
    assert!(matches!(&input.0[2], AuthoredMutation::Rename {
        source: old_source, destination: old_destination, ..
    } if old_source == &source && old_destination == &destination));
    control.disable();
    for requested in [&source, &destination, &removed] {
        let retained = candidate
            .lookup_no_follow(requested, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("failed candidate lost existing record")?;
        let installed = checkout
            .lookup_no_follow(requested, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("installed source record disappeared")?;
        assert_eq!(retained, installed);
    }
    Ok(())
}

#[tokio::test]
async fn inverse_private_mutations_restore_the_base_and_clear_replay_state()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([136; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let base = checkout.generation_id();
    let transient = path("transient")?;
    (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: transient.clone(),
            bytes: (bytes::Bytes::from_static(b"temporary")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(checkout.has_pending_mutations());
    (checkout.mutate(
        vec![crate::kernel::Mutation::Remove {
            path: transient,
            expected_file_id: (None).map_or(
                crate::kernel::MetadataField::Unavailable,
                crate::kernel::MetadataField::Value,
            ),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(!checkout.has_pending_mutations());
    assert_eq!(checkout.generation_id(), base);
    Ok(())
}

#[tokio::test]
async fn authored_transactions_preflight_expansion_noops_and_byte_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();

    let mut operation_limited = config();
    operation_limited.limits.maximum_mutations_per_batch = 1;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        operation_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let missing_noop = (checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("absent")?,
            offset: 0,
            bytes: OwnedBytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("authored missing noop succeeded")?;
    assert!(matches!(
        missing_noop.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(missing_noop.work.page_reads > 0);

    let noop_fs = Fs::memory();
    let noop_volume = (noop_fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut noop_checkout = (noop_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let ordered = (noop_checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::Write {
                path: path("ordered")?,
                offset: 0,
                bytes: OwnedBytes::new(),
            },
            AuthoredMutation::CreateFile {
                path: path("ordered")?,
                bytes: OwnedBytes::new(),
                metadata: FileMetadata::default(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("ordered noop bypassed its missing source")?;
    assert!(matches!(
        ordered.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(
        (noop_checkout.lookup_no_follow(&path("ordered")?, WorkBudget::UNBOUNDED, &cancellation,))
            .await?
            .value
            .record
            .is_none()
    );

    let direct_missing = (noop_checkout.change_content(
        path("missing")?,
        crate::ContentChange::Write {
            offset: 0,
            bytes: (bytes::Bytes::new()).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("direct missing noop succeeded")?;
    assert!(matches!(
        direct_missing.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));

    let created = (noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("present")?,
            bytes: OwnedBytes::new(),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let file_id = created.value.created_file_ids[0].ok_or("created file id missing")?;
    let generation_before_noop = noop_checkout.generation_id();
    let empty = (noop_checkout.apply_authored_transaction(
        Vec::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(empty.value.created_file_ids.is_empty());
    assert_eq!(empty.work, WorkCounters::default());
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let noop = (noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("present")?,
            offset: 0,
            bytes: OwnedBytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(noop.value.created_file_ids, vec![None]);
    assert!(noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let mut empty_source = std::io::Cursor::new(OwnedBytes::new());
    let empty_content =
        (noop_checkout.stage_content(&mut empty_source, 1, WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value;
    assert_eq!(empty_content.logical_bytes(), 0);
    let content_noop = (noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::WriteFromContent {
            path: path("present")?,
            offset: u64::MAX,
            content: empty_content,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(content_noop.value.created_file_ids, vec![None]);
    assert!(content_noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let direct = (noop_checkout.change_content(
        path("present")?,
        crate::ContentChange::Write {
            offset: u64::MAX,
            bytes: (bytes::Bytes::new()).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(direct.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let identity = (noop_checkout.change_content(
        file_id,
        crate::ContentChange::Write {
            offset: u64::MAX,
            bytes: (bytes::Bytes::new()).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(identity.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);

    let expanded = (checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("large")?,
            bytes: OwnedBytes::from(vec![1; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1]),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("expanded authored transaction unexpectedly succeeded")?;
    assert!(matches!(expanded.error, FsError::TooManyPendingMutations));
    assert_eq!(*expanded.work, WorkCounters::default());
    assert!(
        (checkout.lookup_no_follow(&path("large")?, WorkBudget::UNBOUNDED, &cancellation,))
            .await?
            .value
            .record
            .is_none()
    );

    let mut byte_limited = config();
    byte_limited.limits.maximum_read_bytes = 1;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        byte_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
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
        let failure = (checkout.apply_authored_transaction(
            vec![mutation],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("byte-limited authored transaction unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    let direct = (checkout.create(
        crate::AuthoredMutation::CreateSymbolicLink {
            path: path("direct-link")?,
            target: (bytes::Bytes::from_static(b"xx")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("byte-limited direct symlink unexpectedly succeeded")?;
    assert!(matches!(
        direct.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*direct.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn authored_transactions_cover_every_portable_operation_without_hidden_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([114; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;

    let mut create_source = std::io::Cursor::new(OwnedBytes::from_static(b"streamed"));
    let staged_create =
        (checkout.stage_content(&mut create_source, 64, WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value;
    let mut write_source = std::io::Cursor::new(OwnedBytes::from_static(b"TAIL"));
    let staged_write =
        (checkout.stage_content(&mut write_source, 64, WorkBudget::UNBOUNDED, &cancellation))
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
        WorkBudget::UNBOUNDED,
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
            WorkBudget::UNBOUNDED,
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
            WorkBudget::UNBOUNDED,
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
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?
        .value
        .bytes
        .as_ref(),
        large.as_ref()
    );
    assert_eq!(
        (checkout.read_symbolic_link(&path("symbolic")?, WorkBudget::UNBOUNDED, &cancellation,))
            .await?
            .value
            .bytes
            .as_ref(),
        b"../source"
    );
    assert_eq!(
        (checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,))
            .await?
            .value
            .modified_ns,
        MetadataField::Value(42)
    );
    Ok(())
}

mod limits;
