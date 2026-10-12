use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_identity_mutation_preserves_source_and_cancelled_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    use crate::kernel::allocation::KernelAccounting;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("original-identity-candidate", B, &cancel)
        .await?
        .value;
    workspace
        .write_text("/source", "actual source bytes")
        .await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            B,
            &cancellation,
        )
        .await?
        .value;
    let path = path("source")?;
    let source = checkout
        .lookup_no_follow(&path, B, &cancellation)
        .await?
        .value
        .record
        .ok_or("actual source record absent")?;
    let original =
        crate::NativeMaterializationWork::new(B, &cancellation).map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(&original, &cancellation)?;
    let paid = |mutation| -> Result<crate::kernel::Owned<Vec<Mutation>>, FsError> {
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<Mutation>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        operations.value.push(Mutation::File {
            file_id: source.file_id,
            mutation,
        });
        Ok(operations)
    };
    let input = paid(FileMutation::Resize { logical_bytes: 0 })?;
    candidate
        .mutate_owned_with_accounting(
            input,
            &mut KernelAccounting::native(&original, &cancellation),
            &cancellation,
        )
        .await?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let current = crate::kernel::lookup_file_record_async_with_accounting(
        &fs.inner.objects,
        candidate.root.file_table,
        source.file_id,
        DecodeLimits::for_volume(candidate.volume.config),
        &mut accounting,
        &cancellation,
        None,
    )
    .await?;
    let current = current.0.ok_or("mutated identity disappeared")?;
    assert_eq!(current.file_id, source.file_id);
    assert_ne!(current.payload, source.payload);
    assert!(candidate.root.same_original_scope(&original.state));
    assert_eq!(candidate.base_file_table, checkout.root.file_table);
    assert_eq!(
        checkout
            .lookup_no_follow(&path, B, &cancellation,)
            .await?
            .value
            .record,
        Some(source)
    );
    let before = (
        candidate.root.file_table,
        candidate.base_file_table,
        candidate.authored_operation_id,
        candidate.live_operation_id,
    );
    let baseline = original.live_allocation_bytes();
    let input = paid(FileMutation::Resize { logical_bytes: 7 })?;
    let before_io = original.receipt();
    cancellation.cancel();
    let failure = candidate
        .mutate_owned_with_accounting(
            input,
            &mut KernelAccounting::native(&original, &cancellation),
            &cancellation,
        )
        .await
        .err()
        .ok_or("cancelled mutation changed candidate")?;
    assert!(matches!(
        failure,
        FsError::Cancelled(_)
            | FsError::NativeKernel(crate::NativeTreeMaterializationError::Cancelled)
    ));
    assert_eq!(
        before,
        (
            candidate.root.file_table,
            candidate.base_file_table,
            candidate.authored_operation_id,
            candidate.live_operation_id,
        )
    );
    assert_eq!(
        original.receipt().backend_read_operations,
        before_io.backend_read_operations
    );
    assert_eq!(
        original.receipt().backend_write_operations,
        before_io.backend_write_operations
    );
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn identity_writes_rebase_without_namespace_lookup_and_conflict_by_exact_range()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([111; 16]), config(), B, &cancellation))
            .await?
            .value;
    let file = path("identity")?;
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
            path: file.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    (seed.commit(
        OperationId::from_bytes([112; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let mut remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (local.change_content(
        file_id,
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"LOCAL"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (remote.change_content(
        file.clone(),
        ContentChange::Write {
            offset: 64,
            bytes: OwnedBytes::from_static(b"REMOTE"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (remote.commit(
        OperationId::from_bytes([113; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let safe = (local.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let bytes = (local.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(&bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&bytes.value.bytes[64..70], b"REMOTE");
    let plan = (local.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        8,
        B,
        &cancellation,
    ))
    .await?;
    assert!(plan.value.is_some());

    let mut overlapping_local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let mut overlapping_remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (overlapping_local.change_content(
        file_id,
        ContentChange::Write {
            offset: 10,
            bytes: OwnedBytes::from_static(b"local"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (overlapping_remote.change_content(
        file,
        ContentChange::Write {
            offset: 12,
            bytes: OwnedBytes::from_static(b"remote"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (overlapping_remote.commit(
        OperationId::from_bytes([114; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let conflict = (overlapping_local.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { file_id: actual, .. } if actual == file_id
            ))
    ));

    let mut extending = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (extending.change_content(
        file_id,
        ContentChange::Write {
            offset: 128,
            bytes: OwnedBytes::from_static(b"TAIL"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (extending.change_content(
        file_id,
        ContentChange::CloneFrom {
            source: file_id,
            source_offset: 0,
            offset: 140,
            length: 4,
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    let extended = (extending.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 128,
            length: 16,
        },
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(&extended.value.bytes[..4], b"TAIL");
    assert_eq!(&extended.value.bytes[4..12], &[0; 8]);
    assert_eq!(&extended.value.bytes[12..], b"aaaa");
    Ok(())
}

#[tokio::test]
async fn public_identity_and_posix_special_surfaces_share_one_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            ($future).await?
        };
    }

    let mut volume_config = config();
    volume_config.profile = FilesystemProfile::Posix;
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume(volume_config, B, &cancellation,));
    let mut checkout = ready!(volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ));

    let file = path("identity.bin")?;
    let clone = path("clone.bin")?;
    let file_id = ready!(checkout.value.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from_static(b"abcdefgh"),
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ))
    .value;
    let clone_id = ready!(checkout.value.create(
        AuthoredMutation::CreateFile {
            path: clone.clone(),
            bytes: OwnedBytes::from_static(b"........"),
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ))
    .value;

    let record = ready!(
        checkout
            .value
            .read_file_record_by_id(file_id, B, &cancellation,)
    );
    assert_eq!(record.value.file_id, file_id);
    assert_eq!(record.value.kind, FileKind::Regular);
    assert_eq!(
        ready!(checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 2,
                length: 3,
            },
            B,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"cde"
    );
    assert!(
        ready!(checkout.value.plan_file_extents_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 8,
            },
            8,
            B,
            &cancellation,
        ))
        .value
        .is_none()
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            B,
            &cancellation,
        ))
        .value,
        Some(0)
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            B,
            &cancellation,
        ))
        .value,
        Some(8)
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            9,
            ExtentSeekTarget::Data,
            B,
            &cancellation,
        ))
        .value,
        None
    );
    assert_eq!(
        ready!(
            checkout
                .value
                .seek_file_extent(&file, 9, ExtentSeekTarget::Data, B, &cancellation,)
        )
        .value,
        None
    );

    let mut path_metadata = ready!(checkout.value.read_metadata(&file, B, &cancellation,)).value;
    path_metadata.posix_mode = MetadataField::Value(0o100_660);
    ready!(
        checkout
            .value
            .set_attributes(file.clone(), path_metadata, Some(10), B, &cancellation,)
    );
    assert_eq!(
        ready!(checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 8,
                length: 2,
            },
            B,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        &[0, 0]
    );

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.identity".to_vec(),
        128,
    )?;
    assert!(
        ready!(
            checkout
                .value
                .read_named_attribute_by_id(file_id, &attribute, B, &cancellation,)
        )
        .value
        .is_none()
    );
    assert!(
        ready!(
            checkout
                .value
                .list_named_attributes_by_id(file_id, None, 8, B, &cancellation,)
        )
        .value
        .entries
        .is_empty()
    );
    ready!(checkout.value.change_named_attribute_by_id(
        file_id,
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"first"),
            NamedAttributeWriteMode::Create
        )),
        B,
        &cancellation
    ));
    assert_eq!(
        ready!(
            checkout
                .value
                .read_named_attribute_by_id(file_id, &attribute, B, &cancellation,)
        )
        .value
        .map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        ready!(
            checkout
                .value
                .list_named_attributes_by_id(file_id, None, 8, B, &cancellation,)
        )
        .value
        .entries
        .len(),
        1
    );
    ready!(checkout.value.change_named_attribute_by_id(
        file_id,
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"second"),
            NamedAttributeWriteMode::Replace
        )),
        B,
        &cancellation
    ));

    let mut metadata = ready!(
        checkout
            .value
            .read_metadata_by_id(file_id, B, &cancellation,)
    )
    .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    ready!(
        checkout
            .value
            .set_attributes_by_id(file_id, metadata, None, B, &cancellation)
    );
    metadata.posix_mode = MetadataField::Value(0o100_600);
    ready!(
        checkout
            .value
            .set_attributes_by_id(file_id, metadata, Some(12), B, &cancellation,)
    );
    ready!(checkout.value.change_content(
        file_id,
        ContentChange::Write {
            offset: 8,
            bytes: OwnedBytes::from_static(b"tail")
        },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    ready!(checkout.value.change_content(
        file_id,
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 4,
                length: 2,
            },
            allocated: false,
            extend: false
        },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    ready!(checkout.value.change_content(
        file_id,
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 12,
                length: 4,
            },
            keep_size: false
        },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    ready!(checkout.value.change_content(
        clone_id,
        ContentChange::Resize { logical_bytes: 16 },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    ready!(checkout.value.change_content(
        clone_id,
        ContentChange::CloneFrom {
            source: file_id,
            source_offset: 0,
            offset: 4,
            length: 8
        },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    ready!(checkout.value.change_named_attribute_by_id(
        file_id,
        &attribute.clone(),
        None,
        B,
        &cancellation
    ));
    assert!(
        ready!(
            checkout
                .value
                .read_named_attribute_by_id(file_id, &attribute, B, &cancellation,)
        )
        .value
        .is_none()
    );

    let target = OwnedBytes::from_static(b"../identity.bin");
    ready!(checkout.value.create(
        AuthoredMutation::CreateSymbolicLink {
            path: path("link")?,
            target: target.clone(),
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ));
    assert_eq!(
        ready!(
            checkout
                .value
                .read_symbolic_link(&path("link")?, B, &cancellation,)
        )
        .value,
        target
    );
    ready!(checkout.value.create(
        AuthoredMutation::CreateEmptySpecial {
            path: path("fifo")?,
            kind: FileKind::Fifo,
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ));
    ready!(checkout.value.create(
        AuthoredMutation::CreateEmptySpecial {
            path: path("socket")?,
            kind: FileKind::Socket,
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ));
    ready!(checkout.value.create(
        AuthoredMutation::CreateDevice {
            path: path("character")?,
            kind: FileKind::CharacterDevice,
            major: 1,
            minor: 3,
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ));
    ready!(checkout.value.create(
        AuthoredMutation::CreateDevice {
            path: path("block")?,
            kind: FileKind::BlockDevice,
            major: 8,
            minor: 0,
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ));
    assert!(
        (checkout.value.create(
            AuthoredMutation::CreateEmptySpecial {
                path: path("invalid")?,
                kind: FileKind::Regular,
                metadata: FileMetadata::default()
            },
            B,
            &cancellation
        ))
        .await
        .is_err()
    );
    assert!(
        (checkout.value.create(
            AuthoredMutation::CreateDevice {
                path: path("invalid-device")?,
                kind: FileKind::Regular,
                major: 0,
                minor: 0,
                metadata: FileMetadata::default()
            },
            B,
            &cancellation
        ))
        .await
        .is_err()
    );

    ready!(
        checkout
            .value
            .hard_link(file.clone(), path("hard-link")?, B, &cancellation,)
    );
    ready!(checkout.value.rename(
        path("hard-link")?,
        path("renamed-link")?,
        false,
        B,
        &cancellation,
    ));
    ready!(
        checkout
            .value
            .remove(path("renamed-link")?, Some(file_id), B, &cancellation,)
    );
    let listing =
        ready!(
            checkout
                .value
                .list_directory_records(&root_path()?, None, 32, B, &cancellation,)
        );
    assert_eq!(listing.value.entries.len(), 7);
    Ok(())
}

#[tokio::test]
async fn remaining_facade_guards_are_fast_typed_and_non_mutating()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(
        validate_checkout(
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Live,
                mutations: MutationMode::PrivateOverlay,
            },
            serialized_config(),
        )
        .is_ok()
    );

    let prior = WorkCounters {
        bytes_copied: u64::MAX,
        peak_allocation_bytes: 1,
        ..WorkCounters::default()
    };
    let overflow = merge_simultaneous_failure(
        prior,
        WorkCounters {
            bytes_copied: 1,
            peak_allocation_bytes: 1,
            ..WorkCounters::default()
        },
        0,
        FsError::NotFound,
    );
    assert!(matches!(overflow.error, FsError::Work(WorkError::Overflow)));
    assert_eq!(*overflow.work, prior);

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 1;
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([211; 16]), limited, B, &cancellation))
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

    let baseline = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    assert!(matches!(
        checkout.validate_profile_kind(FileKind::ReparsePoint, baseline),
        Err(OperationFailure {
            error: FsError::UnsupportedFileKind,
            ..
        })
    ));
    assert!(matches!(
        (checkout.stage_content(
            &mut std::io::Cursor::new(OwnedBytes::from_static(b"xx")),
            2,
            B,
            &cancellation
        ))
        .await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));

    (checkout.create(
        AuthoredMutation::CreateDirectory {
            path: path("directory")?,
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        (checkout.detach_regular_file(&path("directory")?, B, &cancellation,)).await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::NotRegular),
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn detached_payload_bounds_and_authority_volume_identity_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), B, &cancellation)).await?.value;
    let file = path("bounded-detached")?;
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
            bytes: OwnedBytes::new(),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.bounded".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    (checkout.change_named_attribute(
        file.clone(),
        &attribute.clone(),
        Some((
            &OwnedBytes::from_static(b"xx"),
            NamedAttributeWriteMode::Create,
        )),
        B,
        &cancellation,
    ))
    .await?;
    let mut detached = (checkout.detach_regular_file(&file, B, &cancellation))
        .await?
        .value;
    detached.volume.config.limits.maximum_read_bytes = 1;
    assert!(matches!(
        (detached.read_named_attribute(&attribute, B, &cancellation,)).await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    assert!(matches!(
        (detached.stage_content(
            &mut std::io::Cursor::new(OwnedBytes::from_static(b"xx")),
            2,
            B,
            &cancellation
        ))
        .await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));

    assert_authority_volume_identity_fails_closed()
}

#[tokio::test]
async fn one_reader_answers_a_batch_of_identities() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([191; 16]), config(), B, &cancellation))
            .await?
            .value;
    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let mut identities = Vec::new();
    for name in ["a", "b"] {
        (writer.create(
            AuthoredMutation::CreateFile {
                path: path(name)?,
                bytes: OwnedBytes::from_static(b"x"),
                metadata: FileMetadata::default(),
            },
            B,
            &cancellation,
        ))
        .await?;
        let record = (writer.lookup_no_follow(&path(name)?, B, &cancellation))
            .await?
            .value
            .record
            .ok_or("created file missing")?;
        identities.push(record);
    }
    let absent = FileId::from_bytes([192; 16]);
    let reader = writer.pinned_reader()?;
    assert_eq!(reader.generation_id(), writer.generation_id());
    let records = (reader.file_records_by_id(
        &[identities[1].file_id, absent, identities[0].file_id],
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(
        records.value,
        vec![Some(identities[1]), None, Some(identities[0])]
    );
    Ok(())
}
