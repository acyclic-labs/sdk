use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_identity_mutation_preserves_source_and_cancelled_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::KernelAccounting;
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-identity-candidate", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/source", "actual source bytes").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout(
        GenerationSelector::Head, CheckoutMode::tracking_transaction(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let path = path("source")?;
    let source = checkout.lookup_no_follow(
        &path, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("actual source record absent")?;
    let original = crate::NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(
        &original, &cancellation,
    )?;
    let paid = |mutation| -> Result<crate::kernel::Owned<Vec<Mutation>>, FsError> {
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
        })?;
        operations.value.push(Mutation::File { file_id: source.file_id, mutation });
        Ok(operations)
    };
    let input = paid(FileMutation::Resize { logical_bytes: 0 })?;
    candidate.mutate_owned_with_accounting(input, &mut KernelAccounting::native(&original, &cancellation), &cancellation).await?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let current = crate::kernel::lookup_file_record_async_with_accounting(
        &fs.inner.objects, candidate.root.file_table, source.file_id,
        DecodeLimits::for_volume(candidate.volume.config), &mut accounting, &cancellation, None,
    ).await?;
    let current = current.0.ok_or("mutated identity disappeared")?;
    assert_eq!(current.file_id, source.file_id);
    assert_ne!(current.payload, source.payload);
    assert!(candidate.root.same_original_scope(original.native_read_meter()));
    assert_eq!(candidate.base_file_table, checkout.root.file_table);
    assert_eq!(checkout.lookup_no_follow(
        &path, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record, Some(source));
    let before = (
        candidate.root.file_table, candidate.base_file_table,
        candidate.authored_operation_id, candidate.live_operation_id,
    );
    let baseline = original.live_allocation_bytes();
    let input = paid(FileMutation::Resize { logical_bytes: 7 })?;
    let before_io = original.receipt();
    cancellation.cancel();
    let failure = candidate.mutate_owned_with_accounting(
        input, &mut KernelAccounting::native(&original, &cancellation), &cancellation,
    ).await.err().ok_or("cancelled mutation changed candidate")?;
    assert!(matches!(failure,
        FsError::Cancelled(_) |
        FsError::NativeKernel(crate::NativeTreeMaterializationError::Cancelled)));
    assert_eq!(before, (
        candidate.root.file_table, candidate.base_file_table,
        candidate.authored_operation_id, candidate.live_operation_id,
    ));
    assert_eq!(original.receipt().backend_read_operations, before_io.backend_read_operations);
    assert_eq!(original.receipt().backend_write_operations, before_io.backend_write_operations);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn identity_writes_rebase_without_namespace_lookup_and_conflict_by_exact_range()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([111; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("identity")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file_id = (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    (seed.commit(OperationId::from_bytes([112; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (local.change_content(file_id, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"LOCAL")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (remote.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"REMOTE")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (remote.commit(OperationId::from_bytes([113; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let safe = (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let bytes = (local.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(&bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&bytes.value.bytes[64..70], b"REMOTE");
    let plan = (local.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(plan.value.is_some());

    let mut overlapping_local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut overlapping_remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (overlapping_local.change_content(file_id, crate::ContentChange::Write { offset: 10, bytes: (bytes::Bytes::from_static(b"local")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (overlapping_remote.change_content(file, crate::ContentChange::Write { offset: 12, bytes: (bytes::Bytes::from_static(b"remote")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (overlapping_remote.commit(OperationId::from_bytes([114; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let conflict =
        (overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
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
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (extending.change_content(file_id, crate::ContentChange::Write { offset: 128, bytes: (bytes::Bytes::from_static(b"TAIL")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (extending.change_content(file_id, crate::ContentChange::CloneFrom { source: file_id, source_offset: 0, offset: 140, length: 4 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let extended = (extending.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 128,
            length: 16,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(&extended.value.bytes[..4], b"TAIL");
    assert_eq!(&extended.value.bytes[4..12], &[0; 8]);
    assert_eq!(&extended.value.bytes[12..], b"aaaa");
    Ok(())
}

#[tokio::test]
async fn public_identity_and_posix_special_surfaces_share_one_candidate()
-> Result<(), Box<dyn std::error::Error>> {

    let mut volume_config = config();
    volume_config.profile = FilesystemProfile::Posix;
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume(volume_config, WorkBudget::UNBOUNDED, &cancellation,)).await?;
    let mut checkout = (volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;

    let file = path("identity.bin")?;
    let clone = path("clone.bin")?;
    let file_id = (checkout.value.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abcdefgh")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    let clone_id = (checkout.value.create(crate::AuthoredMutation::CreateFile { path: clone.clone(), bytes: (bytes::Bytes::from_static(b"........")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;

    let record = (checkout.value.read_file_record_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(record.value.file_id, file_id);
    assert_eq!(record.value.kind, FileKind::Regular);
    assert_eq!(
        (checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 2,
                length: 3,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .bytes
        .as_ref(),
        b"cde"
    );
    assert!(
        (checkout.value.plan_file_extents_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 8,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .is_none()
    );
    assert_eq!(
        (checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        Some(0)
    );
    assert_eq!(
        (checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        Some(8)
    );
    assert_eq!(
        (checkout.value.seek_file_extent_by_id(
            file_id,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        None
    );
    assert_eq!(
        (checkout.value.seek_file_extent(
            &file,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        None
    );

    let mut path_metadata = (checkout.value.read_metadata(
        &file,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    path_metadata.posix_mode = MetadataField::Value(0o100_660);
    (checkout.value.set_attributes(
        file.clone(),
        path_metadata,
        Some(10),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(
        (checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 8,
                length: 2,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
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
        (checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .is_none()
    );
    assert!(
        (checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .entries
        .is_empty()
    );
    (checkout.value.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(
        (checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value.map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        (checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .entries
        .len(),
        1
    );
    (checkout.value.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"second"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation)).await?;

    let mut metadata = (checkout.value.read_metadata_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    (checkout.value.set_attributes_by_id(file_id, metadata, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    metadata.posix_mode = MetadataField::Value(0o100_600);
    (checkout.value.set_attributes_by_id(
        file_id,
        metadata,
        Some(12),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (checkout.value.change_content(file_id, crate::ContentChange::Write { offset: 8, bytes: (bytes::Bytes::from_static(b"tail")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.change_content(file_id, crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 4,
            length: 2,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.change_content(file_id, crate::ContentChange::Preallocate { range: ByteRange {
            offset: 12,
            length: 4,
        }, keep_size: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (
        checkout
            .value
            .change_content(clone_id, crate::ContentChange::Resize { logical_bytes: 16 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)
    ).await?;
    (checkout.value.change_content(clone_id, crate::ContentChange::CloneFrom { source: file_id, source_offset: 0, offset: 4, length: 8 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.change_named_attribute_by_id(file_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(
        (checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .is_none()
    );

    let target = bytes::Bytes::from_static(b"../identity.bin");
    (checkout.value.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: (target.clone()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(
        (checkout.value.read_symbolic_link(
            &path("link")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value.bytes.as_ref(),
        target.as_ref()
    );
    (checkout.value.create(crate::AuthoredMutation::CreateEmptySpecial { path: path("fifo")?, kind: FileKind::Fifo, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.create(crate::AuthoredMutation::CreateEmptySpecial { path: path("socket")?, kind: FileKind::Socket, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.create(crate::AuthoredMutation::CreateDevice { path: path("character")?, kind: FileKind::CharacterDevice, major: 1, minor: 3, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.create(crate::AuthoredMutation::CreateDevice { path: path("block")?, kind: FileKind::BlockDevice, major: 8, minor: 0, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(
        (checkout.value.create(crate::AuthoredMutation::CreateEmptySpecial { path: path("invalid")?, kind: FileKind::Regular, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );
    assert!(
        (checkout.value.create(crate::AuthoredMutation::CreateDevice { path: path("invalid-device")?, kind: FileKind::Regular, major: 0, minor: 0, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );

    (checkout.value.mutate(vec![crate::kernel::Mutation::Link { source: file.clone(), destination: path("hard-link")? }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.mutate(vec![crate::kernel::Mutation::Rename { source: path("hard-link")?, destination: path("renamed-link")?, replace: false }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.mutate(vec![crate::kernel::Mutation::Remove { path: path("renamed-link")?, expected_file_id: (Some(file_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let listing = (checkout.value.list_directory_records(
        &root_path()?,
        std::ops::Bound::Unbounded,
        32,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
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
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([211; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
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

    (checkout.create(crate::AuthoredMutation::CreateDirectory { path: path("directory")?, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(
        (checkout.detach_regular_file(
            &path("directory")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await,
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
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let file = path("bounded-detached")?;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::new()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.bounded".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    (checkout.change_named_attribute(file.clone(), &attribute, Some((&bytes::Bytes::from_static(b"xx"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut detached =
        (checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    detached.volume.config.limits.maximum_read_bytes = 1;
    assert!(matches!(
        (
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        ).await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    assert_authority_volume_identity_fails_closed().await
}

#[tokio::test]
async fn one_reader_answers_a_batch_of_identities() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([191; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut identities = Vec::new();
    for name in ["a", "b"] {
        (writer.create(crate::AuthoredMutation::CreateFile { path: path(name)?, bytes: (bytes::Bytes::from_static(b"x")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
        let record =
            (writer.lookup_no_follow(&path(name)?, WorkBudget::UNBOUNDED, &cancellation)).await?
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
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(
        records.value,
        vec![Some(identities[1]), None, Some(identities[0])]
    );
    Ok(())
}
