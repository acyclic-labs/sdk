use super::*;

pub(super) async fn sparse_rebase_across_remote_ranges(
    native: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([86; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let file = path("file")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    (seed.create(
        crate::AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (seed.commit(
        OperationId::from_bytes([87; 16]),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::FsReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?;

    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    (local.change_content(
        file.clone(),
        crate::ContentChange::Write {
            offset: 0,
            bytes: (bytes::Bytes::from_static(b"LOCAL")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (remote.change_content(
        file.clone(),
        crate::ContentChange::Write {
            offset: 64,
            bytes: (bytes::Bytes::from_static(b"REMOTE")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (remote.commit(
        OperationId::from_bytes([88; 16]),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::FsReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?;
    #[cfg(not(target_arch = "wasm32"))]
    if native {
        let before = (
            local.generation_id(),
            local.root.file_table,
            local.base_file_table,
            local.authority_head,
            local.authored_operation_id,
            local.live_operation_id,
            local.dependencies.proof().dependencies.clone(),
        );
        let denied = crate::NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: 1,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(cause, _)| cause)?;
        assert!(
            (local.rebase_head_with_native_work(8, &denied, &cancellation))
                .await
                .is_err()
        );
        assert_eq!(
            before,
            (
                local.generation_id(),
                local.root.file_table,
                local.base_file_table,
                local.authority_head,
                local.authored_operation_id,
                local.live_operation_id,
                local.dependencies.proof().dependencies.clone(),
            )
        );
    }
    #[cfg(not(target_arch = "wasm32"))]
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    #[cfg(not(target_arch = "wasm32"))]
    let safe = if native {
        let result = (local.rebase_head_with_native_work(8, &original, &cancellation)).await?;
        (result.0, Some(result.1))
    } else {
        (
            (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
                .await?
                .value,
            None,
        )
    };
    #[cfg(target_arch = "wasm32")]
    let safe = (
        (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value,
        (),
    );
    assert!(matches!(safe.0, RebaseDecision::Safe { .. }));
    let local_bytes = (local.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(&local_bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&local_bytes.value.bytes[64..70], b"REMOTE");

    let mut overlapping_local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut overlapping_remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    (overlapping_local.change_content(
        file.clone(),
        crate::ContentChange::Write {
            offset: 10,
            bytes: (bytes::Bytes::from_static(b"local")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (overlapping_remote.change_content(
        file,
        crate::ContentChange::Write {
            offset: 12,
            bytes: (bytes::Bytes::from_static(b"remote")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (overlapping_remote.commit(
        OperationId::from_bytes([89; 16]),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::FsReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?;
    let unchanged = (
        overlapping_local.generation_id(),
        overlapping_local.root.file_table,
        overlapping_local.base_file_table,
        overlapping_local.authority_head,
        overlapping_local.authored_operation_id,
        overlapping_local.live_operation_id,
        overlapping_local.dependencies.proof().dependencies.clone(),
    );
    #[cfg(not(target_arch = "wasm32"))]
    let conflict = if native {
        let result =
            (overlapping_local.rebase_head_with_native_work(8, &original, &cancellation)).await?;
        assert!(result.1.work().shares_ledger(&original));
        assert!(result.1.bytes() >= (std::mem::size_of::<crate::kernel::RebaseConflict>()) as u64);
        (result.0, Some(result.1))
    } else {
        (
            (overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
                .await?
                .value,
            None,
        )
    };
    #[cfg(target_arch = "wasm32")]
    let conflict = (
        (overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value,
        (),
    );
    assert_eq!(
        unchanged,
        (
            overlapping_local.generation_id(),
            overlapping_local.root.file_table,
            overlapping_local.base_file_table,
            overlapping_local.authority_head,
            overlapping_local.authored_operation_id,
            overlapping_local.live_operation_id,
            overlapping_local.dependencies.proof().dependencies.clone(),
        )
    );
    assert!(matches!(
        conflict.0,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn original_authored_creates(
    requests: &[(&NamespacePath, OwnedBytes)],
    metadata: FileMetadata,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(Vec<AuthoredMutation>, crate::NativeAllocation), FsError> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let mut accounting = KernelAccounting::native(original, cancellation);
    let mut input =
        crate::kernel::Owned::<Vec<AuthoredMutation>>::allocate(requests.len(), &mut accounting)?;
    for (requested, content) in requests {
        let mut path = clone_authored_path(requested, &mut accounting)?;
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<AuthoredMutation>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let bytes = input
            .bytes
            .checked_add(path.bytes)
            .ok_or(WorkError::Overflow)?;
        input.allocation.absorb(&mut path.allocation)?;
        input.value.push(AuthoredMutation::CreateFile {
            path: path.value,
            bytes: content.clone().into(),
            metadata,
        });
        input.bytes = bytes;
    }
    let KernelOwnedAllocation::NativeOriginal(allocation) = input.allocation else {
        unreachable!("original producer keeps its original guard")
    };
    Ok((input.value, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn original_borrowed_path_changes(
    source: &NamespacePath,
    destination: &NamespacePath,
    removed: &NamespacePath,
    metadata: FileMetadata,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<
    (
        Vec<AuthoredMutation>,
        crate::NativeAllocation,
        crate::NativeAllocation,
    ),
    FsError,
> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let mut accounting = KernelAccounting::native(original, cancellation);
    let input = crate::kernel::Owned::<Vec<AuthoredMutation>>::allocate(4, &mut accounting)?;
    let mut source_allocation = accounting.claim_owned(0, 0)?;
    let mut copy_path = |requested: &NamespacePath| -> Result<NamespacePath, FsError> {
        let mut path = clone_authored_path(requested, &mut accounting)?;
        source_allocation.absorb(&mut path.allocation)?;
        Ok(path.value)
    };
    let metadata_path = copy_path(source)?;
    let resize_path = copy_path(source)?;
    let rename_source = copy_path(source)?;
    let rename_destination = copy_path(destination)?;
    let remove_path = copy_path(removed)?;
    accounting.charge(WorkCounters {
        bytes_copied: (4 * size_of::<AuthoredMutation>()) as u64,
        ..WorkCounters::UNCHARGED
    })?;
    let crate::kernel::Owned {
        mut value,
        allocation,
        ..
    } = input;
    value.push(AuthoredMutation::SetMetadata {
        path: metadata_path,
        metadata,
    });
    value.push(AuthoredMutation::Resize {
        path: resize_path,
        logical_bytes: 4,
    });
    value.push(AuthoredMutation::Rename {
        source: rename_source,
        destination: rename_destination,
        replace: true,
    });
    value.push(AuthoredMutation::Remove {
        path: remove_path,
        expected_file_id: None,
    });
    let KernelOwnedAllocation::NativeOriginal(slots) = allocation else {
        unreachable!("native producer retains original slots")
    };
    let KernelOwnedAllocation::NativeOriginal(source) = source_allocation else {
        unreachable!("native producer retains original paths")
    };
    Ok((value, slots, source))
}

pub(super) async fn assert_authority_volume_identity_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let generation_root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([221; 32]),
    };
    let expected_volume = VolumeId::from_bytes([222; 16]);
    let wrong_volume = VolumeId::from_bytes([223; 16]);
    let creation = DurableCommit {
        settled_at_micros: None,
        epoch: Epoch::new(1)?,
        sequence: Sequence::new(1),
        operation_id: OperationId::from_bytes([224; 16]),
        fingerprint: Digest::from_bytes([225; 32]),
        previous_digest: Digest::ZERO,
        digest: Digest::from_bytes([226; 32]),
        payload: bytes::Bytes::from(crate::kernel::encode_volume_created(
            VolumeCreated {
                volume_id: wrong_volume,
                config: config(),
                initial_generation_root: generation_root,
            },
            |_| Ok::<_, crate::kernel::VolumeCreatedError>(()),
            |_| {
                crate::kernel::VolumeCreatedError::Decode(
                    crate::kernel::CanonicalDecodeError::AllocationFailed,
                )
            },
        )?),
    };
    assert!(matches!(
        generation_from_record(
            &creation,
            expected_volume,
            &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())
        ),
        Err(FsError::VolumeMismatch)
    ));
    let publication = DurableCommit {
        sequence: Sequence::new(2),
        payload: {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"acyclic-fs-publish-generation-v1\0");
            bytes.extend_from_slice(&1_u16.to_le_bytes());
            bytes.extend_from_slice(&wrong_volume.into_bytes());
            bytes.push(generation_root.kind.canonical_tag());
            bytes.extend_from_slice(generation_root.digest.as_bytes());
            bytes::Bytes::from(bytes)
        },
        ..creation
    };
    assert!(matches!(
        generation_from_record(
            &publication,
            expected_volume,
            &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())
        ),
        Err(FsError::VolumeMismatch)
    ));

    let authority = Arc::new(crate::memory::MemoryAuthorityStore::default());
    let fs = Fs::new(
        Arc::clone(&authority),
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let expected_authority = volume_authority_id(expected_volume);
    let active = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = AsyncAuthorityStore::create_authority(
            &*authority,
            expected_authority,
            Epoch::GENESIS,
            &mut __authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?;
    let head = match active.value {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    let encoded = crate::kernel::encode(
        WorkBudget::UNBOUNDED,
        Some(&CancellationToken::new()),
        0,
        |observe| {
            crate::kernel::encode_volume_created(
                VolumeCreated {
                    volume_id: wrong_volume,
                    config: config(),
                    initial_generation_root: generation_root,
                },
                observe,
                |_| {
                    VolumeCreatedError::Decode(
                        crate::kernel::CanonicalDecodeError::AllocationFailed,
                    )
                },
            )
        },
    )?;
    let (proposal, _) = creation_commit(
        OperationId::from_bytes([227; 16]),
        encoded,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )?
    .value;
    ({
        let authority_input_1 = head.epoch;
        let authority_input_2 = Head::genesis(head.epoch);
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = AsyncAuthorityStore::compare_and_append(
            &*authority,
            crate::GuardedAppend {
                authority_id: expected_authority,
                epoch: authority_input_1,
                expected: authority_input_2,
                commit: proposal,
                permit: crate::PublicationPermit::Unrestricted,
            },
            &mut __authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))
        .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })?;
    let cancellation = CancellationToken::new();
    assert!(matches!(
        (fs.read_creation_record(expected_volume, WorkBudget::UNBOUNDED, &cancellation)).await,
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));

    let empty_volume = VolumeId::from_bytes([228; 16]);
    ({
        let authority_input_0 = volume_authority_id(empty_volume);
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = AsyncAuthorityStore::create_authority(
            &*authority,
            authority_input_0,
            Epoch::GENESIS,
            &mut __authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?;
    let empty = Volume {
        fs,
        id: empty_volume,
        config: config(),
    };
    assert!(matches!(
        (empty.resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)).await,
        Err(OperationFailure {
            error: FsError::EmptyAuthority,
            ..
        })
    ));
    Ok(())
}
