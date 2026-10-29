use super::*;

pub(super) async fn sparse_rebase_across_remote_ranges(native: bool) -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let id = create(
        &mut seed,
        "file",
        OwnedBytes::from(vec![b'a'; 128]),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    #[cfg(not(target_arch = "wasm32"))]
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(error, _)| error)?;
    for (local_offset, remote_offset, local_bytes, remote_bytes, safe) in [
        (0, 64, b"LOCAL".as_slice(), b"REMOTE".as_slice(), true),
        (10, 12, b"local".as_slice(), b"remote".as_slice(), false),
    ] {
        let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
        let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
        for (checkout, offset, bytes) in [
            (&mut local, local_offset, local_bytes),
            (&mut remote, remote_offset, remote_bytes),
        ] {
            checkout
                .change_content(
                    path("file")?,
                    ContentChange::Write {
                        offset,
                        bytes: OwnedBytes::copy_from_slice(bytes),
                    },
                    ContentTimes::Preserve,
                    B,
                    &cancel,
                )
                .await?;
        }
        commit(&mut remote, &cancel).await?;
        #[cfg(not(target_arch = "wasm32"))]
        if native {
            local = local.private_candidate(
                &mut crate::kernel::KernelAccounting::native(&original, &cancel),
                &cancel,
            )?;
        }
        let before = (
            checkout_state(&local),
            local.authored_operation_id,
            local.live_operation_id,
        );
        #[cfg(not(target_arch = "wasm32"))]
        if native && safe {
            let denied = original.native_volume_work(WorkBudget {
                allocation_operations: 1,
                ..B
            })?;
            assert!(local.rebase_head(8, &denied, &cancel).await.is_err());
            assert_eq!(
                before,
                (
                    checkout_state(&local),
                    local.authored_operation_id,
                    local.live_operation_id
                )
            );
        }
        #[cfg(not(target_arch = "wasm32"))]
        let (decision, _allocation) = if native {
            let result = local.rebase_head(8, &original, &cancel).await?;
            assert!(result.1.work().shares_ledger(&original));
            (result.0, Some(result.1))
        } else {
            (local.rebase_head(8, B, &cancel).await?.value, None)
        };
        #[cfg(target_arch = "wasm32")]
        let decision = local.rebase_head(8, B, &cancel).await?.value;
        if safe {
            assert!(matches!(decision, RebaseDecision::Safe { .. }));
            let bytes = read(&mut local, id, 0, 70, &cancel).await?;
            assert_eq!(&bytes[..5], b"LOCAL");
            assert_eq!(&bytes[64..70], b"REMOTE");
        } else {
            assert_eq!(
                before,
                (
                    checkout_state(&local),
                    local.authored_operation_id,
                    local.live_operation_id
                )
            );
            assert!(
                matches!(decision, RebaseDecision::Conflicted { conflicts, .. }
                if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::ContentRange { .. })))
            );
        }
    }
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
            bytes: content.clone(),
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

pub(super) async fn assert_authority_volume_identity_fails_closed() -> TestResult {
    let root = ObjectId {
        storage: None,
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([221; 32]),
    };
    let expected = VolumeId::from_bytes([222; 16]);
    let wrong = VolumeId::from_bytes([223; 16]);
    let authority = Arc::new(crate::MemoryAuthorityStore::default());
    let fs = Fs::new(
        authority.clone(),
        crate::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancel = CancellationToken::new();
    let authority_id = volume_authority_id(expected);
    let mut work = crate::AuthorityWork::new(B, &cancel);
    let created = authority
        .create_authority(authority_id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let head = match created {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    let mut accounting = KernelAccounting::standalone(B, &cancel);
    let encoded =
        crate::kernel::codec::encode_owned(&mut accounting, |observe, allocation_error| {
            crate::kernel::encode_volume_created(
                VolumeCreated {
                    volume_id: wrong,
                    config: config(),
                    initial_generation_root: root,
                },
                observe,
                allocation_error,
            )
        })?;
    let proposal = creation_commit(OperationId::from_bytes([227; 16]), encoded, &mut accounting)?;
    let appended = authority
        .compare_and_append(
            crate::GuardedAppend {
                authority_id,
                epoch: head.epoch,
                expected: Head::genesis(head.epoch),
                commit: proposal,
                permit: PublicationPermit::Unrestricted,
            },
            &mut work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let AppendOutcome::Committed(creation) = appended else {
        return Err("fixture creation did not commit".into());
    };
    assert!(matches!(
        fs.read_creation_record(expected, B, &cancel).await,
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));
    let mut payload = Vec::from(b"acyclic-fs-publish-generation-v1\0".as_slice());
    payload.extend_from_slice(&1_u16.to_le_bytes());
    payload.extend_from_slice(&wrong.into_bytes());
    payload.push(root.kind.canonical_tag());
    payload.extend_from_slice(root.digest.as_bytes());
    let publication = DurableCommit {
        sequence: Sequence::new(2),
        payload: payload.into(),
        ..creation.clone()
    };
    for record in [&creation, &publication] {
        assert!(matches!(
            generation_from_record(
                record,
                expected,
                &mut KernelAccounting::standalone(B, &cancel)
            ),
            Err(FsError::VolumeMismatch)
        ));
    }
    let empty = VolumeId::from_bytes([228; 16]);
    authority
        .create_authority(
            volume_authority_id(empty),
            Epoch::GENESIS,
            &mut work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let empty = Volume {
        fs,
        id: empty,
        config: config(),
    };
    assert!(matches!(
        empty
            .resolve_head_generation(&mut KernelAccounting::standalone(B, &cancel), &cancel)
            .await,
        Err(FsError::EmptyAuthority)
    ));
    Ok(())
}
