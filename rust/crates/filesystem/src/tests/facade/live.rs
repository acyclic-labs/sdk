use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_guarded_live_retry_preserves_both_real_identities() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(serialized_config(), &cancel).await?;
    let left = create(
        &mut seed,
        "left",
        OwnedBytes::from_static(b"left real source"),
        &cancel,
    )
    .await?;
    let right = create(
        &mut seed,
        "right",
        OwnedBytes::from_static(b"distinct right real source"),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(cause, _)| cause)?;
    let mut local = volume
        .checkout(
            GenerationSelector::Head,
            writable_live(),
            &original,
            &cancel,
        )
        .await?;
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    local
        .change_content(
            left,
            ContentChange::Resize { logical_bytes: 0 },
            ContentTimes::Preserve,
            &original,
            &cancel,
        )
        .await?;
    remote
        .change_content(
            right,
            ContentChange::Resize { logical_bytes: 0 },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut remote, &cancel).await?;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&callbacks);
    let callback_original = original.clone();
    let mut before_publication = move |meter: &crate::NativeMaterializationWork| {
        assert!(meter.shares_ledger(&callback_original));
        callback_calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok::<(), FsError>(()))
    };
    let outcome = local
        .resume_live_with_native_work(
            OperationId::from_bytes([212; 16]),
            8,
            16,
            PublicationPermit::Unrestricted,
            &original,
            &cancel,
            &mut before_publication,
            None,
            None,
            None,
            None,
        )
        .await?;
    assert!(matches!(outcome.0, LiveMutationOutcome::Committed { .. }));
    assert!(outcome.1.work().shares_ledger(&original));
    assert!(callbacks.load(Ordering::SeqCst) >= 2);
    assert!(!local.has_pending_mutations());
    let left_record = local
        .read_file_record_by_id(left, &original, &cancel)
        .await?;
    let right_record = local
        .read_file_record_by_id(right, &original, &cancel)
        .await?;
    assert_eq!(left_record.file_id, left);
    assert_eq!(right_record.file_id, right);
    assert_eq!(left_record.payload, right_record.payload);
    Ok(())
}

#[tokio::test]
async fn direct_live_mutations_retry_only_across_exactly_safe_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let optimistic =
        (fs.create_volume_with_id(VolumeId::from_bytes([90; 16]), config(), B, &cancellation))
            .await?
            .value;
    let rejected =
        (optimistic.checkout(GenerationSelector::Head, writable_live(), B, &cancellation))
            .await
            .err()
            .ok_or("optimistic volume admitted direct live")?;
    assert!(matches!(
        rejected.error,
        FsError::LiveRequiresSerializedAuthority
    ));

    let (volume, _) = fixture(serialized_config(), &cancellation).await?;
    let file = path("file")?;
    let mut seed = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    (seed.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (seed.commit(
        OperationId::from_bytes([92; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut first = open_checkout(&volume, writable_live(), &cancellation).await?;
    let mut second = open_checkout(&volume, writable_live(), &cancellation).await?;
    let first_blob = (first.stage_content(
        &mut std::io::Cursor::new(OwnedBytes::from_static(b"FIRST")),
        OwnedBytes::from_static(b"FIRST").len() as u64,
        B,
        &cancellation,
    ))
    .await?;
    let second_blob = (second.stage_content(
        &mut std::io::Cursor::new(OwnedBytes::from_static(b"SECOND")),
        OwnedBytes::from_static(b"SECOND").len() as u64,
        B,
        &cancellation,
    ))
    .await?;
    let first_outcome = (first.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 0,
            length: first_blob.value.logical_bytes,
            content: first_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([93; 16]),
        2,
        8,
        B,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        first_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let second_outcome = (second.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 64,
            length: second_blob.value.logical_bytes,
            content: second_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([94; 16]),
        2,
        8,
        B,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        second_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));

    let mut overlapping = open_checkout(&volume, writable_live(), &cancellation).await?;
    let mut racing = open_checkout(&volume, writable_live(), &cancellation).await?;
    let overlap_blob = (overlapping.stage_content(
        &mut std::io::Cursor::new(OwnedBytes::from_static(b"OVERLAP")),
        OwnedBytes::from_static(b"OVERLAP").len() as u64,
        B,
        &cancellation,
    ))
    .await?;
    let racing_blob = (racing.stage_content(
        &mut std::io::Cursor::new(OwnedBytes::from_static(b"RACING")),
        OwnedBytes::from_static(b"RACING").len() as u64,
        B,
        &cancellation,
    ))
    .await?;
    (racing.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 10,
            length: racing_blob.value.logical_bytes,
            content: racing_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([95; 16]),
        2,
        8,
        B,
        &cancellation,
    ))
    .await?;
    let conflict = (overlapping.mutate_live(
        vec![Mutation::Write {
            path: file,
            offset: 12,
            length: overlap_blob.value.logical_bytes,
            content: overlap_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([96; 16]),
        2,
        8,
        B,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        conflict.value,
        LiveMutationOutcome::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    Ok(())
}

#[tokio::test]
async fn explicit_live_refresh_advances_only_across_unobserved_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let (volume, _) = fixture(serialized_config(), &cancellation).await?;
    let file = path("refresh")?;
    let mut seed = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    (seed.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (seed.commit(
        OperationId::from_bytes([98; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut live = open_checkout(&volume, writable_live(), &cancellation).await?;
    assert_eq!(
        (live.read_file_range(
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
        b"aaaa"
    );

    let mut disjoint = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    (disjoint.change_content(
        file.clone(),
        ContentChange::Write {
            offset: 64,
            bytes: OwnedBytes::from_static(b"SAFE"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    let disjoint_generation = match (disjoint.commit(
        OperationId::from_bytes([99; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected disjoint commit: {outcome:?}").into()),
    };
    assert_eq!(
        (live.refresh_live(B, &cancellation)).await?.value,
        disjoint_generation
    );
    assert_eq!(live.generation_id(), disjoint_generation);

    assert_eq!(
        (live.read_file_range(
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
        b"aaaa"
    );
    let mut overlapping = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    (overlapping.change_content(
        file,
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"RACE"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (overlapping.commit(
        OperationId::from_bytes([100; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let before_conflict = live.generation_id();
    let conflict = (live.refresh_live(B, &cancellation))
        .await
        .err()
        .ok_or("conflicting live refresh unexpectedly advanced")?;
    assert!(matches!(
        conflict.error,
        FsError::LiveConflict { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    assert_eq!(live.generation_id(), before_conflict);
    Ok(())
}

#[tokio::test]
async fn direct_live_resume_is_idempotent_retry_bounded_and_epoch_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([117; 16]);
    let volume = (fs.create_volume_with_id(volume_id, serialized_config(), B, &cancellation))
        .await?
        .value;

    let mut winner = open_checkout(&volume, writable_live(), &cancellation).await?;
    let mut retrying = open_checkout(&volume, writable_live(), &cancellation).await?;
    (winner.create(
        AuthoredMutation::CreateFile {
            path: path("winner")?,
            bytes: OwnedBytes::from_static(b"winner"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (winner.resume_live(OperationId::from_bytes([118; 16]), 1, 8, B, &cancellation)).await?;

    (retrying.create(
        AuthoredMutation::CreateFile {
            path: path("retrying")?,
            bytes: OwnedBytes::from_static(b"retrying"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let retry_operation = OperationId::from_bytes([119; 16]);
    let limited = (retrying.resume_live(retry_operation, 1, 8, B, &cancellation)).await?;
    assert!(matches!(
        limited.value,
        LiveMutationOutcome::RetryLimit { .. }
    ));
    assert!(retrying.has_pending_mutations());
    let mutation_while_unresolved = (retrying.create(
        AuthoredMutation::CreateFile {
            path: path("must-remain-fenced")?,
            bytes: OwnedBytes::from_static(b"blocked"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("unresolved live mutation accepted another mutation")?;
    assert!(matches!(
        mutation_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let mismatched_resume =
        (retrying.resume_live(OperationId::from_bytes([120; 16]), 2, 8, B, &cancellation))
            .await
            .err()
            .ok_or("mismatched live resume unexpectedly succeeded")?;
    assert!(matches!(
        mismatched_resume.error,
        FsError::PendingLiveMutation
    ));
    let rebase_while_unresolved = (retrying.rebase_head(8, B, &cancellation))
        .await
        .err()
        .ok_or("unresolved live operation accepted public rebase")?;
    assert!(matches!(
        rebase_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let resumed = (retrying.resume_live(retry_operation, 2, 8, B, &cancellation)).await?;
    assert!(matches!(
        resumed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(!retrying.has_pending_mutations());
    let clean_resume = (retrying.resume_live(retry_operation, 1, 8, B, &cancellation))
        .await
        .err()
        .ok_or("clean live resume unexpectedly succeeded")?;
    assert!(matches!(clean_resume.error, FsError::NoPendingMutations));

    for failure in [
        (retrying.resume_live(retry_operation, 0, 8, B, &cancellation))
            .await
            .err()
            .ok_or("zero-limit live resume unexpectedly succeeded")?,
        (retrying.mutate_live(
            Vec::new(),
            OperationId::from_bytes([124; 16]),
            0,
            8,
            B,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("zero-limit live mutation unexpectedly succeeded")?,
    ] {
        assert!(matches!(failure.error, FsError::ZeroLiveRetryLimit));
    }

    let mut pending_guard = open_checkout(&volume, writable_live(), &cancellation).await?;
    (pending_guard.create(
        AuthoredMutation::CreateFile {
            path: path("pending-guard")?,
            bytes: OwnedBytes::from_static(b"pending"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let pending_failure = (pending_guard.mutate_live(
        Vec::new(),
        OperationId::from_bytes([125; 16]),
        1,
        8,
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("second pending live mutation unexpectedly succeeded")?;
    assert!(matches!(
        pending_failure.error,
        FsError::PendingLiveMutation
    ));

    let authored_pending = (pending_guard.apply_authored_live(
        Vec::new(),
        OperationId::from_bytes([126; 16]),
        1,
        8,
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("pending authored-live operation unexpectedly succeeded")?;
    assert!(matches!(
        authored_pending.error,
        FsError::PendingLiveMutation
    ));

    let mut first_retry = open_checkout(&volume, writable_live(), &cancellation).await?;
    let mut exact_retry = open_checkout(&volume, writable_live(), &cancellation).await?;
    let root = root_path()?;
    let metadata = (first_retry.lookup_no_follow(&root, B, &cancellation))
        .await?
        .value
        .record
        .ok_or("root record absent")?
        .metadata;
    let identical = Mutation::Create {
        path: path("idempotent")?,
        record: FileRecord {
            file_id: FileId::from_bytes([120; 16]),
            kind: FileKind::Regular,
            link_count: 1,
            metadata,
            payload: FilePayload::InlineRegular(InlineFileData::new(b"same")?),
        },
    };
    (first_retry.mutate(vec![identical.clone()], B, &cancellation)).await?;
    (exact_retry.mutate(vec![identical], B, &cancellation)).await?;
    let idempotent_operation = OperationId::from_bytes([121; 16]);
    let committed = (first_retry.resume_live(idempotent_operation, 1, 8, B, &cancellation)).await?;
    assert!(matches!(
        committed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let repeated = (exact_retry.resume_live(idempotent_operation, 1, 8, B, &cancellation)).await?;
    assert!(matches!(
        repeated.value,
        LiveMutationOutcome::AlreadyCommitted { .. }
    ));

    let mut conflicting_retry = open_checkout(&volume, writable_live(), &cancellation).await?;
    (conflicting_retry.create(
        AuthoredMutation::CreateFile {
            path: path("different-operation")?,
            bytes: OwnedBytes::from_static(b"different"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let conflicted =
        (conflicting_retry.resume_live(idempotent_operation, 1, 8, B, &cancellation)).await?;
    assert!(matches!(
        conflicted.value,
        LiveMutationOutcome::IdempotencyConflict { .. }
    ));
    assert!(conflicting_retry.has_pending_mutations());

    let mut fenced = open_checkout(&volume, writable_live(), &cancellation).await?;
    (fenced.create(
        AuthoredMutation::CreateFile {
            path: path("fenced")?,
            bytes: OwnedBytes::from_static(b"fenced"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let fence_expected = ({
        AsyncAuthorityStore::head(
            &fs.inner.authority,
            volume_authority_id(volume_id),
            &mut crate::AuthorityWork::new(B, &cancellation),
        )
        .await
    })?;
    let fence = (async {
        let mut __authority_work = crate::AuthorityWork::new(B, &cancellation);
        let __authority_result = AsyncAuthorityStore::fence(
            &fs.inner.authority,
            volume_authority_id(volume_id),
            fence_expected,
            &mut __authority_work,
        )
        .await;
        __authority_work.receipt(__authority_result)
    })
    .await?;
    let FenceOutcome::Advanced(fenced_head) = fence.value else {
        return Err("fresh facade fence conflicted".into());
    };
    let fenced_outcome =
        (fenced.resume_live(OperationId::from_bytes([122; 16]), 1, 8, B, &cancellation)).await?;
    assert!(matches!(
        fenced_outcome.value,
        LiveMutationOutcome::Fenced { actual_epoch } if actual_epoch == fenced_head.epoch
    ));

    let mut non_live = open_checkout(&volume, writable_manual(), &cancellation).await?;
    assert!((non_live.refresh_live(B, &cancellation)).await.is_err());
    assert!(
        (non_live.mutate_live(
            Vec::new(),
            OperationId::from_bytes([123; 16]),
            1,
            8,
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn authored_live_transaction_preserves_create_ids_and_publishes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let (volume, _) = fixture(serialized_config(), &cancellation).await?;
    let mut checkout = open_checkout(&volume, writable_live(), &cancellation).await?;
    let file = path("authored-live.txt")?;
    let receipt = (checkout.apply_authored_live(
        vec![
            AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: OwnedBytes::from_static(b"initial"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Write {
                path: file.clone(),
                offset: 7,
                bytes: OwnedBytes::from_static(b"-published"),
            },
        ],
        OperationId::from_bytes([118; 16]),
        3,
        8,
        B,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        receipt.value.outcome,
        LiveMutationOutcome::Committed { .. }
    ));
    assert_eq!(receipt.value.created_file_ids.len(), 2);
    assert!(receipt.value.created_file_ids[0].is_some());
    assert!(receipt.value.created_file_ids[1].is_none());
    assert!(!checkout.has_pending_mutations());
    let read = (checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 17,
        },
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(read.value.bytes.as_ref(), b"initial-published");
    Ok(())
}
